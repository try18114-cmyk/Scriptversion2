"""
النظام الكامل — 10 محافظ، لكل محفظة بوت تيليجرام خاص بها:
  - يكتشف مينتات اليوم على Robinhood + Ethereum
  - يشتري لجميع المحافظ المعرفة بالتوازي (Parallel Execution)
  - يرسل إشعار الشراء أو التحديث لكل محفظة على بوت التيليجرام الخاص بها
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta

import requests
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_wallet_lock,
)
from twitter_checker import get_twitter_username_from_opensea

load_dotenv()

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"

# تفكيك المحافظ والمفاتيح وإعدادات التيليجرام
PRIVATE_KEYS = [k.strip() for k in os.environ.get("PRIVATE_KEYS", "").split(",") if k.strip()]
WALLETS = [w.strip() for w in os.environ.get("WALLETS", "").split(",") if w.strip()]
TELEGRAM_BOT_TOKENS = [t.strip() for t in os.environ.get("TELEGRAM_BOT_TOKENS", "").split(",") if t.strip()]
TELEGRAM_CHAT_IDS = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]

if not (len(PRIVATE_KEYS) == len(WALLETS) == len(TELEGRAM_BOT_TOKENS) == len(TELEGRAM_CHAT_IDS)):
    raise ValueError("أعداد المفاتيح، المحافظ، توكنات البوتات، و Chat IDs غير متطابقة في ملف .env!")

# إنشاء هيكلية المحافظ
WALLETS_DATA = []
for i in range(len(WALLETS)):
    WALLETS_DATA.append({
        "wallet": WALLETS[i],
        "private_key": PRIVATE_KEYS[i],
        "bot_token": TELEGRAM_BOT_TOKENS[i],
        "chat_id": TELEGRAM_CHAT_IDS[i],
    })

ALCHEMY_API_KEY_ROBINHOOD = os.environ["ALCHEMY_API_KEY"]
ALCHEMY_API_KEY_ETHEREUM = os.environ["ALCHEMY_API_KEY_ETHEREUM"]

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
LOCAL_TZ = timezone(timedelta(hours=3))

HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
FREE_PRICE_THRESHOLD_USD = 0.01
WATCH_POLL_INTERVAL_SECONDS = 15

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer")

CHAIN_CONFIGS = {
    "robinhood": {
        "stream_chain_name": "robinhood",
        "rpc_url": f"https://robinhood-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ROBINHOOD}",
        "max_gas_fee_usd": 0.18,
        "min_balance_reserve_usd": 0.02,
    },
    "ethereum": {
        "stream_chain_name": "ethereum",
        "rpc_url": f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY_ETHEREUM}",
        "max_gas_fee_usd": 0.50,
        "min_balance_reserve_usd": 0.10,
    },
}

W3_INSTANCES = {key: get_web3(cfg["rpc_url"]) for key, cfg in CHAIN_CONFIGS.items()}
STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

# تتبع المحافظ التي اشترت بنجاح: slug -> set(wallet_address)
successful_mints: dict[str, set[str]] = {}
watchlist: dict[str, dict] = {}
in_flight: set[str] = set()

# تبريد مؤقت للمجموعات التي رُفضت (سعر، تويتر، إلخ) لمنع إعادة فحصها
# مع كل حدث "مينت جديد" من نفس المجموعة (قد يصل عشرات المرات بالثانية)
REJECTION_COOLDOWN_SECONDS = 120
rejected_cooldown: dict[str, float] = {}

# مجموعات تمت محاولة الشراء الفعلي منها مرة واحدة (نجاحًا أو فشلاً) — لا تُعاد أبدًا،
# بخلاف رفض التبريد المؤقت أعلاه الذي يخص فقط حالات ما قبل الشراء (غير مجاني / لا حساب X)
attempted_slugs: set[str] = set()


def is_in_cooldown(slug: str) -> bool:
    ts = rejected_cooldown.get(slug)
    if ts is None:
        return False
    if time.time() - ts >= REJECTION_COOLDOWN_SECONDS:
        rejected_cooldown.pop(slug, None)
        return False
    return True


def mark_rejected(slug: str):
    rejected_cooldown[slug] = time.time()

_eth_price_cache = {"value": None, "ts": 0}


def get_eth_price_usd() -> float:
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < 300):
        return _eth_price_cache["value"]
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=8,
        )
        price = resp.json()["ethereum"]["usd"]
        _eth_price_cache["value"] = price
        _eth_price_cache["ts"] = now
        return price
    except Exception as e:
        log.warning(f"[السعر] تعذر جلب سعر ETH: {e}")
        return _eth_price_cache["value"] or 3000.0


def fetch_drop_detail(slug: str):
    try:
        resp = requests.get(
            f"{DROPS_API_BASE}/{slug}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=10,
        )
        if resp.status_code == 200:
            return True, resp.json()
        if resp.status_code == 404:
            return False, None
        return None, None
    except Exception as e:
        log.warning(f"[Drops API] خطأ: {e}")
        return None, None


def parse_iso(ts: str):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def started_today_local(stage: dict) -> bool:
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        return False
    return start.astimezone(LOCAL_TZ).date() == datetime.now(LOCAL_TZ).date()


def stage_has_ended(stage: dict) -> bool:
    end = parse_iso(stage.get("end_time", ""))
    if not end:
        return False
    return datetime.now(timezone.utc) > end


def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD


def has_viable_free_stage(detail: dict, eth_price_usd: float) -> bool:
    """هل توجد أي مرحلة (حالية أو مستقبلية لم تبدأ بعد) ضمن كامل جدول المينت
    سعرها مجاني؟ يفحص القائمة الكاملة stages وليس فقط active_stage، لتفادي الاستمرار
    بمراقبة مينت لم يعد له أي مرحلة مجانية متبقية (كل ما تبقى مدفوع)."""
    stages = detail.get("stages") or []
    if not stages:
        # لا توجد بيانات جدول كامل — نعتمد على المرحلة النشطة فقط كحل احتياطي
        stage = detail.get("active_stage")
        if not stage:
            return False
        return is_free_or_negligible(int(stage.get("price") or 0), eth_price_usd)

    for stage in stages:
        if stage_has_ended(stage):
            continue  # مرحلة انتهت بالفعل — لا فائدة منها بعد الآن
        if is_free_or_negligible(int(stage.get("price") or 0), eth_price_usd):
            return True
    return False


# ---------------------------------------------------------------------------
# إدارة رسائل التيليجرام الخاصة بالبوتات المتعددة
# ---------------------------------------------------------------------------

send_queue: "asyncio.Queue[dict]" = asyncio.Queue()


def enqueue_message(bot_token: str, chat_id: str, text: str):
    """إضافة إشعار جديد مع تحديد البوت والمستلم"""
    send_queue.put_nowait({
        "bot_token": bot_token,
        "chat_id": chat_id,
        "text": text
    })


def broadcast_message(text: str):
    """إرسال إشعار عام لجميع البوتات المربوطة بالـ 10 محافظ"""
    for w in WALLETS_DATA:
        enqueue_message(w["bot_token"], w["chat_id"], text)


async def telegram_sender():
    while True:
        msg = await send_queue.get()
        try:
            telegram_api = f"https://api.telegram.org/bot{msg['bot_token']}"
            await asyncio.to_thread(
                requests.post,
                f"{telegram_api}/sendMessage",
                data={"chat_id": msg["chat_id"], "text": msg["text"], "parse_mode": "HTML"},
                timeout=10,
            )
        except Exception as e:
            log.error(f"خطأ إرسال تليجرام للبوت ({msg['bot_token'][:10]}...): {e}")
        send_queue.task_done()
        await asyncio.sleep(0.1)  # تسريع الإرسال ليدعم البوتات المتعددة


def build_single_wallet_success_msg(detail: dict, result: dict, chain_key: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    url = detail.get("opensea_url", "")
    chain_label = "Robinhood Chain" if chain_key == "robinhood" else "Ethereum Mainnet"
    w_short = result['wallet'][:6] + "..." + result['wallet'][-4:]
    return (
        f"✅ <b>تم الشراء بنجاح لمحافظتك!</b> ({chain_label})\n\n"
        f"المحفظة: <code>{w_short}</code>\n"
        f"المجموعة: <b>{name}</b>\n"
        f"الكمية: {result['quantity']}\n"
        f"رسوم الغاز: ${result['gas_fee_usd']:.4f}\n"
        f"المعاملة: {result['tx_hash']}\n"
        f"🔗 {url}"
    )


def build_watching_message(detail: dict, reason: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    return f"👀 <b>تحت المراقبة لمحافظتك</b>\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason}\nسنحاول الشراء تلقائيًا فور توفر الفرصة."


def build_gaveup_message(detail: dict, reason: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    return f"❌ <b>انتهت الفرصة</b>\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason}"


FAILURE_REASON_LABELS = {
    "balance_too_low": "رصيد غير كافٍ",
    "gas_too_high": "رسوم الغاز أعلى من الحد المسموح",
    "simulation_failed": "رفضت محاكاة المعاملة",
    "insufficient_funds_for_total_cost": "الرصيد لا يغطي (السعر + الغاز)",
    "tx_error": "خطأ عند إرسال المعاملة",
    "invalid_address": "عنوان محفظة غير صالح",
    "already_bought": "تم الشراء مسبقًا لهذه المحفظة",
    "sold_out": "نفدت الكمية",
    "no_contract_address": "لا يوجد عنوان عقد لهذه المجموعة",
    "all_wallets_completed": "كل المحافظ اشترت مسبقًا",
    "not_eligible_or_sold_out": "هذه المحفظة غير مؤهلة لأي مرحلة حاليًا (أو نفدت الكمية)",
    "not_eligible_for_current_stage": "غير مؤهل للمرحلة النشطة حاليًا (قد تصبح مؤهلاً في مرحلة لاحقة)",
    "stage_not_active": "لا توجد مرحلة نشطة حاليًا لهذا العقد",
    "mint_build_failed": "تعذر بناء معاملة الشراء عبر OpenSea",
    "rate_limited": "تجاوزنا حد طلبات OpenSea (Rate Limit) لحظيًا",
    "stage_not_free_for_wallet": "المرحلة المؤهلة لهذه المحفظة ليست مجانية",
}

# أسباب رفض مؤقتة يُتوقَّع تكرارها كثيرًا أثناء المراقبة المستمرة (كل بضع ثوانٍ)، فلا تُرسَل
# إشعار فشل فردي متكرر لها عبر تيليجرام (ستُعاد المحاولة تلقائيًا). قرار "متابعة المراقبة من
# عدمها" نفسه لم يعد يعتمد على هذه القائمة — بل على has_viable_free_stage فقط (انظر أسفله).
TRANSIENT_FAILURE_REASONS = ("not_eligible_for_current_stage", "rate_limited")


def build_wallet_failure_msg(detail: dict, result: dict, chain_key: str) -> str:
    """رسالة فشل فردية لمحفظة واحدة، بنفس صيغة 'انتهت الفرصة'، تُرسل لبوت هذه المحفظة فقط."""
    name = detail.get("collection_name") or detail.get("collection_slug")
    reason_key = result.get("reason")
    reason_label = FAILURE_REASON_LABELS.get(reason_key, reason_key or "غير معروف")
    error_detail = result.get("error")
    if error_detail and reason_key in ("tx_error", "simulation_failed"):
        reason_label = f"{reason_label} — {error_detail}"
    return f"❌ <b>انتهت الفرصة</b>\n\nالمجموعة: <b>{name}</b>\nالسبب: {reason_label}"


def build_purchase_summary_log(detail: dict, results: list[dict]) -> str:
    """ملخص نصي (بدون HTML) لعرضه في اللوج فقط — لا يُرسَل لتيليجرام."""
    name = detail.get("collection_name") or detail.get("collection_slug")
    total = len(results)
    successes = [r for r in results if r.get("success")]
    failures = [r for r in results if not r.get("success")]

    parts = [f"[{name}] نتيجة محاولة الشراء: {len(successes)} نجاح / {len(failures)} فشل (من أصل {total})"]
    if failures:
        reasons = []
        for r in failures:
            wallet_short = (r.get("wallet") or "?")[:8]
            reason_key = r.get("reason")
            reason_label = FAILURE_REASON_LABELS.get(reason_key, reason_key or "غير معروف")
            error_detail = r.get("error")
            extra = f" ({error_detail})" if error_detail and reason_key in ("tx_error", "simulation_failed") else ""
            reasons.append(f"{wallet_short}={reason_label}{extra}")
        parts.append("الأسباب: " + ", ".join(reasons))
    return " — ".join(parts)


# ---------------------------------------------------------------------------
# الشراء المتوازي وتوزيع الإشعارات على البوتات الخاصة
# ---------------------------------------------------------------------------

async def purchase_task_for_wallet(
    w3, item, slug, max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd, min_balance_reserve_usd
):
    wallet_addr = item["wallet"]
    pk = item["private_key"]
    bot_token = item["bot_token"]
    chat_id = item["chat_id"]

    lock = get_wallet_lock(wallet_addr)
    async with lock:
        if wallet_addr in successful_mints.get(slug, set()):
            return {"success": False, "wallet": wallet_addr, "reason": "already_bought"}

        res = await asyncio.to_thread(
            attempt_purchase_single_wallet,
            w3, pk, wallet_addr,
            max_per_wallet, remaining,
            eth_price_usd, max_gas_fee_usd, min_balance_reserve_usd, slug, OPENSEA_API_KEY,
        )

        if res.get("success"):
            if slug not in successful_mints:
                successful_mints[slug] = set()
            successful_mints[slug].add(wallet_addr)
            
            # إرسال إشعار النجاح فقط للبوت المربوط بهذه المحفظة
            msg = build_single_wallet_success_msg(item.get("current_detail", {}), res, item.get("chain_key", ""))
            enqueue_message(bot_token, chat_id, msg)
        elif res.get("reason") not in (("already_bought",) + TRANSIENT_FAILURE_REASONS):
            # إرسال إشعار الفشل فقط للبوت المربوط بهذه المحفظة تحديدًا
            msg = build_wallet_failure_msg(item.get("current_detail", {}), res, item.get("chain_key", ""))
            enqueue_message(bot_token, chat_id, msg)

        return res


async def try_buy_now_multi_wallet(slug: str, chain_key: str, detail: dict) -> list[dict] | None:
    stage = detail.get("active_stage")
    if not stage:
        return None

    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max_supply - total_supply
    if remaining <= 0:
        return [{"success": False, "reason": "sold_out"}]

    contract_address = detail.get("contract_address")
    if not contract_address:
        return [{"success": False, "reason": "no_contract_address"}]

    w3 = W3_INSTANCES[chain_key]
    eth_price_usd = get_eth_price_usd()

    stage_price_wei = int(stage.get("price") or 0)
    if not is_free_or_negligible(stage_price_wei, eth_price_usd):
        return None  # مدفوع -> للمراقبة

    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]
    min_balance_reserve_usd = CHAIN_CONFIGS[chain_key]["min_balance_reserve_usd"]

    already_bought_wallets = successful_mints.get(slug, set())
    pending_items = [item for item in WALLETS_DATA if item["wallet"] not in already_bought_wallets]

    if not pending_items:
        return [{"success": False, "reason": "all_wallets_completed"}]

    # إلحاق تفاصيل السياق للطلب
    for item in pending_items:
        item["current_detail"] = detail
        item["chain_key"] = chain_key

    tasks = [
        purchase_task_for_wallet(
            w3, item, slug, max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd, min_balance_reserve_usd
        )
        for item in pending_items
    ]

    results = await asyncio.gather(*tasks)
    return list(results)


# ---------------------------------------------------------------------------
# تقييم النتاجات وإدارة قائمة المراقبة
# ---------------------------------------------------------------------------

async def evaluate_new_mint(slug: str, chain_key: str):
    if (
        len(successful_mints.get(slug, set())) >= len(WALLETS_DATA)
        or slug in watchlist
        or slug in in_flight
        or slug in attempted_slugs
        or is_in_cooldown(slug)
    ):
        return

    in_flight.add(slug)
    try:
        # 1. جلب تفاصيل المينت
        found, detail = await asyncio.to_thread(fetch_drop_detail, slug)
        if not found or not detail or not detail.get("is_minting"):
            return

        stage = detail.get("active_stage")
        if not stage or not started_today_local(stage):
            return

        # 2. تحقق شامل: هل توجد أي مرحلة (حالية أو مستقبلية) مجانية في كامل الجدول؟
        # لو لا توجد إطلاقًا، هذا المينت غير قابل للفوز به أبدًا لبوت شراء مجاني — نتجاهله نهائيًا.
        eth_price_usd = get_eth_price_usd()

        if not has_viable_free_stage(detail, eth_price_usd):
            return

        stage_price_wei = int(stage.get("price") or 0)
        if not is_free_or_negligible(stage_price_wei, eth_price_usd):
            # توجد مرحلة مجانية في الجدول لكنها لم تبدأ بعد — نراقب حتى تبدأ
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}
            return

        # 3. الفحص عبر X: يكفي وجود حساب X مربوط بالمجموعة (دون فحص التوثيق أو المتابعين)
        twitter_username = await asyncio.to_thread(get_twitter_username_from_opensea, slug, OPENSEA_API_KEY)
        if not twitter_username:
            log.info(f"⏭️ تجاهل '{slug}': لا يوجد حساب X مربوط.")
            mark_rejected(slug)
            return

        log.info(f"✅ '{slug}': يوجد حساب X مربوط (@{twitter_username}) — المتابعة للشراء.")

        # 4. التنفيذ للشراء التلقائي — محاولة واحدة فقط
        results = await try_buy_now_multi_wallet(slug, chain_key, detail)

        if results is None:
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}
            broadcast_message(build_watching_message(detail, "السعر الحالي مدفوع — تحت المراقبة."))
            return

        # انتهت المحاولة — ملخص في اللوج فقط (الإشعارات الفردية أُرسلت أثناء التنفيذ لكل محفظة)
        log.info(build_purchase_summary_log(detail, results))

        # حالات فشل جماعي لا تخص محفظة بعينها (نفاد الكمية، لا عنوان عقد...) — نهائية دائمًا
        # بغض النظر عن الجدول، فلا فائدة من "نفدت الكمية" حتى لو توجد مرحلة مجانية لاحقة
        if results and "wallet" not in results[0]:
            reason_label = FAILURE_REASON_LABELS.get(results[0].get("reason"), results[0].get("reason"))
            broadcast_message(build_gaveup_message(detail, reason_label))
            attempted_slugs.add(slug)
        elif has_viable_free_stage(detail, eth_price_usd):
            # ما زالت توجد مرحلة مجانية (حالية أو مستقبلية) — نستمر بالمراقبة بغض النظر عن
            # سبب فشل هذه المحاولة تحديدًا (قد نكون غير مؤهلين للمرحلة الحالية فقط، وليس لاحقًا)
            watchlist[slug] = {"chain_key": chain_key, "detail": detail}
        else:
            # لم تعد توجد أي مرحلة مجانية متبقية في الجدول — محاولة نهائية فعلاً
            attempted_slugs.add(slug)

    except Exception as e:
        log.error(f"خطأ بتقييم '{slug}': {e}")
    finally:
        in_flight.discard(slug)


async def process_watchlist_slug(slug: str):
    """فحص عنصر واحد من watchlist. مصممة لتُشغَّل بالتوازي مع بقية العناصر عبر asyncio.gather —
    أي استثناء يُلتقط داخليًا هنا فقط، حتى لا يوقف gather بقية المهام المتوازية الأخرى."""
    if slug in in_flight or len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
        watchlist.pop(slug, None)
        return

    entry = watchlist.get(slug)
    if not entry:
        return

    in_flight.add(slug)
    try:
        chain_key = entry["chain_key"]
        found, fresh_detail = await asyncio.to_thread(fetch_drop_detail, slug)

        if not found or not fresh_detail or not fresh_detail.get("is_minting"):
            watchlist.pop(slug, None)
            attempted_slugs.add(slug)
            broadcast_message(build_gaveup_message(entry["detail"], "المينت لم يعد نشطًا."))
            return

        # فحص شامل: هل ما زالت توجد مرحلة مجانية (حالية أو مستقبلية) في كامل الجدول؟
        # هذا يوقف المراقبة فورًا بمجرد انتهاء آخر أمل حقيقي (مثلاً مرحلة Allowlist المجانية
        # الوحيدة انتهت، والباقي كله مدفوع) بدل الانتظار لأيام حتى تنتهي آخر مرحلة بالجدول.
        eth_price_usd = get_eth_price_usd()
        if not has_viable_free_stage(fresh_detail, eth_price_usd):
            watchlist.pop(slug, None)
            attempted_slugs.add(slug)
            broadcast_message(build_gaveup_message(fresh_detail, "لم تعد هناك أي مرحلة مجانية متبقية في الجدول."))
            return

        stage = fresh_detail.get("active_stage")
        if not stage or (stage_has_ended(stage) and not fresh_detail.get("next_stage")):
            watchlist.pop(slug, None)
            attempted_slugs.add(slug)
            broadcast_message(build_gaveup_message(fresh_detail, "انتهت المرحلة."))
            return

        results = await try_buy_now_multi_wallet(slug, chain_key, fresh_detail)

        if results is None:
            watchlist[slug] = {"chain_key": chain_key, "detail": fresh_detail}
            return

        # ملخص في اللوج فقط (الإشعارات الفردية أُرسلت أثناء التنفيذ لكل محفظة)
        log.info(build_purchase_summary_log(fresh_detail, results))

        if results and "wallet" not in results[0]:
            reason_label = FAILURE_REASON_LABELS.get(results[0].get("reason"), results[0].get("reason"))
            broadcast_message(build_gaveup_message(fresh_detail, reason_label))
            watchlist.pop(slug, None)
            attempted_slugs.add(slug)
        elif has_viable_free_stage(fresh_detail, eth_price_usd):
            # نفس منطق evaluate_new_mint: طالما توجد مرحلة مجانية متبقية، نستمر بالمراقبة
            # بغض النظر عن سبب فشل هذه المحاولة تحديدًا
            watchlist[slug] = {"chain_key": chain_key, "detail": fresh_detail}
        else:
            watchlist.pop(slug, None)
            attempted_slugs.add(slug)

    except Exception as e:
        log.error(f"خطأ بدورة مراقبة '{slug}': {e}")
    finally:
        in_flight.discard(slug)


async def watch_loop():
    while True:
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
        if not watchlist:
            continue

        # نأخذ لقطة من المفاتيح الحالية، ثم نفحص كل العناصر بالتوازي بدل التسلسل —
        # فحص 30 مينتًا تحت المراقبة يستغرق تقريبًا نفس وقت فحص مينت واحد، وليس 30 ضعفًا
        slugs_snapshot = list(watchlist.keys())
        await asyncio.gather(*(process_watchlist_slug(slug) for slug in slugs_snapshot))


async def listen_opensea():
    msg_ref = 0
    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info(f"متصل بـ OpenSea Stream — يراقب لـ {len(WALLETS_DATA)} محافظ.")
                join_ref = str(msg_ref)
                await ws.send(json.dumps([join_ref, join_ref, "collection:*", "phx_join", {}]))
                msg_ref += 1
                last_heartbeat = time.time()

                while True:
                    if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                        hb_ref = str(msg_ref)
                        await ws.send(json.dumps([None, hb_ref, "phoenix", "heartbeat", {}]))
                        msg_ref += 1
                        last_heartbeat = time.time()

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                    except asyncio.TimeoutError:
                        continue

                    try:
                        parsed = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if isinstance(parsed, list) and len(parsed) == 5:
                        _jref, _ref, _topic, event_name, payload_wrapper = parsed
                    else:
                        continue

                    if event_name != "item_transferred":
                        continue

                    payload = (payload_wrapper or {}).get("payload") or {}
                    item = payload.get("item", {}) or {}
                    stream_chain_name = (item.get("chain", {}) or {}).get("name", "")

                    chain_key = STREAM_NAME_TO_CHAIN_KEY.get(stream_chain_name)
                    if chain_key is None:
                        continue

                    from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                    if from_address != ZERO_ADDRESS:
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if not slug:
                        continue

                    asyncio.create_task(evaluate_new_mint(slug, chain_key))

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"انقطع الاتصال ({e}). إعادة الاتصال...")
            await asyncio.sleep(1)
        except Exception as e:
            log.error(f"خطأ غير متوقع: {e}.")
            await asyncio.sleep(5)


async def run():
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        broadcast_message("🔴 البوت شغّال لكن بوضع الإيقاف (BOT_ENABLED=false).")
        await telegram_sender()
        return

    broadcast_message(f"✅ تم تشغيل المحفظة الخاصة بك بنجاح وتم ربطها بهذا البوت!")
    await asyncio.gather(listen_opensea(), watch_loop(), telegram_sender())


def main():
    backoff = 2
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("تم الإيقاف يدويًا.")
            break
        except Exception as e:
            log.critical(f"توقف غير متوقع: {e}.")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        else:
            break


if __name__ == "__main__":
    main()
