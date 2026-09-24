import os
import logging
import requests

log = logging.getLogger("twitter-verifier")

def get_twitter_username_from_opensea(slug: str, opensea_api_key: str) -> str | None:
    try:
        url = f"https://api.opensea.io/api/v2/collections/{slug}"
        headers = {"x-api-key": opensea_api_key}
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code == 200:
            return resp.json().get("twitter_username")
        else:
            log.warning(f"[OpenSea Collections API] HTTP {resp.status_code} عند جلب '{slug}': {resp.text[:200]}")
    except Exception as e:
        log.warning(f"[Twitter Check] تعذر جلب معلومات المجموعة لـ {slug}: {e}")
    return None

def is_valid_twitter_account(username: str) -> bool:
    bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
    if not username:
        return False
    if not bearer_token:
        log.error("[X API] TWITTER_BEARER_TOKEN غير مضبوط في متغيرات البيئة — سيتم رفض كل الحسابات.")
        return False

    try:
        url = f"https://api.x.com/2/users/by/username/{username}?user.fields=verified,public_metrics"
        headers = {"Authorization": f"Bearer {bearer_token}"}
        resp = requests.get(url, headers=headers, timeout=5)

        if resp.status_code == 200:
            user_data = resp.json().get("data", {})
            metrics = user_data.get("public_metrics", {})

            is_verified = user_data.get("verified", False)
            followers_count = metrics.get("followers_count", 0)

            if is_verified or followers_count >= 100:
                log.info(f"✅ حساب X موثوق: @{username} (متابعين: {followers_count})")
                return True
            else:
                log.info(f"⚠️ حساب X ضعيف فعليًا: @{username} (متابعين: {followers_count})")
                return False

        elif resp.status_code == 429:
            log.error(f"[X API] تجاوزت حد الطلبات (429) عند فحص @{username} — لن نعتبره مرفوضًا بشكل نهائي.")
            return False
        elif resp.status_code in (401, 403):
            log.error(
                f"[X API] فشل مصادقة/صلاحية (HTTP {resp.status_code}) عند فحص @{username}: "
                f"{resp.text[:300]} — تحقق من أن TWITTER_BEARER_TOKEN صالح وأن الـ App "
                f"مربوط بمشروع (Project) على خطة تدعم قراءة بيانات المستخدمين."
            )
            return False
        else:
            log.error(f"[X API] استجابة غير متوقعة (HTTP {resp.status_code}) عند فحص @{username}: {resp.text[:300]}")
            return False

    except Exception as e:
        log.error(f"[X API Error] خطأ أثناء التحقق من حساب @{username}: {e}")

    return False
