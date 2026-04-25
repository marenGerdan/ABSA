import re
import pandas as pd
import requests
import time
from langdetect import detect, DetectorFactory, LangDetectException

CSV_FILE_PATH = "reviews.csv"
API_URL = "http://127.0.0.1:8000/analyze"
MIN_TEXT_LENGTH = 10
MAX_REVIEWS = 500

# Патерни для фільтрації незмістовного тексту
NOISE_PATTERNS = re.compile(
    r'^[\W\d\s]+$'
    r'|^(.)\1{3,}$'
    r'|^(ok|ок|ок\.?|👍+|👎+|\+|-|\.)$',
    re.IGNORECASE | re.UNICODE
)


def extract_original_text(text: str) -> str:
    """Витягує тільки оригінальну частину відгуку, ігноруючи переклад Google."""
    if not isinstance(text, str):
        return ""

    if "(Original)" in text:
        parts = text.split("(Original)", 1)
        return parts[1].strip() if len(parts) > 1 else text.strip()

    if "(Translated by Google)" in text:
        parts = text.split("(Translated by Google)", 1)
        return parts[0].strip()

    return text.strip()


DetectorFactory.seed = 0


def is_meaningful(text: str) -> bool:
    if len(text) < MIN_TEXT_LENGTH:
        return False

    # ВИПРАВЛЕНО: ловимо конкретний виняток замість голого except
    try:
        lang = detect(text)
        if lang not in ['uk', 'en']:
            return False
    except LangDetectException:
        return False

    if NOISE_PATTERNS.match(text):
        return False

    letter_ratio = sum(c.isalpha() for c in text) / len(text)
    return letter_ratio >= 0.4


def build_payload(row: pd.Series, text: str) -> dict:
    """Формує payload для API із рядка датафрейму."""
    return {
        "content": text,
        "rating": int(row["rating"]) if pd.notna(row.get("rating")) else 5,
        "location": str(row["location_label"]) if pd.notna(row.get("location_label")) else "Не вказано",
    }


def start_import():
    # ── Завантаження ──────────────────────────────────────────────────────────
    try:
        df = pd.read_csv(CSV_FILE_PATH, low_memory=False, encoding="utf-8")
        df = df.dropna(subset=["body"])
        print(f"📄 Завантажено {len(df)} рядків. Починаємо фільтрацію...")
    except Exception as e:
        print(f"❌ Помилка читання CSV: {e}")
        return

    # ── Фільтрація ────────────────────────────────────────────────────────────
    valid_reviews = []
    skipped = 0

    for _, row in df.iterrows():
        text = extract_original_text(row["body"])
        if is_meaningful(text):
            valid_reviews.append(build_payload(row, text))
        else:
            skipped += 1

    print(f"🔍 Прийнято: {len(valid_reviews)}, відхилено: {skipped}")

    # ВИПРАВЛЕНО: попередження якщо дані зрізаються
    if len(valid_reviews) > MAX_REVIEWS:
        print(f"⚠️  Знайдено {len(valid_reviews)} відгуків, але буде відправлено лише перші {MAX_REVIEWS} (MAX_REVIEWS).")

    batch = valid_reviews[:MAX_REVIEWS]
    total = len(batch)
    print(f"🚀 Починаємо імпорт {total} відгуків...\n")

    # ── Відправка ─────────────────────────────────────────────────────────────
    success = failed = 0

    for index, payload in enumerate(batch, start=1):
        try:
            res = requests.post(API_URL, json=payload, timeout=10)
            if res.status_code == 200:
                print(f"✅ [{index}/{total}] Відправлено")
                success += 1
            else:
                print(f"⚠️  [{index}/{total}] Сервер повернув {res.status_code}")
                failed += 1
            time.sleep(7.0)

        except requests.exceptions.ConnectionError:
            print(f"❌ [{index}/{total}] Сервер недоступний — чекаємо 10 с")
            failed += 1
            time.sleep(10)
        except Exception as e:
            print(f"❌ [{index}/{total}] Помилка: {e}")
            failed += 1
            time.sleep(5)

    print(f"\n🏁 Імпорт завершено! Успішно: {success}, помилок: {failed}")


if __name__ == "__main__":
    start_import()