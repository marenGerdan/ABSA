import asyncio
import httpx

API_URL = "http://127.0.0.1:8000/analyze"
MAX_RETRIES = 3
RETRY_DELAYS = [2, 5, 10]

# ВИПРАВЛЕНО: клієнт на рівні модуля для перевикористання з'єднань
_http_client: httpx.AsyncClient | None = None


def get_http_client() -> httpx.AsyncClient:
    """Повертає спільний AsyncClient, створює при першому виклику."""
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(timeout=10.0)
    return _http_client


async def close_http_client():
    """Закриває клієнт при завершенні застосунку (викликати в lifespan або on_shutdown)."""
    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()
        _http_client = None


async def _post_with_retry(payload: dict) -> bool:
    """
    Відправляє POST-запит з автоматичними повторними спробами.
    Повертає True при успіху, False якщо всі спроби вичерпано.
    """
    client = get_http_client()

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = await client.post(API_URL, json=payload)

            if response.status_code == 200:
                return True

            if response.status_code == 429:
                wait = RETRY_DELAYS[attempt - 1] * 3
                print(f"⏳ Rate limit (спроба {attempt}/{MAX_RETRIES}), чекаємо {wait} с...")
                await asyncio.sleep(wait)
                continue

            # Інші HTTP-помилки — не повторюємо
            print(f"⚠️  Сервер повернув {response.status_code}, відгук пропущено")
            return False

        except httpx.ConnectError:
            wait = RETRY_DELAYS[attempt - 1]
            print(f"🔌 Сервер недоступний (спроба {attempt}/{MAX_RETRIES}), чекаємо {wait} с...")
            await asyncio.sleep(wait)

        except httpx.TimeoutException:
            wait = RETRY_DELAYS[attempt - 1]
            print(f"⏱️  Таймаут (спроба {attempt}/{MAX_RETRIES}), чекаємо {wait} с...")
            await asyncio.sleep(wait)

        except Exception as e:
            print(f"❌ Несподівана помилка синхронізації: {e}")
            return False

    print(f"❌ Всі {MAX_RETRIES} спроби вичерпано, відгук не відправлено")
    return False


async def on_new_review_received(
    text: str,
    rating: int = 5,
    location: str = "Unknown",
) -> bool:
    """
    Публічний інтерфейс синхронізації.
    Приймає новий відгук та передає його на сервер аналізу.
    """
    payload = {
        "content": text,
        "rating": rating,
        "location": location,
        "external_source": "sync_service",
    }
    return await _post_with_retry(payload)