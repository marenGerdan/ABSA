import json
import hashlib
import re
import httpx
import os
import asyncio
from collections import deque
from time import monotonic

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, field_validator
from sqlalchemy import create_engine, Column, Integer, String, Text, ForeignKey, func
from sqlalchemy.orm import sessionmaker, relationship, Session, declarative_base
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "change-me-in-production")
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:8000").split(",")

MAX_CONTENT_LENGTH = 2000  # символів — захист від prompt injection

# ── База даних ────────────────────────────────────────────────────────────────
engine = create_engine(
    DATABASE_URL,
    pool_size=20,
    max_overflow=30,
    pool_timeout=60,
    pool_recycle=3600,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Review(Base):
    __tablename__ = "reviews"
    id = Column(Integer, primary_key=True, index=True)
    content = Column(Text, nullable=False)
    text_hash = Column(String(32), unique=True)
    rating = Column(Integer)
    location = Column(String(100))
    is_processed = Column(Integer, default=0)
    results = relationship("AnalysisResult", back_populates="owner", cascade="all, delete-orphan")


class AnalysisResult(Base):
    __tablename__ = "analysis_results"
    id = Column(Integer, primary_key=True, index=True)
    review_id = Column(Integer, ForeignKey("reviews.id"))
    category = Column(String(100))
    sentiment = Column(String(20))
    quote = Column(Text)
    owner = relationship("Review", back_populates="results")


Base.metadata.create_all(bind=engine)

# ── Rate Limiter ──────────────────────────────────────────────────────────────
class RateLimiter:
    def __init__(self, max_calls: int = 25, period: float = 60.0):
        self.max_calls = max_calls
        self.period = period
        self._timestamps: deque = deque()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = monotonic()
            while self._timestamps and now - self._timestamps[0] > self.period:
                self._timestamps.popleft()

            if len(self._timestamps) >= self.max_calls:
                wait = self.period - (now - self._timestamps[0])
                print(f"⏳ Rate limit: чекаємо {wait:.1f} с...")
                await asyncio.sleep(wait)

            self._timestamps.append(monotonic())


rate_limiter = RateLimiter(max_calls=25, period=60.0)

# ── FastAPI ───────────────────────────────────────────────────────────────────
app = FastAPI(title="ABSA Review Analyzer")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,  # ВИПРАВЛЕНО: більше не "*"
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── Автентифікація ─────────────────────────────────────────────────────────────
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_api_key(key: str = Security(api_key_header)):
    if key != INTERNAL_API_KEY:
        raise HTTPException(status_code=403, detail="Недійсний або відсутній API-ключ")
    return key


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── Аналіз ───────────────────────────────────────────────────────────────────
def _sanitize_content(text: str) -> str:
    """Обрізає текст та прибирає символи, що можуть зламати JSON-промпт."""
    text = text[:MAX_CONTENT_LENGTH]
    # Видаляємо керуючі символи, залишаємо \n та \t
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
    return text


def _parse_llm_response(raw: str) -> list[dict] | None:
    """
    Витягує та парсить JSON із сирої відповіді моделі.

    ВИПРАВЛЕНО: замість жадібного `.*` пробуємо json.loads по всьому рядку,
    а потім шукаємо перший валідний JSON-об'єкт вручну через декодер.
    """
    # Спроба 1: весь рядок одразу
    try:
        data = json.loads(raw)
        return data.get("results")
    except json.JSONDecodeError:
        pass

    # Спроба 2: знайти перший повний JSON-об'єкт через JSONDecoder
    decoder = json.JSONDecoder()
    for i, char in enumerate(raw):
        if char == '{':
            try:
                data, _ = decoder.raw_decode(raw, i)
                return data.get("results")
            except json.JSONDecodeError:
                continue

    print("⚠️  Не вдалося розпарсити JSON з відповіді LLM")
    return None


async def _call_groq_api(content: str) -> httpx.Response:
    prompt = (
        f"Analyze ALL aspects mentioned in this review: '{content}'\n\n"
        "Return ONLY JSON. Find EVERY aspect mentioned, even if there are 5-6 of them.\n"
        "EXACT format:\n"
        '{"results": [\n'
        '  {"category": "Food", "sentiment": "positive", "quote": "смачні суші"},\n'
        '  {"category": "Delivery", "sentiment": "positive", "quote": "швидку доставку"}\n'
        ']}\n'
        "Categories (use only these): Food, Service, Price, Atmosphere, Delivery.\n"
        "Sentiments (use only these): positive, negative, neutral.\n"
        "IMPORTANT: quote must be the EXACT substring from the review text, copied verbatim."
    )
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient() as client:
        return await client.post(
            GROQ_URL,
            headers=headers,
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a JSON-only generator. No intro, no comments. Output strictly double-quoted JSON.",
                    },
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.1,
            },
            timeout=40.0,
        )


def _save_results(db: Session, review_id: int, results: list[dict]):
    """Зберігає результати аналізу в БД."""
    db.query(AnalysisResult).filter(AnalysisResult.review_id == review_id).delete()
    for item in results:
        db.add(AnalysisResult(
            review_id=review_id,
            category=item.get("category", "General"),
            sentiment=item.get("sentiment", "neutral"),
            quote=item.get("quote", ""),
        ))
    db.query(Review).filter(Review.id == review_id).update({"is_processed": 1})
    db.commit()


async def perform_analysis(content: str, review_id: int):
    """Фонова задача: відправляє відгук до Groq і зберігає результати."""
    await asyncio.sleep(5.0)
    db = SessionLocal()

    try:
        await rate_limiter.acquire()

        safe_content = _sanitize_content(content)  # ВИПРАВЛЕНО: санітизація перед промптом
        response = await _call_groq_api(safe_content)

        if response.status_code == 429:
            print(f"🚫 Rate limit від Groq для ID {review_id} — позначаємо як необроблений")
            db.query(Review).filter(Review.id == review_id).update({"is_processed": 0})
            db.commit()
            return

        if response.status_code != 200:
            raise RuntimeError(f"Groq API повернув {response.status_code}: {response.text}")

        raw_content = response.json()["choices"][0]["message"]["content"].strip()
        results = _parse_llm_response(raw_content)

        if results is None:
            db.query(Review).filter(Review.id == review_id).update({"is_processed": -1})
            db.commit()
            return

        _save_results(db, review_id, results)
        print(f"✅ ID {review_id} оброблено успішно")

    except Exception as e:
        print(f"🔥 Критична помилка ID {review_id}: {e}")
        try:
            db.query(Review).filter(Review.id == review_id).update({"is_processed": -1})
            db.commit()
        except Exception:
            db.rollback()
    finally:
        db.close()  # Гарантоване закриття сесії


# ── Схеми ─────────────────────────────────────────────────────────────────────
class ReviewCreate(BaseModel):
    content: str
    rating: int
    location: str

    @field_validator("rating")
    @classmethod
    def rating_range(cls, v: int) -> int:
        if not (1 <= v <= 5):
            raise ValueError("Рейтинг має бути від 1 до 5")
        return v

    @field_validator("content")
    @classmethod
    def content_not_empty(cls, v: str) -> str:
        if len(v.strip()) < 3:
            raise ValueError("Текст відгуку занадто короткий")
        return v.strip()


# ── Ендпоінти ─────────────────────────────────────────────────────────────────
@app.post("/analyze")
async def analyze(
    data: ReviewCreate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    text_hash = hashlib.md5(data.content.strip().lower().encode()).hexdigest()
    existing = db.query(Review).filter(Review.text_hash == text_hash).first()

    if existing:
        return {"status": "exists", "id": existing.id}

    new_rev = Review(
        content=data.content,
        text_hash=text_hash,
        rating=data.rating,
        location=data.location,
    )
    db.add(new_rev)
    db.commit()
    db.refresh(new_rev)
    background_tasks.add_task(perform_analysis, data.content, new_rev.id)
    return {"status": "processing", "id": new_rev.id}


@app.get("/stats")
async def get_stats(db: Session = Depends(get_db)):
    rows = (
        db.query(
            AnalysisResult.category,
            AnalysisResult.sentiment,
            func.count(AnalysisResult.id),
        )
        .group_by(AnalysisResult.category, AnalysisResult.sentiment)
        .all()
    )
    stats: dict = {}
    for cat, sent, count in rows:
        if cat not in stats:
            stats[cat] = {"positive": 0, "negative": 0, "neutral": 0}
        if sent in stats[cat]:
            stats[cat][sent] = count
    return stats


@app.get("/reviews")
async def get_reviews(db: Session = Depends(get_db)):
    reviews = db.query(Review).order_by(Review.id.desc()).limit(50).all()
    return [
        {
            "id": r.id,
            "content": r.content,
            "rating": r.rating,
            "location": r.location,
            "analysis": [
                {"category": a.category, "sentiment": a.sentiment, "quote": a.quote}
                for a in r.results
            ],
        }
        for r in reviews
    ]


# ВИПРАВЛЕНО: захищений ендпоінт з API-ключем
@app.post("/process_pending")
async def process_pending(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    _: str = Depends(require_api_key),
):
    pending = db.query(Review).filter(Review.is_processed.in_([0, -1])).all()
    for rev in pending:
        background_tasks.add_task(perform_analysis, rev.content, rev.id)
    return {"status": "started", "count": len(pending)}