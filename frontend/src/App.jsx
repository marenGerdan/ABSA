import React, { useEffect, useState, useCallback } from 'react';
import axios from 'axios';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer
} from 'recharts';
import './App.css';

const API_BASE = 'http://127.0.0.1:8000';
const POLL_INTERVAL = 5000;

// ── Утиліти ──────────────────────────────────────────────────────────────────
const escapeRegExp = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const SENTIMENT_LABELS = {
  positive: 'Позитив',
  negative: 'Негатив',
  neutral:  'Нейтрально',
};

// ── Компонент: підсвічений текст ──────────────────────────────────────────────
const HighlightedText = ({ text, analysis }) => {
  if (!text) return null;
  if (!analysis?.length) return <p className="review-text">{text}</p>;

  let parts = [{ text, isHighlighted: false }];

  analysis.forEach(({ quote, sentiment, category }) => {
    if (!quote) return;
    const regex = new RegExp(`(${escapeRegExp(quote)})`, 'gi');

    parts = parts.flatMap((part) => {
      if (part.isHighlighted) return [part];
      return part.text.split(regex).map((chunk) =>
        chunk.toLowerCase().trim() === quote.toLowerCase().trim()
          ? { text: chunk, isHighlighted: true, sentiment, category }
          : { text: chunk, isHighlighted: false }
      ).filter((p) => p.text);
    });
  });

  return (
    <p className="review-text">
      {parts.map((part, i) =>
        part.isHighlighted ? (
          <mark
            key={i}
            title={part.category}
            className={`highlight-${part.sentiment.slice(0, 3)}`}
          >
            {part.text}
          </mark>
        ) : (
          <span key={i}>{part.text}</span>
        )
      )}
    </p>
  );
};

// ── Компонент: картка відгуку ─────────────────────────────────────────────────
const ReviewCard = ({ review }) => {
  const uniqueAspects = [...new Set(
    review.analysis.map((a) => `${a.category}-${a.sentiment}`)
  )];

  return (
    <div className="review-card">
      <div className="review-header">
        <span className="stars">{'★'.repeat(review.rating)}</span>
        <span className="location">📍 {review.location}</span>
      </div>

      <HighlightedText text={review.content} analysis={review.analysis} />

      <div className="badge-container">
        {uniqueAspects.map((key, i) => {
          const [cat, sent] = key.split('-');
          return (
            <span key={i} className={`badge ${sent}`} title={SENTIMENT_LABELS[sent]}>
              {cat}
            </span>
          );
        })}
      </div>
    </div>
  );
};

// ── Компонент: графік ─────────────────────────────────────────────────────────
const StatsChart = ({ data }) => (
  <div className="chart-container">
    <ResponsiveContainer width="100%" height={300}>
      <BarChart data={data}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="name" />
        <YAxis />
        <Tooltip />
        <Legend />
        <Bar dataKey="positive" fill="#22c55e" name={SENTIMENT_LABELS.positive} />
        <Bar dataKey="negative" fill="#ef4444" name={SENTIMENT_LABELS.negative} />
        <Bar dataKey="neutral"  fill="#94a3b8" name={SENTIMENT_LABELS.neutral}  />
      </BarChart>
    </ResponsiveContainer>
  </div>
);

// ── Компонент: індикатор завантаження ─────────────────────────────────────────
const LoadingBanner = () => (
  <div className="loading-banner">⏳ Завантаження даних...</div>
);

// ── Головний компонент ────────────────────────────────────────────────────────
function App() {
  const [stats,   setStats]   = useState([]);
  const [reviews, setReviews] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error,   setError]   = useState(null);

  const fetchData = useCallback(async () => {
    // ВИПРАВЛЕНО: не оновлюємо якщо вкладка прихована
    if (document.hidden) return;

    // ВИПРАВЛЕНО: Promise.allSettled замість Promise.all — не падає якщо один запит відмовив
    const [statsResult, reviewsResult] = await Promise.allSettled([
      axios.get(`${API_BASE}/stats`),
      axios.get(`${API_BASE}/reviews`),
    ]);

    if (statsResult.status === 'fulfilled') {
      setStats(
        Object.entries(statsResult.value.data).map(([name, counts]) => ({ name, ...counts }))
      );
    } else {
      console.error('Помилка отримання статистики:', statsResult.reason);
    }

    if (reviewsResult.status === 'fulfilled') {
      setReviews(reviewsResult.value.data);
      setError(null);
    } else {
      console.error('Помилка отримання відгуків:', reviewsResult.reason);
      setError('Не вдалося завантажити дані. Перевірте, чи запущений сервер.');
    }

    setLoading(false);
  }, []);

  useEffect(() => {
    fetchData();

    const interval = setInterval(fetchData, POLL_INTERVAL);

    // ВИПРАВЛЕНО: пауза polling коли вкладка прихована
    const handleVisibilityChange = () => {
      if (!document.hidden) fetchData();
    };
    document.addEventListener('visibilitychange', handleVisibilityChange);

    return () => {
      clearInterval(interval);
      document.removeEventListener('visibilitychange', handleVisibilityChange);
    };
  }, [fetchData]);

  return (
    <div className="app-container">
      <h1 className="main-title">📊 ABSA: Аналітика відгуків</h1>

      {loading && <LoadingBanner />}

      {error && (
        <div className="error-banner">⚠️ {error}</div>
      )}

      {!loading && !error && (
        <>
          <StatsChart data={stats} />

          <div className="reviews-list">
            {reviews.length === 0 ? (
              <p className="empty-state">Відгуків ще немає. Запустіть імпорт даних.</p>
            ) : (
              reviews.map((rev) => <ReviewCard key={rev.id} review={rev} />)
            )}
          </div>
        </>
      )}
    </div>
  );
}

export default App;