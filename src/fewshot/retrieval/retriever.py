from __future__ import annotations

from typing import List, Tuple
from pathlib import Path
import logging
import os
import threading
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

try:
    from sentence_transformers import SentenceTransformer
    _HAS_SENTENCE_TRANSFORMERS = True
except Exception:
    _HAS_SENTENCE_TRANSFORMERS = False

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    _HAS_TFIDF = True
except Exception:
    _HAS_TFIDF = False


class SampleRetriever:
    _fallback_warning_lock = threading.Lock()
    _fallback_warning_emitted = False
    _embedder_lock = threading.Lock()
    _embedder_cache: dict[str, object] = {}

    @staticmethod
    def _env_bool(name: str, default: bool = False) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    def __init__(self, embedding_model: str = "all-MiniLM-L6-v2", top_k: int = 3) -> None:
        self.embedding_model = embedding_model
        self.top_k = top_k
        self._embedder = None
        self._vectorizer = None
        self._train_vectors = None
        self._train_texts: List[str] = []
        backend = os.getenv("RETRIEVER_BACKEND", "auto").strip().lower()
        strict_semantic = self._env_bool("RETRIEVER_STRICT_SEMANTIC", False)

        if backend == "tfidf":
            if _HAS_TFIDF:
                self._vectorizer = TfidfVectorizer()
                return
            raise RuntimeError("TF-IDF backend requested but sklearn is unavailable.")

        if backend in {"semantic", "embedding"} and not _HAS_SENTENCE_TRANSFORMERS:
            raise RuntimeError(
                "Semantic backend requested but sentence-transformers is unavailable."
            )

        require_semantic = strict_semantic or backend in {"semantic", "embedding"}
        if _HAS_SENTENCE_TRANSFORMERS:
            model_path = self._resolve_model_path(self.embedding_model)
            load_target = str(model_path) if model_path is not None else self.embedding_model
            load_kwargs = {
                "device": "cpu",
                "model_kwargs": {"low_cpu_mem_usage": False},
            }
            if model_path is not None:
                load_kwargs["local_files_only"] = True
            try:
                self._embedder = self._get_or_load_embedder(load_target, load_kwargs)
            except Exception as exc:
                if require_semantic:
                    raise RuntimeError(
                        "Semantic retriever initialization failed and TF-IDF fallback is disabled. "
                        f"Original error: {exc}"
                    ) from exc
                if _HAS_TFIDF:
                    self._warn_fallback_once(exc)
                    self._vectorizer = TfidfVectorizer()
                else:
                    raise
        elif _HAS_TFIDF and not require_semantic:
            self._vectorizer = TfidfVectorizer()
        else:
            raise RuntimeError(
                "No embedding backend available (sentence-transformers unavailable and TF-IDF disabled)."
            )

    def fit(self, train_texts: List[str]) -> None:
        self._train_texts = train_texts
        if self._embedder is not None:
            self._train_vectors = np.asarray(self._embedder.encode(train_texts, show_progress_bar=False))
        else:
            self._train_vectors = self._vectorizer.fit_transform(train_texts)

    def retrieve(self, query_text: str, top_k: int | None = None) -> List[Tuple[int, float]]:
        if top_k is None:
            top_k = self.top_k
        if self._train_vectors is None:
            raise RuntimeError("Retriever not fitted")

        query_vec = self._encode_query(query_text)
        sims = cosine_similarity(query_vec, self._train_vectors)[0]

        ranked = np.argsort(sims)[::-1][:top_k]
        return [(int(idx), float(sims[idx])) for idx in ranked]

    def _encode_query(self, query_text: str):
        if self._embedder is not None:
            return np.asarray(self._embedder.encode([query_text], show_progress_bar=False))
        return self._vectorizer.transform([query_text])

    @staticmethod
    def _resolve_model_path(model_name: str) -> Path | None:
        candidate = Path(model_name)
        if candidate.exists():
            return candidate
        repo_root = Path(__file__).resolve().parents[2]
        candidate = repo_root / model_name
        if candidate.exists():
            return candidate
        return None

    @classmethod
    def _warn_fallback_once(cls, exc: Exception) -> None:
        with cls._fallback_warning_lock:
            if cls._fallback_warning_emitted:
                return
            logging.warning(
                "SentenceTransformer initialization failed (%s). Falling back to TF-IDF retriever.",
                exc,
            )
            cls._fallback_warning_emitted = True

    @classmethod
    def _get_or_load_embedder(cls, load_target: str, load_kwargs: dict) -> object:
        with cls._embedder_lock:
            cached = cls._embedder_cache.get(load_target)
            if cached is not None:
                return cached
            embedder = SentenceTransformer(load_target, **load_kwargs)
            cls._embedder_cache[load_target] = embedder
            return embedder
