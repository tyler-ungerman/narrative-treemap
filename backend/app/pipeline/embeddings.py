from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import numpy as np
from sklearn.feature_extraction.text import HashingVectorizer

from app.core.database import Database

logger = logging.getLogger(__name__)


class EmbeddingEngine:
    def __init__(self, database: Database):
        self.database = database
        self.preferred_model_name = "all-MiniLM-L6-v2"
        self.fallback_model_name = "hashing-fallback-v1"
        self.active_model_name = self.preferred_model_name
        self._sentence_model: Any | None = None
        self._hashing_vectorizer = HashingVectorizer(
            n_features=384,
            alternate_sign=False,
            norm="l2",
            stop_words="english",
        )

    async def _ensure_encoder(self) -> None:
        if self._sentence_model is not None:
            return

        def load_model() -> Any:
            os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
            os.environ.setdefault("TRANSFORMERS_NO_ADVISORY_WARNINGS", "1")
            os.environ.setdefault("TRANSFORMERS_VERBOSITY", "error")

            try:
                from huggingface_hub.utils import disable_progress_bars

                disable_progress_bars()
            except Exception:
                pass

            try:
                from transformers.utils import logging as transformers_logging

                transformers_logging.set_verbosity_error()
                transformers_logging.disable_progress_bar()
            except Exception:
                pass

            from sentence_transformers import SentenceTransformer

            return SentenceTransformer(self.preferred_model_name)

        try:
            self._sentence_model = await asyncio.to_thread(load_model)
            self.active_model_name = self.preferred_model_name
            logger.info("embedding_model_loaded", extra={"extra": {"model": self.active_model_name}})
        except Exception as exc:
            self._sentence_model = None
            self.active_model_name = self.fallback_model_name
            logger.warning(
                "embedding_model_fallback",
                extra={"extra": {"model": self.active_model_name, "reason": str(exc)}},
            )

    async def _encode_texts(self, texts: list[str]) -> np.ndarray:
        await self._ensure_encoder()
        if self._sentence_model is not None:
            encoded = await asyncio.to_thread(
                self._sentence_model.encode,
                texts,
                convert_to_numpy=True,
                normalize_embeddings=True,
            )
            return np.asarray(encoded, dtype=np.float32)
        hashed = self._hashing_vectorizer.transform(texts).toarray()
        return np.asarray(hashed, dtype=np.float32)

    async def embed_items(self, item_ids: list[str], texts: list[str]) -> tuple[np.ndarray, str]:
        await self._ensure_encoder()
        model_name = self.active_model_name
        cached = self.database.get_embeddings(item_ids=item_ids, model_name=model_name)

        vectors_by_id: dict[str, list[float]] = {}
        missing_ids: list[str] = []
        missing_texts: list[str] = []

        for item_id, text in zip(item_ids, texts, strict=True):
            if item_id in cached:
                vectors_by_id[item_id] = cached[item_id]
            else:
                missing_ids.append(item_id)
                missing_texts.append(text)

        if missing_ids:
            encoded = await self._encode_texts(missing_texts)
            for item_id, vector in zip(missing_ids, encoded, strict=True):
                vectors_by_id[item_id] = vector.tolist()
            self.database.save_embeddings(model_name=model_name, vectors={
                item_id: vectors_by_id[item_id] for item_id in missing_ids
            })

        matrix = np.asarray([vectors_by_id[item_id] for item_id in item_ids], dtype=np.float32)
        return matrix, model_name
