"""
Vector store service with pluggable provider support (Qdrant/OpenSearch),
metered by an atomic per-org credit balance.

Copied near-verbatim from a production codebase for portfolio purposes — this
module was already self-contained (no secrets, no client-identifying data;
only generic environment-variable *names*, never values). Depends on
`Controllers.db` (Mongo client) and `services.ai_embedding_config_service`
from the original codebase, which are not included here.

Two things worth pointing out to a reviewer:
1. The Qdrant/OpenSearch backends implement an identical interface
   (create_collection/add_documents/search/delete_collection/get_collection_info),
   selected at runtime via `resolve_vector_provider()` — swapping vector
   stores doesn't touch calling code.
2. `_deduct_embedding_credits_sync` uses the same atomic
   compare-and-set pattern as the billing module in this showcase
   (`modules/billing_idempotency/`): the balance check and the deduction
   happen in one `update_one` call with a `{"$gte": cost}` filter, so two
   concurrent embedding calls can't both succeed past a balance neither of
   them can actually afford.
"""

import hashlib
import logging
import os
import asyncio
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import openai
from bson import ObjectId

from Controllers import db  # noqa: F401 - original codebase dependency
from services.ai_embedding_config_service import get_effective_rag_embedding_model_id  # noqa: F401

organizations_collection = db["organizations"]
ai_model_pricing_collection = db["ai_model_pricing"]
credit_transactions_collection = db["credit_transactions"]
logger = logging.getLogger(__name__)

CREDIT_PRECISION = Decimal("0.0001")
MIN_CREDIT_DEDUCTION = Decimal("0.0010")


def _round_credit_value(value: Any) -> float:
    try:
        return float(Decimal(str(value or 0)).quantize(CREDIT_PRECISION, rounding=ROUND_HALF_UP))
    except Exception:
        return 0.0

EMBEDDING_MODEL_DIMENSIONS = {
    "text-embedding-3-small": 1536,
    "text-embedding-3-large": 3072,
    "text-embedding-ada-002": 1536,
}


class InsufficientCreditsError(Exception):
    pass

_ALLOWED_VECTOR_PROVIDERS = {"auto", "qdrant", "opensearch"}


def _is_truthy(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _has_value(name: str) -> bool:
    return bool(os.getenv(name, "").strip())


def resolve_vector_provider() -> str:
    configured = os.getenv("VECTOR_PROVIDER", "auto").strip().lower()
    if configured not in _ALLOWED_VECTOR_PROVIDERS:
        raise ValueError(
            "Invalid VECTOR_PROVIDER. Use one of: auto, qdrant, opensearch"
        )

    if configured != "auto":
        return configured

    has_qdrant = _has_value("QDRANT_URL") or _has_value("QDRANT_HOST")
    has_opensearch = _has_value("OPENSEARCH_URL")

    if has_qdrant and has_opensearch:
        raise RuntimeError(
            "VECTOR_PROVIDER=auto is ambiguous because both Qdrant and OpenSearch config are present. "
            "Set VECTOR_PROVIDER explicitly to qdrant or opensearch."
        )

    if has_opensearch:
        return "opensearch"

    return "qdrant"


class QdrantVectorBackend:
    def __init__(self, embedding_dim: int):
        from qdrant_client import QdrantClient
        from qdrant_client.models import Distance, PointStruct, VectorParams

        self.PointStruct = PointStruct
        self.VectorParams = VectorParams
        self.Distance = Distance

        self.embedding_dim = embedding_dim
        self.qdrant_host = os.getenv("QDRANT_HOST", "localhost")
        self.qdrant_port = int(os.getenv("QDRANT_PORT", "6333"))
        self.qdrant_api_key = os.getenv("QDRANT_API_KEY")

        qdrant_url = os.getenv("QDRANT_URL", "").strip()
        if not qdrant_url:
            protocol = os.getenv("QDRANT_PROTOCOL")
            if protocol:
                qdrant_url = f"{protocol}://{self.qdrant_host}:{self.qdrant_port}"
            elif self.qdrant_host in ["localhost", "127.0.0.1"]:
                qdrant_url = f"http://{self.qdrant_host}:{self.qdrant_port}"
            else:
                qdrant_url = f"https://{self.qdrant_host}:{self.qdrant_port}"

        if self.qdrant_api_key:
            self.client = QdrantClient(url=qdrant_url, api_key=self.qdrant_api_key)
        else:
            self.client = QdrantClient(url=qdrant_url)

    async def create_collection(self, collection_name: str, embedding_dim: Optional[int] = None):
        collections = self.client.get_collections().collections
        if any(c.name == collection_name for c in collections):
            return

        vector_dim = int(embedding_dim or self.embedding_dim)

        self.client.create_collection(
            collection_name=collection_name,
            vectors_config=self.VectorParams(
                size=vector_dim,
                distance=self.Distance.COSINE,
            ),
        )

    async def add_documents(
        self,
        collection_name: str,
        documents: List[Dict[str, str]],
        embeddings: List[List[float]],
    ) -> int:
        points = []
        for doc, embedding in zip(documents, embeddings):
            doc_id = hashlib.md5(doc["text"].encode()).hexdigest()
            points.append(
                self.PointStruct(
                    id=doc_id,
                    vector=embedding,
                    payload={
                        "text": doc["text"],
                        "metadata": doc.get("metadata", {}),
                    },
                )
            )

        self.client.upsert(collection_name=collection_name, points=points)
        return len(points)

    async def search(
        self,
        collection_name: str,
        query_embedding: List[float],
        top_k: int,
    ) -> List[Dict]:
        results = self.client.search(
            collection_name=collection_name,
            query_vector=query_embedding,
            limit=top_k,
        )
        formatted_results = []
        for result in results:
            payload = result.payload or {}
            formatted_results.append(
                {
                    "text": payload.get("text", ""),
                    "metadata": payload.get("metadata", {}),
                    "score": result.score,
                }
            )
        return formatted_results

    async def delete_collection(self, collection_name: str):
        self.client.delete_collection(collection_name)

    def get_collection_info(self, collection_name: str) -> Dict:
        info = self.client.get_collection(collection_name)
        return {
            "name": collection_name,
            "vectors_count": info.vectors_count,
            "points_count": info.points_count,
        }


class OpenSearchVectorBackend:
    def __init__(self, embedding_dim: int):
        from opensearchpy import OpenSearch

        self.OpenSearch = OpenSearch
        self.embedding_dim = embedding_dim
        self.client = self._build_client()

    def _build_client(self):
        from opensearchpy import RequestsHttpConnection

        endpoint = os.getenv("OPENSEARCH_URL", "").strip()
        if not endpoint:
            raise RuntimeError(
                "VECTOR_PROVIDER=opensearch requires OPENSEARCH_URL to be set."
            )

        parsed = urlparse(endpoint)
        if not parsed.scheme or not parsed.hostname:
            raise RuntimeError("Invalid OPENSEARCH_URL. Use a full URL like https://host:443")

        verify_ssl = _is_truthy(os.getenv("VERIFY_SSL"), default=True)
        auth_mode = os.getenv("OPENSEARCH_AUTH_MODE", "none").strip().lower()

        kwargs = {
            "hosts": [{
                "host": parsed.hostname,
                "port": parsed.port or (443 if parsed.scheme == "https" else 80),
                "scheme": parsed.scheme,
            }],
            "verify_certs": verify_ssl,
            "connection_class": RequestsHttpConnection,
        }

        if auth_mode == "basic":
            username = os.getenv("OPENSEARCH_USERNAME", "").strip()
            password = os.getenv("OPENSEARCH_PASSWORD", "").strip()
            if not username or not password:
                raise RuntimeError(
                    "OPENSEARCH_AUTH_MODE=basic requires OPENSEARCH_USERNAME and OPENSEARCH_PASSWORD."
                )
            kwargs["http_auth"] = (username, password)
        elif auth_mode == "aws_iam":
            import boto3
            from requests_aws4auth import AWS4Auth

            region = os.getenv("OPENSEARCH_REGION", "").strip() or "us-east-1"
            session = boto3.Session()
            creds = session.get_credentials()
            if not creds:
                raise RuntimeError("AWS credentials not found for OpenSearch IAM auth.")
            frozen = creds.get_frozen_credentials()
            kwargs["http_auth"] = AWS4Auth(
                frozen.access_key,
                frozen.secret_key,
                region,
                "es",
                session_token=frozen.token,
            )
        elif auth_mode != "none":
            raise RuntimeError(
                "Invalid OPENSEARCH_AUTH_MODE. Use one of: none, basic, aws_iam"
            )

        return self.OpenSearch(**kwargs)

    async def create_collection(self, collection_name: str, embedding_dim: Optional[int] = None):
        if self.client.indices.exists(index=collection_name):
            return

        vector_dim = int(embedding_dim or self.embedding_dim)

        body = {
            "settings": {"index": {"knn": True}},
            "mappings": {
                "properties": {
                    "text": {"type": "text"},
                    "metadata": {"type": "object", "enabled": True},
                    "vector": {"type": "knn_vector", "dimension": vector_dim},
                }
            },
        }
        self.client.indices.create(index=collection_name, body=body)

    async def add_documents(
        self,
        collection_name: str,
        documents: List[Dict[str, str]],
        embeddings: List[List[float]],
    ) -> int:
        from opensearchpy.helpers import bulk

        actions = []
        for doc, embedding in zip(documents, embeddings):
            doc_id = hashlib.md5(doc["text"].encode()).hexdigest()
            actions.append(
                {
                    "_op_type": "index",
                    "_index": collection_name,
                    "_id": doc_id,
                    "_source": {
                        "text": doc["text"],
                        "metadata": doc.get("metadata", {}),
                        "vector": embedding,
                    },
                }
            )

        bulk(self.client, actions)
        return len(actions)

    async def search(
        self,
        collection_name: str,
        query_embedding: List[float],
        top_k: int,
    ) -> List[Dict]:
        knn_query = {
            "size": top_k,
            "query": {
                "knn": {
                    "vector": {
                        "vector": query_embedding,
                        "k": top_k,
                    }
                }
            },
            "_source": ["text", "metadata"],
        }

        try:
            response = self.client.search(index=collection_name, body=knn_query)
        except Exception:
            script_query = {
                "size": top_k,
                "query": {
                    "script_score": {
                        "query": {"match_all": {}},
                        "script": {
                            "source": "cosineSimilarity(params.query_vector, 'vector') + 1.0",
                            "params": {"query_vector": query_embedding},
                        },
                    }
                },
                "_source": ["text", "metadata"],
            }
            response = self.client.search(index=collection_name, body=script_query)

        hits = response.get("hits", {}).get("hits", [])
        return [
            {
                "text": hit.get("_source", {}).get("text", ""),
                "metadata": hit.get("_source", {}).get("metadata", {}),
                "score": hit.get("_score", 0),
            }
            for hit in hits
        ]

    async def delete_collection(self, collection_name: str):
        self.client.indices.delete(index=collection_name, ignore=[404])

    def get_collection_info(self, collection_name: str) -> Dict:
        if not self.client.indices.exists(index=collection_name):
            return {}
        stats = self.client.count(index=collection_name)
        return {
            "name": collection_name,
            "vectors_count": stats.get("count", 0),
            "points_count": stats.get("count", 0),
        }


class VectorStoreService:
    """Service for vector database operations via pluggable providers."""

    def __init__(self, org_id: Optional[str] = None, provider: Optional[str] = None):
        self.org_id = org_id
        self.billing_mode = self._get_billing_mode(org_id)
        self.openai_api_key = self._get_openai_key(org_id)

        if self.openai_api_key:
            openai.api_key = self.openai_api_key

        self.embedding_model = get_effective_rag_embedding_model_id()
        self.embedding_dim = self._resolve_embedding_dim(self.embedding_model)

        resolved_provider = (provider or resolve_vector_provider()).strip().lower()
        if resolved_provider == "qdrant":
            self.backend = QdrantVectorBackend(embedding_dim=self.embedding_dim)
        elif resolved_provider == "opensearch":
            self.backend = OpenSearchVectorBackend(embedding_dim=self.embedding_dim)
        else:
            raise RuntimeError(f"Unsupported vector provider: {resolved_provider}")

        self.provider = resolved_provider

    def _get_billing_mode(self, org_id: Optional[str]) -> str:
        if not org_id:
            return "own_key"

        try:
            oid = ObjectId(org_id) if isinstance(org_id, str) else org_id
            org = organizations_collection.find_one({"_id": oid}, {"ai_credits.mode": 1})
            mode = str(((org or {}).get("ai_credits") or {}).get("mode") or "own_key").strip().lower()
            return "platform" if mode == "platform" else "own_key"
        except Exception as error:
            logger.warning("Error fetching billing mode for vector store: %s", error)
            return "own_key"

    def _get_openai_key(self, org_id: Optional[str]) -> Optional[str]:
        """Fetch API key based on billing mode for vector operations."""
        if self.billing_mode == "platform":
            master_key = os.getenv("MASTER_OPENAI_KEY", "").strip()
            if not master_key:
                logger.error("MASTER_OPENAI_KEY not set for platform embedding mode")
                return None
            return master_key

        if org_id:
            try:
                oid = ObjectId(org_id) if isinstance(org_id, str) else org_id
                org = organizations_collection.find_one({"_id": oid})
                if org and org.get("openai_api_key"):
                    return org.get("openai_api_key")
            except Exception as error:
                logger.warning("Error fetching org API key: %s", error)
        return None

    def _resolve_embedding_model(self, embedding_model: Optional[str] = None) -> str:
        model = str(embedding_model or "").strip()
        if model:
            return model
        default_model = str(self.embedding_model or "").strip()
        return default_model or "text-embedding-3-small"

    def _resolve_embedding_dim(self, embedding_model: Optional[str]) -> int:
        model_id = str(embedding_model or "").strip()
        if model_id in EMBEDDING_MODEL_DIMENSIONS:
            return EMBEDDING_MODEL_DIMENSIONS[model_id]
        return 1536

    def _get_embedding_credit_cost_sync(self, model_id: str, input_tokens: int) -> float:
        pricing = ai_model_pricing_collection.find_one({"model_id": model_id})
        if not pricing:
            logger.warning("No embedding pricing configured for model %s", model_id)
            return 0.0

        input_rate = Decimal(str(pricing.get("credits_per_1k_input_tokens") or 0))
        input_tokens_decimal = Decimal(str(max(0, int(input_tokens or 0))))
        total_cost = (input_tokens_decimal / Decimal("1000")) * input_rate
        rounded_total = total_cost.quantize(CREDIT_PRECISION, rounding=ROUND_HALF_UP)
        if rounded_total <= 0:
            return 0.0
        final_cost = max(MIN_CREDIT_DEDUCTION, rounded_total).quantize(CREDIT_PRECISION, rounding=ROUND_HALF_UP)
        return float(final_cost)

    def _deduct_embedding_credits_sync(self, model_id: str, input_tokens: int, meta: Optional[Dict[str, Any]] = None) -> None:
        if not self.org_id or self.billing_mode != "platform":
            return

        cost = self._get_embedding_credit_cost_sync(model_id, input_tokens)
        if cost <= 0:
            return

        oid = ObjectId(self.org_id) if isinstance(self.org_id, str) else self.org_id
        # Atomic compare-and-set: the balance check and the deduction happen
        # in one update. If two concurrent calls race, only one can match
        # {"$gte": cost} against the *current* balance and modify a document.
        result = organizations_collection.update_one(
            {"_id": oid, "ai_credits.balance": {"$gte": cost}},
            {
                "$inc": {
                    "ai_credits.balance": -cost,
                    "ai_credits.lifetime_used": cost,
                }
            },
        )

        if result.modified_count == 0:
            org = organizations_collection.find_one({"_id": oid}, {"ai_credits": 1})
            balance = _round_credit_value(((org or {}).get("ai_credits") or {}).get("balance") or 0)
            raise InsufficientCreditsError(
                f"Insufficient AI credits for embeddings. Need {cost:.4f}, have {balance:.4f}."
            )

        org_after = organizations_collection.find_one({"_id": oid}, {"ai_credits": 1})
        balance_after = _round_credit_value(((org_after or {}).get("ai_credits") or {}).get("balance") or 0)

        txn = {
            "organization_id": oid,
            "type": "ai_embedding_deduction",
            "credits_delta": -cost,
            "balance_after": balance_after,
            "description": f"Embedding usage via {model_id}",
            "meta": {
                "model": model_id,
                "input_tokens": int(input_tokens or 0),
                **(meta or {}),
            },
            "created_at": datetime.utcnow(),
        }
        credit_transactions_collection.insert_one(txn)

    async def create_collection(self, collection_name: str, embedding_model: Optional[str] = None):
        try:
            model_id = self._resolve_embedding_model(embedding_model)
            vector_dim = self._resolve_embedding_dim(model_id)
            await self.backend.create_collection(collection_name, embedding_dim=vector_dim)
        except Exception as error:
            logger.error("Error creating vector collection: %s", error)
            raise

    async def add_documents(
        self,
        collection_name: str,
        documents: List[Dict[str, str]],
        embedding_model: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> int:
        if not self.openai_api_key:
            raise ValueError("OpenAI API Key not configured for this organization.")

        if not documents:
            return 0

        try:
            model_id = self._resolve_embedding_model(embedding_model)
            await self.create_collection(collection_name, embedding_model=model_id)
            texts = [doc["text"] for doc in documents]
            embeddings = await self._generate_embeddings(
                texts,
                embedding_model=model_id,
                operation="index",
                meta={
                    "collection_name": collection_name,
                    "document_count": len(documents),
                    **(metadata or {}),
                },
            )
            return await self.backend.add_documents(collection_name, documents, embeddings)
        except Exception as error:
            logger.error("Error adding documents: %s", error)
            raise

    async def search(
        self,
        collection_name: str,
        query: str,
        top_k: int = 5,
        embedding_model: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[Dict]:
        if not self.openai_api_key:
            logger.warning("OpenAI API Key missing for search")
            return []

        try:
            model_id = self._resolve_embedding_model(embedding_model)
            query_embedding = await self._generate_embeddings(
                [query],
                embedding_model=model_id,
                operation="search",
                meta={
                    "collection_name": collection_name,
                    "query_length": len(str(query or "")),
                    **(metadata or {}),
                },
            )
            return await self.backend.search(
                collection_name=collection_name,
                query_embedding=query_embedding[0],
                top_k=top_k,
            )
        except InsufficientCreditsError as error:
            logger.warning("Insufficient credits for embedding search: %s", error)
            return []
        except Exception as error:
            logger.error("Error searching vector store: %s", error)
            return []

    async def delete_collection(self, collection_name: str):
        try:
            await self.backend.delete_collection(collection_name)
        except Exception as error:
            logger.error("Error deleting collection: %s", error)
            raise

    async def _generate_embeddings(
        self,
        texts: List[str],
        embedding_model: Optional[str] = None,
        operation: str = "index",
        meta: Optional[Dict[str, Any]] = None,
    ) -> List[List[float]]:
        try:
            if not texts:
                return []

            import httpx
            from openai import AsyncOpenAI

            model_id = self._resolve_embedding_model(embedding_model)
            verify_ssl = _is_truthy(os.getenv("VERIFY_SSL"), default=True)

            async with httpx.AsyncClient(trust_env=False, verify=verify_ssl) as http_client:
                client = AsyncOpenAI(
                    api_key=self.openai_api_key,
                    http_client=http_client,
                )

                response = await client.embeddings.create(
                    model=model_id,
                    input=texts,
                )

                usage = response.usage
                prompt_tokens = 0
                if usage is not None:
                    if isinstance(usage, dict):
                        prompt_tokens = int(usage.get("prompt_tokens") or 0)
                    else:
                        prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)

                if self.billing_mode == "platform" and prompt_tokens > 0:
                    await asyncio.to_thread(
                        self._deduct_embedding_credits_sync,
                        model_id,
                        prompt_tokens,
                        {
                            "operation": str(operation or "index"),
                            "text_count": len(texts),
                            **(meta or {}),
                        },
                    )

                return [item.embedding for item in response.data]
        except InsufficientCreditsError:
            raise
        except Exception as error:
            logger.error("Error generating embeddings: %s", error)
            raise

    def get_collection_info(self, collection_name: str) -> Dict:
        try:
            return self.backend.get_collection_info(collection_name)
        except Exception as error:
            logger.error("Error getting collection info: %s", error)
            return {}
