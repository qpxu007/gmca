#!/usr/bin/env python
"""
Integration tests for the direct Argo API service (replacing the local proxy).

These hit the live Argo API and require ANL network access + valid $USER.

Run all tests:
    python qp2/image_viewer/ai/test_argo_api.py

Run a specific test class:
    python qp2/image_viewer/ai/test_argo_api.py TestChatStreaming
    python qp2/image_viewer/ai/test_argo_api.py TestEmbeddings

Override defaults via env vars:
    ARGO_BASE_URL=https://apps-dev.inside.anl.gov/argoapi/v1 \\
    ARGO_CHAT_MODEL=gpt5mini \\
    python qp2/image_viewer/ai/test_argo_api.py
"""

import os
import sys
import time
import unittest
import requests
from openai import OpenAI

# ---------------------------------------------------------------------------
# Configuration
#
# The OpenAI-compatible endpoints live on apps-dev (not prod).
# Prod only exposes the legacy /resource/chat/ endpoint.
# Model names: use internal_id values (e.g. "gpt4turbo", "gpt5mini").
# ---------------------------------------------------------------------------

ARGO_BASE_URL = os.environ.get(
    "ARGO_BASE_URL", "https://apps-dev.inside.anl.gov/argoapi/v1"
)
ARGO_API_KEY = os.environ.get("AI_API_KEY", os.environ.get("USER", ""))

# Default models — internal_id format (what the proxy uses with "argo:" prefix)
CHAT_MODEL = os.environ.get("ARGO_CHAT_MODEL", "gpt4turbo")
EMBEDDING_MODEL = os.environ.get("ARGO_EMBED_MODEL", "text-embedding-3-small")


def _make_client(base_url=None):
    """Create an OpenAI client pointed at Argo."""
    return OpenAI(api_key=ARGO_API_KEY, base_url=base_url or ARGO_BASE_URL)


# ---------------------------------------------------------------------------
# 1. Authentication & connectivity
# ---------------------------------------------------------------------------

class TestConnection(unittest.TestCase):
    """Verify basic connectivity and auth against Argo API."""

    def test_api_key_is_set(self):
        self.assertTrue(ARGO_API_KEY, "No API key. Set AI_API_KEY or USER env var.")

    def test_models_endpoint_reachable(self):
        """GET /models should return available models."""
        headers = {"Authorization": f"Bearer {ARGO_API_KEY}"}
        resp = requests.get(f"{ARGO_BASE_URL}/models", headers=headers, timeout=15)
        self.assertEqual(resp.status_code, 200, f"Got {resp.status_code}: {resp.text[:200]}")
        data = resp.json()
        # /models returns display names as "id" and short names as "internal_id"
        models = data.get("data", [])
        self.assertGreater(len(models), 0, "No models returned")
        display_names = [m["id"] for m in models]
        internal_ids = [m.get("internal_id", "") for m in models]
        print(f"\n  Found {len(models)} models")
        print(f"  Display names: {display_names[:5]}...")
        print(f"  Internal IDs:  {internal_ids[:5]}...")

    def test_chat_model_available(self):
        """Configured chat model should exist (by internal_id)."""
        headers = {"Authorization": f"Bearer {ARGO_API_KEY}"}
        resp = requests.get(f"{ARGO_BASE_URL}/models", headers=headers, timeout=15)
        data = resp.json()
        internal_ids = [m.get("internal_id", "") for m in data.get("data", [])]
        self.assertIn(CHAT_MODEL, internal_ids,
                      f"'{CHAT_MODEL}' not in internal_ids: {internal_ids}")

    def test_embedding_model_available(self):
        """Configured embedding model should exist."""
        headers = {"Authorization": f"Bearer {ARGO_API_KEY}"}
        resp = requests.get(f"{ARGO_BASE_URL}/models", headers=headers, timeout=15)
        data = resp.json()
        # Embedding models may use a different naming — check both id and internal_id
        all_ids = set()
        for m in data.get("data", []):
            all_ids.add(m.get("id", ""))
            all_ids.add(m.get("internal_id", ""))
        # text-embedding-3-small may appear as internal_id "v3small" or similar
        # The actual call works regardless, so just verify the endpoint accepts it
        resp2 = requests.post(
            f"{ARGO_BASE_URL}/embeddings",
            headers={**headers, "Content-Type": "application/json"},
            json={"model": EMBEDDING_MODEL, "input": ["test"]},
            timeout=15,
        )
        self.assertEqual(resp2.status_code, 200,
                         f"Embedding model '{EMBEDDING_MODEL}' not accepted: {resp2.text[:200]}")


# ---------------------------------------------------------------------------
# 2. Chat completions (non-streaming)
# ---------------------------------------------------------------------------

class TestChatCompletion(unittest.TestCase):
    """Chat completions against the direct Argo API."""

    @classmethod
    def setUpClass(cls):
        cls.client = _make_client()

    def test_simple_chat(self):
        response = self.client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": "Reply in one sentence."},
                {"role": "user", "content": "What is 2+2?"},
            ],
        )
        self.assertTrue(response.choices, "No choices in response")
        content = response.choices[0].message.content
        self.assertTrue(content, "Empty response")
        print(f"\n  Chat response: {content[:200]}")

    def test_multi_turn_conversation(self):
        """Multi-turn context (matches how the app sends history)."""
        messages = [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "My name is TestUser."},
            {"role": "assistant", "content": "Hello TestUser! How can I help you?"},
            {"role": "user", "content": "What is my name?"},
        ]
        response = self.client.chat.completions.create(
            model=CHAT_MODEL, messages=messages,
        )
        content = response.choices[0].message.content
        self.assertIn("TestUser", content,
                      f"Model didn't recall name. Response: {content}")


# ---------------------------------------------------------------------------
# 3. Chat streaming — primary mode used by AIClient.generate_code()
# ---------------------------------------------------------------------------

class TestChatStreaming(unittest.TestCase):
    """Streaming chat completions."""

    @classmethod
    def setUpClass(cls):
        cls.client = _make_client()

    def test_streaming_yields_chunks(self):
        stream = self.client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[{"role": "user", "content": "Say hello in exactly three words."}],
            stream=True,
        )
        chunks = []
        for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                chunks.append(chunk.choices[0].delta.content)

        full = "".join(chunks)
        self.assertGreater(len(chunks), 0, "No chunks received")
        self.assertTrue(full, "Empty concatenated response")
        print(f"\n  Streaming ({len(chunks)} chunks): {full[:200]}")

    def test_streaming_finish_reason_stop(self):
        stream = self.client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[{"role": "user", "content": "Say 'ok'."}],
            stream=True,
        )
        finish_reason = None
        for chunk in stream:
            if chunk.choices and chunk.choices[0].finish_reason:
                finish_reason = chunk.choices[0].finish_reason
        self.assertEqual(finish_reason, "stop",
                         f"Expected finish_reason='stop', got '{finish_reason}'")


# ---------------------------------------------------------------------------
# 4. Embeddings — used by rag_helper.py (CodebaseRAG)
# ---------------------------------------------------------------------------

class TestEmbeddings(unittest.TestCase):
    """Embedding generation via the Argo API."""

    @classmethod
    def setUpClass(cls):
        cls.client = _make_client()

    def test_single_embedding_openai_sdk(self):
        response = self.client.embeddings.create(
            model=EMBEDDING_MODEL,
            input="Argonne National Laboratory is a multidisciplinary research center.",
        )
        self.assertTrue(response.data, "No embedding data")
        emb = response.data[0].embedding
        self.assertGreater(len(emb), 0, "Empty embedding")
        self.assertIsInstance(emb[0], float)
        print(f"\n  Embedding dim={len(emb)}, preview={emb[:3]}")

    def test_batch_embedding(self):
        """Multiple inputs → one vector per input."""
        inputs = ["First sentence.", "Second sentence.", "Third sentence."]
        response = self.client.embeddings.create(
            model=EMBEDDING_MODEL, input=inputs,
        )
        self.assertEqual(len(response.data), len(inputs),
                         f"Expected {len(inputs)}, got {len(response.data)}")
        dims = {len(d.embedding) for d in response.data}
        self.assertEqual(len(dims), 1, f"Inconsistent dims: {dims}")

    def test_embedding_via_raw_requests(self):
        """Raw requests — mirrors rag_helper.py's _get_embedding."""
        payload = {
            "model": EMBEDDING_MODEL,
            "input": ["Test embedding via raw requests."],
        }
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {ARGO_API_KEY}",
        }
        resp = requests.post(
            f"{ARGO_BASE_URL}/embeddings",
            headers=headers, json=payload, timeout=30,
        )
        self.assertEqual(resp.status_code, 200,
                         f"Failed: {resp.status_code} {resp.text[:200]}")
        emb = resp.json()["data"][0]["embedding"]
        self.assertGreater(len(emb), 0)
        print(f"\n  Raw request embedding dim={len(emb)}")


# ---------------------------------------------------------------------------
# 5. Model compatibility — same call patterns as the proxy
# ---------------------------------------------------------------------------

class TestModelCompatibility(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = _make_client()

    def test_temperature_param(self):
        response = self.client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[{"role": "user", "content": "Say 'hello'."}],
            temperature=0.1,
        )
        self.assertTrue(response.choices[0].message.content)

    def test_system_message(self):
        """System messages work (some Argo models don't support them)."""
        response = self.client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": "Always respond with exactly 'PONG'."},
                {"role": "user", "content": "PING"},
            ],
        )
        self.assertTrue(response.choices[0].message.content)


# ---------------------------------------------------------------------------
# 6. Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling(unittest.TestCase):

    def test_invalid_model_raises(self):
        client = _make_client()
        with self.assertRaises(Exception):
            client.chat.completions.create(
                model="nonexistent-model-xyz-999",
                messages=[{"role": "user", "content": "hello"}],
            )

    def test_empty_messages_raises(self):
        client = _make_client()
        with self.assertRaises(Exception):
            client.chat.completions.create(
                model=CHAT_MODEL, messages=[],
            )


# ---------------------------------------------------------------------------
# 7. Performance — latency acceptable for interactive use
# ---------------------------------------------------------------------------

class TestPerformance(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = _make_client()

    def test_chat_latency_under_30s(self):
        t0 = time.time()
        response = self.client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[{"role": "user", "content": "What is 1+1?"}],
        )
        elapsed = time.time() - t0
        self.assertTrue(response.choices[0].message.content)
        self.assertLess(elapsed, 30, f"Took {elapsed:.1f}s")
        print(f"\n  Chat latency: {elapsed:.2f}s")

    def test_streaming_ttft_under_10s(self):
        """Time-to-first-token should be under 10s."""
        t0 = time.time()
        stream = self.client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[{"role": "user", "content": "Say 'hi'."}],
            stream=True,
        )
        ttft = None
        for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                ttft = time.time() - t0
                break
        # Drain remaining
        for _ in stream:
            pass
        self.assertIsNotNone(ttft, "No content chunks received")
        self.assertLess(ttft, 10, f"TTFT={ttft:.2f}s — too slow")
        print(f"\n  Time to first token: {ttft:.2f}s")


# ---------------------------------------------------------------------------
# 8. Proxy-to-Argo mapping — verify the model name translation
# ---------------------------------------------------------------------------

class TestProxyMapping(unittest.TestCase):
    """
    The current proxy uses 'argo:' prefixed model names (e.g. 'argo:gpt-5-mini').
    Argo API uses internal_ids (e.g. 'gpt5mini').
    Verify the mapping for models we actually use.
    """

    PROXY_TO_ARGO = {
        # proxy model name -> argo internal_id
        "argo:gpt-5-mini": "gpt5mini",
        "argo:gpt-4-turbo": "gpt4turbo",
        "argo:gpt-4o": "gpt4o",
    }

    @classmethod
    def setUpClass(cls):
        cls.client = _make_client()

    def test_mapped_models_exist(self):
        """All mapped Argo internal_ids should be in the models list."""
        headers = {"Authorization": f"Bearer {ARGO_API_KEY}"}
        resp = requests.get(f"{ARGO_BASE_URL}/models", headers=headers, timeout=15)
        data = resp.json()
        internal_ids = {m.get("internal_id", "") for m in data.get("data", [])}

        for proxy_name, argo_id in self.PROXY_TO_ARGO.items():
            with self.subTest(proxy=proxy_name, argo=argo_id):
                self.assertIn(argo_id, internal_ids,
                              f"Argo ID '{argo_id}' (proxy: '{proxy_name}') not found")

    def test_default_model_chat_works(self):
        """The default proxy model 'argo:gpt-5-mini' → 'gpt5mini' should work."""
        response = self.client.chat.completions.create(
            model="gpt5mini",
            messages=[{"role": "user", "content": "Say 'hello'."}],
        )
        self.assertTrue(response.choices[0].message.content)
        print(f"\n  gpt5mini response: {response.choices[0].message.content[:100]}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Argo API Test Configuration:")
    print(f"  Base URL:       {ARGO_BASE_URL}")
    print(f"  API Key (user): {ARGO_API_KEY}")
    print(f"  Chat model:     {CHAT_MODEL}")
    print(f"  Embed model:    {EMBEDDING_MODEL}")
    print()

    unittest.main(verbosity=2)
