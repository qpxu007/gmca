import os
import glob
import numpy as np
import hashlib
import pickle
import redis
import requests
import json
from typing import List, Dict, Any, Optional
from openai import OpenAI
from qp2.log.logging_config import get_logger

logger = get_logger(__name__)

class CodebaseRAG:
    def __init__(self, client: OpenAI, embedding_model: str = "text-embedding-3-small", redis_client: Optional[redis.Redis] = None):
        self.client = client
        self.embedding_model = embedding_model
        self.knowledge_base: List[Dict[str, Any]] = []
        self.redis_client = redis_client
        # Construct embeddings URL from the base_url of the OpenAI client
        base_url = str(self.client.base_url)
        if base_url.endswith('/'):
            base_url = base_url[:-1]
        self.embedding_endpoint = f"{base_url}/embeddings"
        
    def _get_embedding(self, text: str) -> List[float]:
        """Helper to get embedding using requests, matching the user's working example."""
        payload = {
            "model": self.embedding_model,
            "input": [text], # API expects a list
        }
        headers = {"Content-Type": "application/json"}
        # Add auth header if API key is present
        if self.client.api_key:
             headers["Authorization"] = f"Bearer {self.client.api_key}"

        try:
            response = requests.post(self.embedding_endpoint, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            # Extract embedding from the response structure: {"data": [{"embedding": [...], ...}]}
            return data['data'][0]['embedding']
        except Exception as e:
            logger.error(f"Error getting embedding from {self.embedding_endpoint}: {e}")
            if 'response' in locals():
                logger.error(f"Response: {response.text}")
            raise e

    def _get_redis_key(self, root_dir: str) -> str:
        """Generates a unique Redis key for the directory."""
        path_hash = hashlib.md5(os.path.abspath(root_dir).encode()).hexdigest()
        return f"rag:kb:{path_hash}"

    def save_to_redis(self, root_dir: str):
        """Saves the current knowledge base to Redis."""
        if not self.redis_client or not self.knowledge_base:
            return
        
        key = self._get_redis_key(root_dir)
        try:
            # Pickle the entire knowledge base list
            data = pickle.dumps(self.knowledge_base)
            self.redis_client.set(key, data)  # no TTL — recalculation is expensive
            logger.info(f"Saved RAG knowledge base to Redis: {key}")
        except Exception as e:
            logger.error(f"Error saving to Redis: {e}")

    def load_from_redis(self, root_dir: str) -> bool:
        """Loads the knowledge base from Redis if available."""
        if not self.redis_client:
            return False
            
        key = self._get_redis_key(root_dir)
        try:
            data = self.redis_client.get(key)
            if data:
                self.knowledge_base = pickle.loads(data)
                logger.info(f"Loaded {len(self.knowledge_base)} chunks from Redis: {key}")
                return True
        except Exception as e:
            logger.error(f"Error loading from Redis: {e}")
            
        return False

    def index_directory(self, root_dir: str, file_extensions: List[str] = [".py", ".md"], chunk_size: int = 1000, force_refresh: bool = False):
        """
        Scans a directory, reads files, chunks them, and creates embeddings.
        Checks Redis cache first unless force_refresh is True.
        """
        # Try loading from Redis first
        if not force_refresh and self.load_from_redis(root_dir):
            return

        self.knowledge_base = []
        
        all_files = []
        for ext in file_extensions:
            # Recursive glob for files
            all_files.extend(glob.glob(os.path.join(root_dir, "**", f"*{ext}"), recursive=True))
            
        total_files = len(all_files)
        logger.info(f"Found {total_files} files to index in {root_dir}")
        
        for idx, file_path in enumerate(all_files):
            if idx % 10 == 0:
                logger.info(f"Indexing file {idx+1}/{total_files}: {file_path}")
                
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    
                # Simple chunking (can be improved with AST parsing)
                chunks = self._chunk_text(content, chunk_size)
                
                for i, chunk in enumerate(chunks):
                    # Get embedding from API
                    embedding = self._get_embedding(chunk)
                    
                    self.knowledge_base.append({
                        "file_path": file_path,
                        "chunk_index": i,
                        "content": chunk,
                        "embedding": np.array(embedding)
                    })
                    
            except Exception as e:
                logger.error(f"Error indexing {file_path}: {e}")
                    
        logger.info(f"Indexed {len(self.knowledge_base)} chunks from {root_dir}")
        
        # Save to Redis after indexing
        self.save_to_redis(root_dir)

    def _chunk_text(self, text: str, size: int) -> List[str]:
        """Splits text into chunks of roughly 'size' characters."""
        return [text[i:i+size] for i in range(0, len(text), size)]

    def retrieve(self, query: str, top_k: int = 3) -> List[Dict[str, Any]]:
        """
        Finds the most relevant code chunks for a user query.
        """
        if not self.knowledge_base:
            return []

        # Embed the query
        query_vec = np.array(self._get_embedding(query))

        # Calculate Cosine Similarity
        # Sim(A, B) = dot(A, B) / (norm(A) * norm(B))
        scored_chunks = []
        
        query_norm = np.linalg.norm(query_vec)
        
        for item in self.knowledge_base:
            doc_vec = item["embedding"]
            doc_norm = np.linalg.norm(doc_vec)
            
            if doc_norm == 0 or query_norm == 0:
                similarity = 0
            else:
                similarity = np.dot(query_vec, doc_vec) / (query_norm * doc_norm)
                
            scored_chunks.append((similarity, item))

        # Sort by similarity (descending) and take top K
        scored_chunks.sort(key=lambda x: x[0], reverse=True)
        
        return [item for score, item in scored_chunks[:top_k]]

    def build_context_string(self, query: str, top_k: int = 3) -> str:
        """Retrieves relevant chunks and formats them into a context string."""
        relevant_items = self.retrieve(query, top_k)
        
        context_parts = []
        for item in relevant_items:
            context_parts.append(
                f"--- Snippet from {os.path.basename(item['file_path'])} ---\n{item['content']}\n"
            )
            
        return "\n".join(context_parts)
