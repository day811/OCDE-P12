# src/rag/rag_engine.py
import logging
import time
import json

from pathlib import Path
from typing import Dict, Optional
from datetime import datetime

from engine import RAGEngine
from src.utils.token_accounting import get_accounting
from config import Config
from engine import get_llm


logger = logging.getLogger(__name__)

class SearchBot:
    """Orchestrates RAG pipeline: parsing -> retrieval -> context -> LLM"""
    
    def __init__(self, embedder:str, snapshot_date: Optional[str] = None, environment: str = 'prod'):
        self.snapshot_date = snapshot_date or datetime.now().strftime('%Y-%m-%d')
        self.environment = environment
        self.embedder =embedder
        
        # ✅ INITIALIZE LLM FROM CONFIG
        self.search_llm = get_llm(
            temperature=Config.LLM_TEMPERATURE,
            provider=Config.LLM_PROVIDER
        )
        
        if embedder != Config.LLM_PROVIDER:
            self.embed_llm = get_llm(
                temperature=Config.LLM_TEMPERATURE,
                provider=embedder
            )
        else:
            self.embed_llm = self.search_llm
            
        
        
        
        # Load metadata
        
        self.rag_engine = RAGEngine()

        
    
    def embed_query(self, query_text: str) -> list:
        """Embed query using LLM"""

        return self.embed_llm.embed(query_text)
        
    
    def answer_question(self,

        question: str,
        top_k: int = 5,
        temperature: float = 0.7
    ) -> Dict:
        """
        Answer a user question by retrieving relevant chunks from a RAG engine and generating a response using an LLM.
        This method orchestrates the following pipeline:
        1. Parse constraints (date, city, department) from the question
        2. Retrieve relevant chunks from the vector database using FAISS
        3. Build context from the retrieved chunks
        4. Generate an answer using the LLM based on the context
        5. Format and return the response with sources and metadata
        Args:
            question (str): The user's question to answer.
            top_k (int, optional): Maximum number of chunks to retrieve and include in sources. Defaults to 5.
            temperature (float, optional): Temperature parameter for LLM generation controlling randomness. Defaults to 0.7.
        Returns:
            Dict: A dictionary containing:
                - answer (str): The generated answer from the LLM
                - sources (list): List of source chunks with metadata (event_id, title, city, dept, address, dates, url, distance, top_k)
                - constraints (dict): Parsed constraints from the question
                - mode (str): Mode of operation ('search')
                - query_tokens (int): Number of tokens used for the query embedding
                - context_tokens (int): Estimated number of tokens in the context
                - llm_tokens (int): Estimated number of tokens in the LLM response
                - faiss_time (float): Time taken for FAISS retrieval in seconds
        Raises:
            Exception: If an error occurs during question answering, the exception is logged and re-raised.
        """        
        try:
            # Step 1: Parse constraints
          
            # Step 2: Retrieve chunks
            result = self.rag_engine.retrieve(
                query_text=question,
                k=top_k,
           )

            chunks = result['chunks'][:top_k]
            embed_tokens = result['embed_tokens']
            faiss_time = result['faiss_time']
            
            # Step 3: Build context
            context = self.rag_engine.build_context(chunks)
            
            # Step 4: Generate answer with LLM
            prompt = self._build_prompt(question, context)
            answer = self._generate_answer(prompt,temperature = temperature)

            # Step 5: Format response
            sources = []
            for chunk in chunks:
                sources.append(
                    {
                        'event_id': chunk.get('event_id'),
                        'title': chunk.get('title'),
                        'city': chunk.get('city'),
                        'dept': chunk.get('dept'),
                        'address' : chunk.get('dept'),
                        'url': chunk.get('url'),
                        'distance': chunk.get('distance'),
                        'top_k' :top_k
                    }
                )
            
            
            # ✅ LOG TOKENS
            query_tokens = embed_tokens
            context_tokens = int(len(context.split()) * 1.3)
            llm_tokens = int(len(answer.split()) * 1.3)
            
           
            return {
                'answer': answer,
                'sources': sources,
                'mode': 'search',
                'query_tokens' : query_tokens,
                'context_tokens' : context_tokens,
                'llm_tokens' : llm_tokens,
                'faiss_time' : faiss_time,
            }
        
        except Exception as e:
            logger.error(f"Error in answer_question: {e}")
            raise
    
    def _build_prompt(self, question: str, context: str) -> str:
        """Build prompt for LLM"""
        return f"""Tu es un assistant pour recommander des événements.

Contexte (événements trouvés):
{context}

Question: {question}

Basé sur le contexte, fournis une réponse concise recommandant les événements pertinents."""
    
    def _generate_answer(self, prompt: str, temperature: float = 0.7) -> str:
        """Generate answer using LLM"""
        try:
            return self.search_llm.generate(prompt, temperature=temperature)

        except Exception as e:
            logger.error(f"LLM generation error: {e}")
            return "Désolé, je n'ai pas pu générer une réponse."
