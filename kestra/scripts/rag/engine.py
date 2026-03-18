# src/rag/retriever.py
import numpy as np
from typing import List, Dict, Optional, Callable, Tuple
from datetime import datetime
from config import Config
from abc import ABC, abstractmethod
from mistral_llm import MistralLLM
from gemini_llm import GeminiLLM
import logging, sys 

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout  
)
logger = logging.getLogger("sds.infra.common_tools")


class RAGEngine:
    """
    RAGEngine: Retrieve and Augment Generation Engine
    A retrieval system that integrates FAISS vector search with metadata filtering
    to retrieve and rank relevant chunks based on semantic similarity and constraints.
    Features:
        - Vector similarity search using FAISS indices
        - Multi-constraint filtering (date, city, department)
        - Distance-based relevance ranking
        - Formatted context building for LLM consumption
    Attributes:
        faiss_index: FAISS index for vector similarity search
        metadata (Dict): Mapping of chunk IDs to metadata dictionaries
        embed_function (Callable): Function to embed text queries
        top_k (int): Default number of results to retrieve (default: 5)
    Methods:
        retrieve: Main entry point for searching and filtering chunks
        _filter_chunks: Apply constraint-based filtering to search results
        _matches_date: Check if chunk date falls within specified range
        _matches_city: Check if chunk city/address matches target city
        _matches_dept: Check if chunk department matches target department
        build_context: Format retrieved chunks into readable context string
    """

    
    def __init__(self):
        """
        Initialize the RAG engine with FAISS index and retrieval configuration.
        Args:
            faiss_index: FAISS index object for similarity search.
            metadata (Dict): Dictionary containing metadata associated with indexed documents.
            embed_function (Callable): Function to generate embeddings for queries and documents.
            top_k (int, optional): Number of top results to retrieve. Defaults to 5.
        """
    
    

    def retrieve(
        self,
        query_text: str,
        k: int = 0,
    ) -> Dict :
        """
        Retrieve and filter chunks based on semantic similarity and optional constraints.
        Args:
            query_text (str): The query text to search for.
            k (int, optional): Number of top results to return. Defaults to 0, which uses self.top_k.
            date_constraint (Optional[tuple[datetime, int]], optional): Tuple containing a datetime and an integer 
                for filtering results by date range. Defaults to None.
            city_constraint (Optional[str], optional): City name to filter results by. Defaults to None.
            dept_constraint (Optional[str], optional): Department code to filter results by. Defaults to None.
        Returns:
            Dict: A dictionary containing:
                - 'chunks': List of filtered chunk metadata dictionaries sorted by relevance, limited to k results.
                - 'embed_tokens': Approximate number of tokens used in the query embedding.
                - 'faiss_time': Execution time for the FAISS search in milliseconds.
        Process:
            1. Augments query text with date constraints if provided.
            2. Generates embedding for the augmented query.
            3. Performs vector similarity search using FAISS index (searches k*70 results, minimum 1000).
            4. Deduplicates results by event_id and assigns rankings based on distance scores.
            5. Applies spatial and temporal filters based on provided constraints.
            6. Returns top k filtered results with metadata.
        """
        

        start_time = datetime.now()

        exec_time =  (datetime.now() - start_time       ).total_seconds() * 1000
 
        # Search Faiss
        search_k = max(k * 70,1000)
        
        # Build results with distances
        results = []
        seen= []

        
        
        
        return {'chunks' : [], 
                    'faiss_time' : exec_time
                }
    


    def build_context(self,chunks: list[Dict]) -> str:
    
        """
        Build a formatted context string from a list of event chunks and constraints.
        This method takes a list of event data chunks and constraint filters, then creates
        a human-readable formatted string containing relevant event information. Each event
        is numbered and includes title, location, dates, relevance score, description, and URL.
        Args:
            chunks (list[Dict]): A list of dictionaries containing event data. Each dictionary
                should have keys: 'title', 'city', 'text', 'url', and optionally 'distance'.
            constraints (dict): A dictionary containing filter constraints, including a 'date'
                key used for filtering events by date.
        Returns:
            str: A formatted markdown-style string containing:
                - A message if no chunks are found
                - Numbered list of events with:
                    - Title (bold)
                    - Location (📍 emoji)
                    - Dates (📅 emoji, formatted as DD/MM/YYYY, HH:MM:SS)
                    - Relevance percentage (⭐ emoji, calculated from distance)
                    - Event description text
                    - URL link (🔗 emoji) if available
            All text is in French with event details separated by newlines.
        """
    
        if not chunks:
            return "Aucun événement n'a été trouvé pour votre recherche."
        
        context = "Voici les événements pertinents trouvés :\n\n"
        
        for i, chunk in enumerate(chunks, 1):
            title = chunk.get('title', 'Sans titre')
            city = chunk.get('city', 'Lieu non spécifié')
            text = chunk.get('text', 'Description non disponible')
            url = chunk.get('url', '')
            distance = chunk.get('distance')
            
            context += f"{i}. **{title}**\n"
            context += f"   📍 Lieu: {city}\n"
            
            if distance:
                relevance = int(distance * 100)
                context += f"   ⭐ Pertinence: {relevance}%\n"
            
            context += f"\n   {text}\n"
            
            if url:
                context += f"   🔗 {url}\n"
            
            context += "\n"
        
        return context


class BaseLLM(ABC):
    """Abstract base class for LLM providers"""
    
    def __init__(self, temperature: float = 0.7):
        self.temperature = temperature

    @abstractmethod
    def generate(self, prompt: str, temperature: float = 0.7) -> str:
        """Generate text from prompt"""
        pass
    
    @abstractmethod
    def embed(self, text) :
        """Generate embedding for text"""
        return text
    
class LLMFactory:
    """Factory for creating LLM instances"""
    
    PROVIDERS = {
        'mistral': MistralLLM,
        'gemini': GeminiLLM
    }
    
    @staticmethod
    def create_llm(temperature: float = 0.7, provider:str=""):
        """Create LLM instance based on provider"""
        if not provider:
            provider = Config.LLM_PROVIDER
        
        if provider not in LLMFactory.PROVIDERS:
            raise ValueError(f"Unknown provider: {provider}. Available: {list(LLMFactory.PROVIDERS.keys())}")
        
        logger.info(f"Creating LLM instance - Provider: {provider}")
        return LLMFactory.PROVIDERS[provider](temperature=temperature )


def get_llm(temperature: float = 0.7, provider:str=Config.LLM_PROVIDER):
    """Convenience function to get LLM instance"""

    return LLMFactory.create_llm( temperature, provider)


