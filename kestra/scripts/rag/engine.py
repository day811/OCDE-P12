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



class RAGEngine:
 
    
    def __init__(self):
        """
        Initialize the RAG engine with FAISS index and retrieval configuration.
        Args:
            faiss_index: FAISS index object for similarity search.
            metadata (Dict): Dictionary containing metadata associated with indexed documents.
            embed_function (Callable): Function to generate embeddings for queries and documents.
            top_k (int, optional): Number of top results to retrieve. Defaults to 5.
        """
        # ✅ INITIALIZE LLM FROM CONFIG
        self.search_llm = get_llm(
            temperature=Config.LLM_TEMPERATURE,
            provider=Config.LLM_PROVIDER
        )
     
    

    def build_context(self, data) -> str:
         
        context = "Voici les événements pertinents trouvés :\n\n"
        

        
        return context

    def answer_question(self,

        question: str,
        temperature: float = 0.7
    ) -> Dict:

        try:
            # Step 1: Parse constraints
          
            # Step 2: Retrieve chunks
            start_time = datetime.now()


            # Step 3: Build context
            context = self.build_context('')
            
            # Step 4: Generate answer with LLM
            prompt = self._build_prompt(question, context)
            answer = self._generate_answer(prompt,temperature = temperature)

            # Step 5: Format response
            sources = []
           
            
            # ✅ LOG TOKENS
            context_tokens = int(len(context.split()) * 1.3)
            llm_tokens = int(len(answer.split()) * 1.3)
            
            exec_time =  (datetime.now() - start_time       ).total_seconds() * 1000
           
            return {
                'answer': answer,
                'sources': sources,
                'mode': 'search',
                'context_tokens' : context_tokens,
                'llm_tokens' : llm_tokens,
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