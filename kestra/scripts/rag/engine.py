# src/rag/retriever.py
import numpy as np
from typing import List, Dict, Optional, Callable, Tuple
from datetime import datetime
from rag.config import Config
from rag.mistral_llm import MistralLLM
#from rag.gemini_llm import GeminiLLM
from rag.config import Config

import logging, sys 
import pandas as pd

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout  
)
logger = logging.getLogger("sds.infra.common_tools")



    
class LLMFactory:
    """Factory for creating LLM instances"""
    
    PROVIDERS = {
        'mistral': MistralLLM,
#        'gemini': GeminiLLM
    }
    
    @staticmethod
    def create_llm(temperature: float = 0.7, provider:str=""):
        """Create LLM instance based on provider"""
        if not provider:
            provider = Config.LLM_PROVIDER
        
        if provider not in LLMFactory.PROVIDERS:
            raise ValueError(f"Unknown provider: {provider}. Available: {list(LLMFactory.PROVIDERS.keys())}")
        
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
   

    def get_comments(self,  context:List[Dict], temperature: float = 0.7
    ) -> Dict:

        try:
            start_time = datetime.now()

            prompt = self._build_prompt(context)
            answer = self._generate_answer(prompt,temperature = temperature)
            sources = []
            # ✅ LOG TOKENS
            context_tokens = int(len(prompt) * 1.3)
            llm_tokens = int(len(answer.split()) * 1.3)
            
            exec_time =  (datetime.now() - start_time       ).total_seconds() * 1000
           
            return {
                'answer': answer,
                'context_tokens' : context_tokens,
                'llm_tokens' : llm_tokens,
            }
        
        except Exception as e:
            logger.error(f"Error in answer_question: {e}")
            raise
    
    def _build_prompt(self, activities_json) -> str:
        """Build prompt for LLM"""

        debut = f"""
        Tu es un coach sportif expert, enthousiaste et très loquace. Ta mission est de rédiger des félicitations personnalisées pour des employés.
        Chaque commentaire qui doit être un paragraphe riche et motivant est posté dans un canal Slack .

        Données à traiter (JSON) :
        {activities_json}
        """

        fin = """        
        RÈGLES DE GÉNÉRATION (STRICTES) :

        1. LONGUEUR : Chaque commentaire DOIT faire entre 200 et 250 caractères. Ne sois pas concis, brode sur la performance et l'état d'esprit. Fait des passages à la ligne avec \\n pour aérer le résultat et met les mots importants en gras
        2. STRUCTURE : 
        - Salutation personnalisée (Name).
        - Analyse de la performance (Distance, Durée, Sport).
        - Rebond narratif sur la "Situation" (si présente) ou extrapolation sur les bienfaits du sport cité.
        - Conclusion inspirante avec emojis.
        3. IDENTIFIANT : Reprends exactement l'ID fourni.
        4. FORMAT DE SORTIE : Uniquement un objet JSON avec une liste "results".

        EXEMPLE DE STYLE ATTENDU (220 caractères) :
        {
        "results": [
            { 
            "id": "ACT-2026-001", 
            "comment": "**Incroyable performance**, Juliette D. ! Nager 0.9 km en seulement 20 minutes demande une technique de respiration et une force mentale impressionnantes. 
            C'est en enchaînant ces longueurs que tu construis une **endurance d'acier**. Continue sur cette lancée, tu es une véritable source d'inspiration pour toute l'équipe ! 🏊‍♀️🔥" 
            }
        ]
        }
        """
        return debut + fin

    def _generate_answer(self, prompt: str, temperature: float = 0.7) -> str:
        """Generate answer using LLM"""
        try:
            return self.search_llm.generate(prompt, temperature=temperature)

        except Exception as e:
            logger.error(f"LLM generation error: {e}")
            return "Désolé, je n'ai pas pu générer une réponse."