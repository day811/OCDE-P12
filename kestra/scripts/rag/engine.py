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

        debut =  f"""
Tu es un coach sportif. Tu vas recevoir une liste d'activités sous forme de tableau JSON.

Ta mission : Générer un commentaire de félicitations pour chaque entrée.

Données à traiter :
{activities_json}
"""
        fin = """        

Règles de génération :

Lien Identifiant : Tu dois impérativement reprendre l' id fourni dans le contexte pour chaque réponse.

Contenu : Adresse-toi à Name, et utilise la performance. Si situation est remplie, rebondis sur son contenu. Sinon, encourage l'employé sur son sport.

Format du commentaire : Style amical, motivant, entre 200 et 250 caractères.

Format de sortie (Impératif) :
Tu dois répondre par un objet JSON unique contenant une liste nommée results. Chaque élément de la liste doit avoir exactement deux champs : id et comment.

Exemple de structure attendue :
{
  "results": [
    { "id": "ACT-2026-001", "comment": "Bravo Juliette M. ! Tu viens de nager 0.9 km en 20 min ! Quelle
énergie !🏅" },
    { "id": "ACT-2026-002", "comment": "Magnifique Laurence D. ! Une randonnée de 10 km terminée et
un nouveau spot à découvrir ! 🌄 😍('Randonnée de St Guilhem le
désert, je vous la conseille c'est top')" }
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