from typing import List
from rag.base_llm import BaseLLM
from google import genai
import logging
from rag.config import Config

logger = logging.getLogger(__name__)

class GeminiLLM(BaseLLM):
    """
    Gemini LLM integration for the OCDE project.
    This class provides an interface to Google's Gemini AI models for both text generation
    and embeddings. It extends the BaseLLM base class and handles initialization of the Gemini
    client, text generation with configurable parameters, and embedding generation for both
    single and multiple texts.
    Attributes:
        NAME (str): Human-readable name of the LLM provider ("Gemnii AI").
        PROVIDER (str): Provider identifier ("gemini").
        CHAT_MODEL (str): The chat model identifier retrieved from configuration.
        EMBED_MODEL (str): The embedding model identifier retrieved from configuration.
        API_KEY (str): Google API key retrieved from configuration.
    Methods:
        __init__(temperature: float = 0.7):
            Initialize the GeminiLLM client with the specified temperature parameter.
        generate(prompt: str, temperature: float = 0.7) -> str:
            Generate text content based on a given prompt using the Gemini chat model.
    """


    NAME = "Gemnii AI"
    PROVIDER = "gemini"
    CHAT_MODEL = Config.get_chat_model(PROVIDER)
    
    API_KEY = Config.get_api_key(PROVIDER)

    def __init__(self, temperature: float = 0.7):

        super().__init__(temperature)

        self.client = genai.Client(api_key=self.API_KEY)# type: ignore
        logger.info(f"GeminiLLM initialized - Chat: {self.CHAT_MODEL}, Temp: {self.temperature}")
    
    def generate(self, prompt: str, temperature: float = 0.7) -> str:
        """
        Generate content using the Gemini API.
        Args:
            prompt (str): The input prompt for content generation.
            temperature (float, optional): Controls randomness of the output. 
                Defaults to 0.7. If not provided, uses the instance's temperature setting.
        Returns:
            str: The generated text response from the Gemini model.
        """
        
        temperature  = temperature if temperature is not None else self.temperature
        response = self.client.models.generate_content(
            model= self.CHAT_MODEL,
            contents= prompt,
            config={
                "temperature": self.temperature,
                "max_output_tokens": 512,
            },
            )
        return response.text if response.text else ""
    
