from typing import List, Union
from rag.base_llm import BaseLLM
from mistralai.client import Mistral
import logging
from rag.config import Config

logger = logging.getLogger(__name__)

class MistralLLM(BaseLLM):
    """
    MistralLLM class for interacting with Mistral AI models.
    This class extends BaseLLM to provide integration with Mistral AI's chat and embedding APIs.
    It handles text generation and embedding creation using Mistral's models.
    Attributes:
        NAME (str): Human-readable name of the LLM provider ("Mistral AI")
        PROVIDER (str): Provider identifier ("mistral")
        CHAT_MODEL (str): Chat model identifier from configuration
        EMBED_MODEL (str): Embedding model identifier from configuration
        API_KEY (str): API key for Mistral authentication from configuration
        client (Mistral): Mistral API client instance
        temperature (float): Default temperature for generation (inherited from BaseLLM)
    Methods:
        __init__(temperature: float = 0.7) -> None:
            Initialize the MistralLLM instance with specified temperature.
        generate(prompt: str, temperature: float = 0.7) -> str:
            Generate text using Mistral's chat API.
        embed(text: str | list) -> list | list[float]:
            Generate embeddings for single or multiple texts.
        get_langchain(temperature: float = 0.7) -> ChatMistralAI:
            Get a LangChain-compatible ChatMistralAI instance.
    """


    NAME = "Mistral AI"
    PROVIDER = "mistral"
    CHAT_MODEL = Config.get_chat_model(PROVIDER)
    API_KEY = Config.get_api_key(PROVIDER)

    def __init__(self, temperature: float = 0.7):

        super().__init__(temperature)


        self.client = Mistral(api_key=self.API_KEY)
        logger.info(f"MistralLLM initialized - Chat: {self.CHAT_MODEL}, Temp: {self.temperature}")
    
    def generate(self, prompt: str, temperature: float = 0.7) -> str:
        """
        Generate a response from the Mistral LLM model based on the provided prompt.
        Args:
            prompt (str): The input prompt to send to the model.
            temperature (float, optional): Controls the randomness of the response.
                Values closer to 0 make output more deterministic, while higher values
                increase creativity. Defaults to 0.7. If not provided, uses the instance's
                default temperature setting.
        Returns:
            str: The generated response content from the model. Returns an empty string
                if no content is generated.
        """
        
        temp = temperature if temperature is not None else self.temperature
        response = self.client.chat.complete(
            model=self.CHAT_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=temp
        )
        content = response.choices[0].message.content
        return str(content) if content else "" 
    
