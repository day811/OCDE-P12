from typing import List, Union
from src.llm.base import BaseLLM
from mistralai import Mistral
import logging
from src.config import Config

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
    EMBED_MODEL = Config.get_embed_model(PROVIDER)
    API_KEY = Config.get_api_key(PROVIDER)

    def __init__(self, temperature: float = 0.7):

        super().__init__(temperature)


        self.client = Mistral(api_key=self.API_KEY)
        logger.info(f"MistralLLM initialized - Chat: {self.CHAT_MODEL}, Embed: {self.EMBED_MODEL}, Temp: {self.temperature}")
    
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
    
    def embed(self, text) :
        def embed(self, text) -> list | list[float]:
            """
            Generate embeddings for one or more texts using the Mistral embedding model.
            Args:
                text (str | list[str]): A single text string or a list of text strings to embed.
            Returns:
                list[float] | list[list[float]]: If input is a string, returns a single embedding 
                    as a list of floats. If input is a list of strings, returns a list of embeddings,
                    where each embedding is a list of floats.
            Example:
                >>> single_embedding = embed("Hello world")
                >>> multiple_embeddings = embed(["Hello", "World"])
            """
        
        if isinstance(text, list):
            # Multiple texts → return list of embeddings
            response = self.client.embeddings.create(
                model=self.EMBED_MODEL,
                inputs=text
            )
            return [embed.embedding for embed in response.data]
        else:
            # Single text → return single embedding
            response = self.client.embeddings.create(
                model=self.EMBED_MODEL,
                inputs=[text]
            )
            return response.data[0].embedding
    
    @classmethod
    def get_langchain(cls,temperature: float = 0.7):
        """
        Create and return a LangChain ChatMistralAI instance.
        This class method initializes a Chat  Mistarl AI chat model configured
        with the class's predefined model and API key settings.
        Args:
            temperature (float, optional): Controls the randomness of the model's responses.
                Range is typically 0.0 to 1.0, where 0.0 makes output deterministic and
                higher values increase creativity. Defaults to 0.7.
        Returns:
            ChatMistralAI: A configured LangChain chat model instance ready
                for use in language model operations.
        Raises:
            ImportError: If langchain_mistralai package is not installed.
        """


        from langchain_mistralai import ChatMistralAI

        return ChatMistralAI(
            model=cls.CHAT_MODEL, # type: ignore
            api_key=cls.API_KEY,
            temperature=temperature
        )
