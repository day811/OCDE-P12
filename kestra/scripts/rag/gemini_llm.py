from typing import List
from src.llm.base import BaseLLM
from google import genai
import logging
from src.config import Config

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
        embed(text: str | list) -> list | list[list]:
            Generate embeddings for one or multiple texts using the Gemini embed model.
            Returns a single embedding vector for a string input or a list of embedding
            vectors for a list input.
        get_langchain(temperature: float = 0.7):
            Factory class method that returns a LangChain-compatible ChatGoogleGenerativeAI
            instance configured with the Gemini model and API key.
    """


    NAME = "Gemnii AI"
    PROVIDER = "gemini"
    CHAT_MODEL = Config.get_chat_model(PROVIDER)
    EMBED_MODEL = Config.get_embed_model(PROVIDER)
    API_KEY = Config.get_api_key(PROVIDER)

    def __init__(self, temperature: float = 0.7):

        super().__init__(temperature)

        self.client = genai.Client(api_key=self.API_KEY)# type: ignore
        logger.info(f"GeminiLLM initialized - Chat: {self.CHAT_MODEL}, Embed: {self.EMBED_MODEL}, Temp: {self.temperature}")
    
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
        return response.text
    
    def embed(self, text: str):
        """
        Generate embeddings for one or more texts using the Gemini embedding model.
        Args:
            text (str or list): A single text string or a list of text strings to embed.
        Returns:
            list or list[float]: If input is a list, returns a list of embeddings (each embedding 
                                is a list of floats). If input is a single string, returns a single 
                                embedding as a list of floats.
        Raises:
            Exception: If the API call to the embedding model fails.
        """
        

        if isinstance(text, list):
            # Multiple texts → return list of embeddings
            response = self.client.models.embed_content( #type:ignore
                model=self.EMBED_MODEL,
                contents=text
            )
            return [embed.values for embed in response.embeddings]
        else:
            # Single text → return single embedding
            response = self.client.models.embed_content( #type:ignore
                model=self.EMBED_MODEL,
                contents=[text]
            )
            return response.embeddings[0].values

    @classmethod
    def get_langchain(cls,temperature: float = 0.7):
        """
        Create and return a LangChain ChatGoogleGenerativeAI instance.
        This class method initializes a Google Generative AI chat model configured
        with the class's predefined model and API key settings.
        Args:
            temperature (float, optional): Controls the randomness of the model's responses.
                Range is typically 0.0 to 1.0, where 0.0 makes output deterministic and
                higher values increase creativity. Defaults to 0.7.
        Returns:
            ChatGoogleGenerativeAI: A configured LangChain chat model instance ready
                for use in language model operations.
        Raises:
            ImportError: If langchain_google_genai package is not installed.
        """

        from langchain_google_genai import ChatGoogleGenerativeAI

        return ChatGoogleGenerativeAI(
            model=cls.CHAT_MODEL, # type: ignore
            google_api_key=cls.API_KEY,
            temperature=temperature
        )
