import os

# ==================== LLM CONFIGURATION ====================    
class Config:
    LLM_MISTRAL = "mistral"
    LLM_OPENAI = "openai"
    LLM_GEMINI = "gemini"
    
    
    
    # API Keys
    API_KEYS = { 
        LLM_MISTRAL : os.getenv('MISTRAL_API_KEY','') ,
        LLM_GEMINI :  os.getenv('GEMINI_API_KEY','') }
    
    # Provider in preference order
    ALL_LLM = [LLM_GEMINI, LLM_MISTRAL]
    
    # Default models fallback (if not specified in .env)
    LLM_MODELS = {
        'mistral': 'mistral-small',
        'gemini' : 'models/gemini-2.5-flash'
        }
    
    # Models per provider
    LLM_CHAT_MODEL = os.getenv('LLM_CHAT_MODEL') or LLM_MODELS[ALL_LLM[0]]
    
    # Temperature for generation (0.0 = deterministic, 1.0 = random)
    LLM_TEMPERATURE = float(os.getenv('LLM_TEMPERATURE', '0.7'))

    @classmethod
    def get_api_key(cls, provider:str=""):
        def get_api_key(cls, provider: str = "") -> str:
            """
            Retrieve the API key for the specified provider.
            Args:
                provider (str, optional): The name of the API provider. If not provided,
                    defaults to the configured ALL_LLM[0]. Defaults to "".
            Returns:
                str: The API key associated with the specified provider.
            Raises:
                KeyError: If the provider is not found in the API_KEYS dictionary.
            """

        if not provider: provider= cls.ALL_LLM[0]
        return cls.API_KEYS[provider] 
        # Get models from config

    @classmethod
    def get_chat_model(cls, provider:str=""):
        """
        Get the chat model for the specified LLM provider.
        Args:
            provider (str, optional): The name of the LLM provider. If not provided,
                defaults to the class's ALL_LLM[0] attribute.
        Returns:
            The chat model instance/configuration for the specified provider.
        Raises:
            KeyError: If the provider is not found in cls.LLM_MODELS.
        """

        if not provider: provider= cls.ALL_LLM[0]
        return cls.LLM_MODELS[provider]

