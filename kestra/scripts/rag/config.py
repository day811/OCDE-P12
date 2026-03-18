import os

# ==================== LLM CONFIGURATION ====================    
class Config:
    LLM_MISTRAL = "mistral"
    LLM_OPENAI = "openai"
    LLM_GEMINI = "gemini"
    
    
    # Provider: 'mistral', 'openai', 'gemini'
    LLM_PROVIDER = os.getenv('LLM_PROVIDER', 'mistral')
    
    # API Keys
    API_KEYS = { 
        LLM_MISTRAL : os.getenv('MISTRAL_API_KEY','') ,
        LLM_GEMINI :  os.getenv('GEMINI_API_KEY','') }
    
    ALL_LLM = [LLM_MISTRAL, LLM_GEMINI, LLM_OPENAI]
    
    # Default models fallback (if not specified in .env)
    LLM_MODELS = {
        'mistral': {
            'chat': os.getenv('MISTRAL_CHAT_MODEL', 'mistral-small'),
        },
        'gemini': {
            'chat': os.getenv('GEMINI_CHAT_MODEL', 'gemini-2.5-flash'),
        }
    }
    
    # Models per provider
    LLM_CHAT_MODEL = os.getenv('LLM_CHAT_MODEL') or LLM_MODELS[LLM_PROVIDER]['chat']
    
    # Temperature for generation (0.0 = deterministic, 1.0 = random)
    LLM_TEMPERATURE = float(os.getenv('LLM_TEMPERATURE', '0.7'))

    @classmethod
    def get_api_key(cls, provider:str=""):
        def get_api_key(cls, provider: str = "") -> str:
            """
            Retrieve the API key for the specified provider.
            Args:
                provider (str, optional): The name of the API provider. If not provided,
                    defaults to the configured LLM_PROVIDER. Defaults to "".
            Returns:
                str: The API key associated with the specified provider.
            Raises:
                KeyError: If the provider is not found in the API_KEYS dictionary.
            """

        if not provider: provider= cls.LLM_PROVIDER
        return cls.API_KEYS[provider] 
        # Get models from config

    @classmethod
    def get_chat_model(cls, provider:str=""):
        """
        Get the chat model for the specified LLM provider.
        Args:
            provider (str, optional): The name of the LLM provider. If not provided,
                defaults to the class's LLM_PROVIDER attribute.
        Returns:
            The chat model instance/configuration for the specified provider.
        Raises:
            KeyError: If the provider is not found in cls.LLM_MODELS.
        """

        if not provider: provider= cls.LLM_PROVIDER
        return cls.LLM_MODELS[provider]['chat']

