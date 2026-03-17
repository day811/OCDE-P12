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

