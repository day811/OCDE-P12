from abc import ABC, abstractmethod

class BaseLLM(ABC):
    """Abstract base class for LLM providers"""
    
    def __init__(self, temperature: float = 0.7):
        self.temperature = temperature

    @abstractmethod
    def generate(self, prompt: str, temperature: float = 0.7) -> str:
        """Generate text from prompt"""
        pass
    
