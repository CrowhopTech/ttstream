from abc import ABC, abstractmethod
from typing import List, Iterable
import ollama

class LLMMessage:
    def __init__(self, role: str, content: str):
        self.role = role
        self.content = content
    
    @staticmethod
    def from_ollama(msg: ollama.Message) -> 'LLMMessage':
        """
        Converts the given ollama.Message into an LLMMessage
        """
        return LLMMessage(role=msg.role, content=msg.content)

    def to_ollama(self) -> ollama.Message:
        """
        Converts this LLMMessage to an ollama.Message
        """
        return ollama.Message(role=self.role, content=self.content)

class BaseLLMStreamGenerator(ABC):
    @abstractmethod
    def get_next_msg(self, history: List[LLMMessage]) -> Iterable[LLMMessage]:
        pass