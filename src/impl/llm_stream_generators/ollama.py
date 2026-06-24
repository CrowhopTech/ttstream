from src.base.llm_stream_generator import BaseLLMStreamGenerator, LLMMessage
from typing import override, List, Iterable
from ollama import chat

class OllamaLLMStreamGenerator(BaseLLMStreamGenerator):
    def __init__(self, model: str):
        self.model = model

    @override
    def get_next_msg(self, history: List[LLMMessage]) -> Iterable[LLMMessage]:
        stream = chat(self.model, [m.to_ollama() for m in history], stream=True)
        for m in stream:
            yield LLMMessage.from_ollama(m.message)