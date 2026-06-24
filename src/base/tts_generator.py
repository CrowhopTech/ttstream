from abc import ABC, abstractmethod

class TTSGenerator(ABC):
    @abstractmethod
    def render_text_to_audio(text: str) -> bytes:
        pass