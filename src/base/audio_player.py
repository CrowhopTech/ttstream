from abc import ABC, abstractmethod

class AudioPlayer(ABC):
    @abstractmethod
    def play_audio(data: bytes) -> None:
        pass