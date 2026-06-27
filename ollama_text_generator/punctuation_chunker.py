from typing import Iterable

class PunctuationChunker:
    def chunk_str(self, stream: str) -> Iterable[str]:
        buffer: str = ""
        # Build up a buffer of bytes. Once we detect a period, take the chunk and send it, and clear the buffer.
        # Handle mid-message periods well.
        for char in stream.strip():
            buffer += char
            if self.is_delimiter(char):
                cleaned = self.clean_chunk(buffer)
                if cleaned == "":
                    continue
                yield cleaned
                buffer = ""
                continue
        if len(buffer) > 0:
            # At the end send all of our remaining buffer
            cleaned = self.clean_chunk(buffer)
            if cleaned != "":
                yield cleaned
    
    def is_delimiter(self, char: str) -> bool:
        return char == '.' or char == '?' or char == '!'

    def clean_chunk(self, chunk: str) -> str:
        return chunk.strip()