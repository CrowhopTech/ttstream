from typing import Iterable

class PunctuationChunker:
    MIN_CHARS_PER_CHUNK = 100

    def chunk_str(self, stream: str) -> Iterable[str]:
        buffer: str = ""
        # Build up a buffer of bytes. Once we detect a period, take the chunk and send it, and clear the buffer.
        # Handle mid-message periods well.
        for char in stream.strip():
            buffer += char
            if self.is_delimiter(char) and len(buffer) >= self.MIN_CHARS_PER_CHUNK:
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
    
    def preclean_str(self, pre: str) -> str:
        unicode_ellipsis = pre.replace("...", "\u2026")

        return unicode_ellipsis

    def is_delimiter(self, char: str) -> bool:
        # \u2026 = ellipsis
        return char in ['.', '?', '!', '\u2026']

    def clean_chunk(self, chunk: str) -> str:
        return chunk.strip()