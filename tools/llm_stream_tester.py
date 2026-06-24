from src.base.llm_stream_generator import BaseLLMStreamGenerator, LLMMessage
from src.impl.llm_stream_generators.ollama import OllamaLLMStreamGenerator
import sys
from dotenv import dotenv_values

def main():
    values = dotenv_values()
    model = values["OLLAMA_MODEL"]
    print(f"Using model {model}")
    print("="*20)

    generator: BaseLLMStreamGenerator
    generator = OllamaLLMStreamGenerator(model)
    response = generator.get_next_msg([
        LLMMessage(role="system", content=sys.argv[1])
    ])

    c: LLMMessage
    for c in response:
        print(c.content, end='', flush=True)

if __name__ == "__main__":
    main()