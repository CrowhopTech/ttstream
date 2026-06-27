import argparse
import asyncio
import redis
import ollama
from ollama_text_generator.punctuation_chunker import PunctuationChunker

async def main():
    parser = argparse.ArgumentParser(prog="ollama_text_generator")
    parser.add_argument("-p", "--prompt", default="")
    parser.add_argument("-m", "--model", default="")
    parser.add_argument("-a", "--redis-address", default="localhost")
    parser.add_argument("-s", "--redis-port", default=6379, type=int)
    parser.add_argument("-o", "--output-redis-queue-name", default="generated_text")
    args = parser.parse_args()

    assert args.model != "", "--model required but not specified"
    assert args.prompt != "", "--prompt required but not specified"

    r = redis.Redis(host=args.redis_address, port=args.redis_port)
    r.delete(args.output_redis_queue_name)

    chunker = PunctuationChunker()

    response = ollama.chat(args.model, [ollama.Message(role="system", content=args.prompt)])
    chunked = chunker.chunk_str(response.message.content)
    for part in chunked:
        print(f"Pushing chat chunk: '{part}'")
        r.lpush(args.output_redis_queue_name, part)


if __name__ == "__main__":
    asyncio.run(main())