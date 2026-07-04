import argparse
import asyncio
import redis
import ollama
from ollama_text_generator.punctuation_chunker import PunctuationChunker
from yaspin import yaspin
from yaspin.spinners import Spinners
import os, sys

PROMPTS_RELDIR="prompts"

def get_prompts_dir() -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), PROMPTS_RELDIR)

async def main():
    parser = argparse.ArgumentParser(prog="ollama_text_generator")
    parser.add_argument("-p", "--prompt-file", default="")
    parser.add_argument("-t", "--prompt-text", default="")
    parser.add_argument("-m", "--model", default="")
    parser.add_argument("-a", "--redis-address", default="localhost")
    parser.add_argument("-s", "--redis-port", default=6379, type=int)
    parser.add_argument("-o", "--output-redis-queue-name", default="generated_text")
    parser.add_argument("-q", "--max-queue-length", default=10, type=int)
    args = parser.parse_args()

    assert args.model != "", "--model required but not specified"
    assert args.prompt_file != "" or args.prompt_text != "", "one of --prompt-file or --prompt-text required but not specified"

    r = redis.Redis(host=args.redis_address, port=args.redis_port)
    r.delete(args.output_redis_queue_name)

    if args.prompt_text != "":
        initial_prompt = args.prompt_text
    else:
        with open(os.path.join(get_prompts_dir(), args.prompt_file+".txt")) as prompt_file:
            initial_prompt = prompt_file.read()
    
    print(f"Prompting with intial prompt:\n'{initial_prompt}'")

    chunker = PunctuationChunker()

    last_msg: str = None
    
    while True:
        try:
            if r.llen(args.output_redis_queue_name) >= args.max_queue_length:
                with yaspin(text=f"Waiting for redis to be below the limit of {args.max_queue_length} items..."):
                    while r.llen(args.output_redis_queue_name) >= args.max_queue_length:
                        await asyncio.sleep(1.0)

            chat_history = [
                ollama.Message(role="system", content=initial_prompt)
            ]
            if last_msg is not None:
                chat_history.append(ollama.Message(role="assistant", content=last_msg))
                chat_history.append(ollama.Message(role="system", content="Continue."))

            with yaspin(text="Waiting for ollama to spit out some text...", spinner=Spinners.sand):
                response = ollama.chat(args.model, chat_history)
                last_msg = response.message.content
                chunked = chunker.chunk_str(response.message.content)
            
            for part in chunked:
                print(f"Pushing chat chunk ({len(part)} chars): '{part}'")
                r.lpush(args.output_redis_queue_name, part)
        except KeyboardInterrupt:
            sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())