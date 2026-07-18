import argparse
import asyncio
from typing import List
import redis
from punctuation_chunker import PunctuationChunker
import openai
import json
from environs import env
from yaspin import yaspin
from yaspin.spinners import Spinners
from datetime import datetime
import os, sys

PROMPTS_RELDIR="prompts"

class OpenAITextGeneratorStatus:
    def __init__(self, status: str, as_of: datetime=datetime.now()):
        self.as_of = as_of.timestamp()
        self.status = status
    
    def json(self) -> str:
        return json.dumps(self.__dict__)

    def __str__(self) -> str:
        return self.json()

def get_prompts_dir() -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), PROMPTS_RELDIR)

async def main():
    parser = argparse.ArgumentParser(prog="openai_text_generator")
    parser.add_argument("-t", "--prompt-text", default="")
    prompt_file = env.str("PROMPT_FILE", default="")
    model = env.str("LLAMA_MODEL")
    llama_server = env.str("LLAMA_SERVER", "localhost")
    llama_port = env.int("LLAMA_PORT", 8080)
    redis_address = env.str("REDIS_ADDRESS", default="localhost")
    redis_port = env.int("REDIS_PORT", default=6379)
    output_redis_queue_name = env.str("REDIS_TEXT_OUTPUT_QUEUE_NAME", default="generated_text")
    redis_status_output_name = env.str("REDIS_STATUS_OUTPUT_NAME", default="openai_text_generator_status")
    redis_trigger_key_name = env.str("REDIS_TRIGGER_KEY_NAME", default="webpage.keepalive")
    parser.add_argument("-q", "--max-queue-length", default=10, type=int)
    args = parser.parse_args()

    assert model != "", "--model required but not specified"
    assert prompt_file != "" or args.prompt_text != "", "one of --prompt-file or --prompt-text required but not specified"

    openai_client = openai.OpenAI(base_url=f"http://{llama_server}:{llama_port}/v1", api_key="")

    r = redis.Redis(host=redis_address, port=redis_port)

    def set_status(new_status: str) -> None:
        print(f"Setting status to '{new_status}'")
        r.set(redis_status_output_name, OpenAITextGeneratorStatus(status=new_status).json())
    
    set_status("Starting up...")
    r.delete(output_redis_queue_name)

    if args.prompt_text != "":
        initial_prompt = args.prompt_text
    else:
        with open(os.path.join(get_prompts_dir(), prompt_file+".txt")) as pf:
            initial_prompt = pf.read()
    
    print(f"Prompting with intial prompt:\n'{initial_prompt}'")

    chunker = PunctuationChunker()

    last_msg: str = None
    
    while True:
        try:
            redis_trigger_val = r.get(redis_trigger_key_name)
            if redis_trigger_val is None or redis_trigger_val.decode("UTF-8") == "false":
                # Stop generation, clear the queues
                set_status(f"Waiting for trigger key to be set to true...")
                print("Clearing generated text queue")
                r.delete(output_redis_queue_name)
                while True:
                    redis_trigger_val = r.get(redis_trigger_key_name)
                    if redis_trigger_val is not None and redis_trigger_val.decode("UTF-8") != "false":
                        break
                    await asyncio.sleep(1.0)
            
            if r.llen(output_redis_queue_name) >= args.max_queue_length:
                set_status(f"Waiting for redis to be below the limit of {args.max_queue_length} items...")
                with yaspin(text=f"Waiting for redis to be below the limit of {args.max_queue_length} items..."):
                    while r.llen(output_redis_queue_name) >= args.max_queue_length:
                        await asyncio.sleep(1.0)

            chat_history: List[openai.ChatCompletionMessageParam] = [
                {"role": "user", "content": initial_prompt}
            ]
            if last_msg is not None:
                chat_history.append({"role": "assistant", "content": last_msg})
                chat_history.append({"role": "user", "content": "Continue."})

            set_status("Waiting for the model to spit out some text...")
            with yaspin(text="Waiting for the model to spit out some text...", spinner=Spinners.sand):
                response = openai_client.chat.completions.create(messages=chat_history, model=model)
                last_msg = response.choices[0].message.content
                chunked = chunker.chunk_str(response.choices[0].message.content)
            
            for part in chunked:
                print(f"Pushing chat chunk ({len(part)} chars): '{part}'")
                r.lpush(output_redis_queue_name, part)
        except KeyboardInterrupt:
            set_status("Exiting")
            sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())