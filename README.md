# ttstream
## Summary
`ttstream` is a free-time project that generates nonstop text from an LLM based on a prompt, feeds it through a text-to-speech system, and then pipes the resulting audio into Icecast to be streamed on a webpage. All components (for the core stack) are dockerized and built around a central Redis instance.

The text generation is based off of locally downloaded GGUF files, and the text-to-speech is implemented using Qwen3-TTS. All components are designed with single purposes and can be easily swapped out.

## Configuration and Running
The entire core stack can be deployed using the docker-compose.yml file. In addition, create a file named `.env` in the root directory with contents like the following:
```
ICECAST_PASSWORD=<some random password>  # Purely used to authenticate pushing to stream, not used to view stream
LOCAL_IP=<IP or hostname that icecast thinks it is running at. Not too important AFAIK>
OPENAI_MODEL_FILE=<name of model in the models folder>.gguf
PROMPT_FILE=<name of prompt in the openai_text_generator/prompts directory, without .txt extension>
QWEN_TTS_MODEL=Qwen/Qwen3-TTS-12Hz-0.6B-Base
TTS_VOICE=<name of voice sample in the qwen_tts_speaker/audio_samples directory, without extension. Both .txt and .wav should exist>
```

## Architecture
All data is pushed through a central Redis instance. The primary reason is that it gives a single easy place to work around for all configuration, regardless of implementation for individual modules.

The data flows in this order:
1. `openai_text_generator` makes requests to a locally hosted `llama.cpp` server, providing the initial prompt and then simply repeating "Continue." to get more text. Text is pushed to a redis queue `generated_text`.

    a. Alternatively, you can push text to the queue with `lpush` if you want to save on the GPU memory for that part

    b. Text is additionally passed through a simple chunker to make it into ~200 character sections, but keeping sentences intact. This helps the text-to-speech keep up in real time a little better.
    
2. `qwen_tts_speaker` continually polls the `generated_text` queue for the next chunk, and when it gets one it will feed it through the `faster_qwen3_tts` library and pipe the resulting audio bytes to the redis `generated_audio_bytes` queue.

    a. Alternatively, you can use the `audio_file_injector` package to push an audio file into the queue to bypass the text-to-speech mechanism

    b. Voices can either be done using Qwen3's voice design feature, or replicating audio samples in `qwen_tts_speaker/audio_samples`

3. `icecast_audio_pusher` continually polls the `generated_audio_bytes` queue and pushes the bytes it gets into `ffmpeg`, which in turn pushes it to Icecast using its built-in support

    b. Alternatively, you can use the `speaker_audio_player` tool to listen to the queue and play the audio through your speaker instead of through Icecast for more direct debugging

4. This is enough to listen to the stream using VLC, or you can serve `static_web_content` for a website with a simple embedded audio player in it