#!/bin/bash

SCRIPT_PATH="$(dirname $0)"
REQ_PATH="$SCRIPT_PATH/requirements_unified.txt"

echo "# ollama_text_generator"                           > $REQ_PATH
cat $SCRIPT_PATH/ollama_text_generator/requirements.txt >> $REQ_PATH
echo ""                                                 >> $REQ_PATH
echo "# qwen_tts_speaker"                               >> $REQ_PATH
cat $SCRIPT_PATH/qwen_tts_speaker/requirements.txt      >> $REQ_PATH
echo ""                                                 >> $REQ_PATH
echo "# speaker_audio_player"                           >> $REQ_PATH
cat $SCRIPT_PATH/speaker_audio_player/requirements.txt  >> $REQ_PATH
echo ""                                                 >> $REQ_PATH