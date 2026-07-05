#!/bin/bash

SCRIPT_PATH="$(dirname $0)"
REQ_PATH="$SCRIPT_PATH/requirements_unified.txt"

echo "# Use this file to install requirements for all modules at once (useful for local development)" > $REQ_PATH
echo "# openai_text_generator"                          >> $REQ_PATH
cat $SCRIPT_PATH/openai_text_generator/requirements.txt >> $REQ_PATH
echo ""                                                 >> $REQ_PATH
echo "# qwen_tts_speaker"                               >> $REQ_PATH
cat $SCRIPT_PATH/qwen_tts_speaker/requirements.txt      >> $REQ_PATH
echo ""                                                 >> $REQ_PATH
echo "# speaker_audio_player"                           >> $REQ_PATH
cat $SCRIPT_PATH/speaker_audio_player/requirements.txt  >> $REQ_PATH
echo ""                                                 >> $REQ_PATH
echo "# file_audio_saver"                               >> $REQ_PATH
cat $SCRIPT_PATH/file_audio_saver/requirements.txt      >> $REQ_PATH
echo ""                                                 >> $REQ_PATH
echo "# audio_file_injector"                            >> $REQ_PATH
cat $SCRIPT_PATH/audio_file_injector/requirements.txt   >> $REQ_PATH
echo ""                                                 >> $REQ_PATH
echo "# icecast_audio_pusher"                           >> $REQ_PATH
cat $SCRIPT_PATH/icecast_audio_pusher/requirements.txt  >> $REQ_PATH
echo ""                                                 >> $REQ_PATH
