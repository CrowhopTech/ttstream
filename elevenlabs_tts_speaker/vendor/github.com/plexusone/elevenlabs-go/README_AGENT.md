# Using ElevenLabs with AI Agent Frameworks

This document covers integrating ElevenLabs voice capabilities with AI agent platforms.

## Architecture Pattern

The common architecture for voice AI agents combines:

```
STT (Speech-to-Text) → Agent Brain → TTS (Text-to-Speech)
     (Whisper)         (LangGraph,    (ElevenLabs)
                        CrewAI, etc.)
```

## Framework Compatibility

| Framework | Voice Agent Support | Notes |
|-----------|---------------------|-------|
| LangGraph/LangChain | Strong | Native ElevenLabs integration, extensive tutorials |
| Google ADK | Strong | Bidirectional audio/video streaming, announced April 2025 |
| [Eino](https://github.com/cloudwego/eino) | Orchestration | Go-native framework by ByteDance/CloudWeGo, pair with voice stack |
| CrewAI | Orchestration | Pair with voice stack (STT + TTS) |
| AutoGen | Orchestration | Pair with voice stack (STT + TTS) |

## ElevenLabs Integration Approaches

### 1. ElevenLabs Native Agent Platform

ElevenLabs provides a built-in [Agents Platform](https://elevenlabs.io/docs/agents-platform/overview) that can integrate with external agents via WebSocket. This allows plugging in LangGraph, CrewAI, or other frameworks as the "brain" while ElevenLabs handles voice I/O.

- [Integrating External Agents with ElevenLabs](https://elevenlabs.io/blog/integrating-complex-external-agents)
- [ElevenLabs Agents Documentation](https://elevenlabs.io/docs/agents-platform/overview)

### 2. LangChain/LangGraph Integration

The most documented combination for programmatic control:

- [LangChain ElevenLabs Integration Docs](https://docs.langchain.com/oss/python/integrations/providers/elevenlabs)
- [Voice Agents: Building Real-Time Conversational AI with Whisper, Groq, LangGraph and ElevenLabs](https://medium.com/@pankaj_pandey/voice-agents-building-real-time-conversational-ai-with-whisper-groq-langgraph-and-elevenlabs-efdc8b4ffc84)
- [From No-Code to Full Control: Rebuilding ElevenLabs' AI Agent with LangGraph](https://ai.plainenglish.io/from-no-code-to-full-control-how-i-rebuilt-elevenlabs-ai-agent-with-langgraph-and-whisper-from-fd8fe1a112ee)

### 3. Custom Integration via API/SDK

Use this Go SDK (`go-elevenlabs`) or the official Python/JS SDKs to build custom integrations with any agent framework.

## When to Use Each Approach

| Use Case | Recommended Approach |
|----------|---------------------|
| Simple voice agents, rapid prototyping | ElevenLabs Native Platform (no-code) |
| Multi-turn dialogues, complex memory | LangGraph + ElevenLabs |
| Multi-agent orchestration | CrewAI/AutoGen + voice stack |
| Go-native, high-performance agents | Eino + ElevenLabs via go-elevenlabs |
| Google Cloud integration | Google ADK with ElevenLabs TTS |
| Full control, custom logic | Direct SDK integration |

## Enterprise Considerations

For production voice agents (e.g., call center automation):

1. **Latency**: Real-time STT and TTS pipelines need sub-second latency
2. **Telephony**: Integration with phone systems (Twilio, etc.)
3. **Compliance**: Recording consent, data retention policies
4. **Fallback**: Human handoff mechanisms

No agent framework is "plug and play" for enterprise voice—expect to layer STT, TTS, telephony, and compliance components.

## Related Integrations

- [ElevenLabs + Mem0](https://docs.mem0.ai/integrations/elevenlabs) - Persistent memory across voice conversations
- [ElevenLabs + Voiceflow](https://www.voiceflow.com/blog/build-a-custom-voice-ai-agent-with-elevenlabs-api) - Visual conversation design

## Resources

- [ElevenLabs Developer Documentation](https://elevenlabs.io/docs/overview/intro)
- [ElevenLabs API](https://elevenlabs.io/developers)
- [Top AI Agent Frameworks Comparison (2025)](https://softcery.com/lab/top-14-ai-agent-frameworks-of-2025-a-founders-guide-to-building-smarter-systems)
- [AI Voice Agent Frameworks for Enterprise](https://smallest.ai/blog/ai-voice-agent-frameworks-enterprise)
