# Ollama Local Setup

## 1. Install Ollama
Download: https://ollama.ai/download
- Mac: `brew install ollama` or download .dmg
- Runs as local service on port 11434

## 2. Download Model (one-time)
```bash
ollama pull llama2
```
Or larger model:
```bash
ollama pull llama2:13b
ollama pull neural-chat
```

Model sizes:
- llama2 (7B): ~4GB, fast
- llama2:13b: ~8GB, better quality
- neural-chat: ~5GB, optimized for chat

## 3. Start Ollama
```bash
ollama serve
```
(Runs in background, accessible at http://localhost:11434)

## 4. Test it
```bash
curl http://localhost:11434/api/generate -d '{
  "model": "llama2",
  "prompt": "Why is the sky blue?",
  "stream": false
}'
```

## 5. Integration
- Intel will automatically detect Ollama running locally
- If available: use Ollama (free, fast)
- If unavailable: fall back to Groq free tier
- No API keys needed, no rate limits

## Cost
**$0.00** — Ollama is completely free. You just need the local machine running the service.
