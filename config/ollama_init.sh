#!/bin/bash
# MiniStack Bedrock — Pull Ollama models
# Run this after starting the ollama container:
#   docker exec ministack-ollama /bin/bash /app/config/ollama_init.sh

set -e

echo "Pulling models for MiniStack Bedrock..."

# Embedding model for Knowledge Base (~274 MB)
ollama pull nomic-embed-text

# Default model — Sonnet tier (~1.5 GB)
ollama pull qwen3.5:2b

# Claude-tier models (Opus / Haiku)
ollama pull qwen3.5:4b     # Opus tier   (~2.6 GB)
ollama pull gemma3:1b      # Haiku tier  (~815 MB)

echo "All models pulled successfully."
