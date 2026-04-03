#!/bin/bash
# MiniStack Bedrock — Ollama entrypoint
# 1. Start the Ollama server in background
# 2. Wait for it to be ready
# 3. Read bedrock_models.yaml and pull any missing models
# 4. Keep the server running in foreground

set -e

# Start Ollama server in background
/bin/ollama serve &
SERVER_PID=$!

# Wait for server to be ready
echo "[ministack-ollama] Waiting for Ollama server..."
for i in $(seq 1 30); do
    if ollama list >/dev/null 2>&1; then
        echo "[ministack-ollama] Ollama server ready."
        break
    fi
    sleep 1
done

# Read models from bedrock_models.yaml and pull missing ones
CONFIG_FILE="/config/bedrock_models.yaml"
if [ -f "$CONFIG_FILE" ]; then
    echo "[ministack-ollama] Reading model config from $CONFIG_FILE"

    # Extract unique Ollama model names from YAML values
    # Matches values after ": " — filters out AWS model IDs and config keys
    MODELS=$(grep -E '^\s+"[^"]+"\s*:\s*"[^"]+"' "$CONFIG_FILE" \
        | sed 's/.*: *"\([^"]*\)".*/\1/' \
        | sort -u)
    # Also grab embedding_model and default_model values
    for KEY in embedding_model default_model; do
        VAL=$(grep "^${KEY}:" "$CONFIG_FILE" | sed 's/.*: *"\([^"]*\)".*/\1/' | head -1)
        [ -n "$VAL" ] && MODELS=$(printf '%s\n%s' "$MODELS" "$VAL" | sort -u)
    done

    # Get currently installed models
    INSTALLED=$(ollama list 2>/dev/null | tail -n +2 | awk '{print $1}' | sed 's/:latest$//')

    for MODEL in $MODELS; do
        MODEL_BASE=$(echo "$MODEL" | sed 's/:latest$//')
        if echo "$INSTALLED" | grep -qxF "$MODEL_BASE" || echo "$INSTALLED" | grep -qxF "$MODEL"; then
            echo "[ministack-ollama] ✓ $MODEL already available"
        else
            echo "[ministack-ollama] ↓ Pulling $MODEL..."
            if ollama pull "$MODEL"; then
                echo "[ministack-ollama] ✓ $MODEL pulled successfully"
            else
                echo "[ministack-ollama] ⚠ Failed to pull $MODEL (continuing)"
            fi
        fi
    done

    echo "[ministack-ollama] All models ready."
else
    echo "[ministack-ollama] No config at $CONFIG_FILE — pulling defaults"
    ollama pull qwen2.5:3b || true
    ollama pull nomic-embed-text || true
fi

# Keep server running in foreground
wait $SERVER_PID
