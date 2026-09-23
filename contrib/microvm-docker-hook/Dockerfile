FROM python:3.13-alpine

WORKDIR /app
COPY server.py .

ENV HOOK_PORT=9000
EXPOSE 9000
CMD ["python", "server.py"]
