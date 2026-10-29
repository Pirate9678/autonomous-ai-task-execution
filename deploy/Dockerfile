# Custom n8n image with the project repo baked in.
#
# The Dockerfile is intentionally minimal: it layers this repository on top
# of the official n8n image so the EC2 host has everything (workflow export
# + Python pipeline) in one container image. Build and run via
# deploy/docker-compose.yml on the EC2 instance.
#
# Build locally (optional):  docker build -f deploy/Dockerfile -t ai-task-exec-n8n:latest .

FROM n8nio/n8n:latest

# The repo is copied into the container so the Execute Command node can run
# the pipeline directly on the host filesystem of the container.
COPY . /home/node/pipeline-host

# Python is available in the n8n image; no extra packages needed
# (the pipeline uses the standard library only).

USER node
WORKDIR /home/node

EXPOSE 5678
