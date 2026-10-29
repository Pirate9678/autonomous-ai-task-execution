# Deploying on AWS EC2 (Ubuntu, t2.micro / t3.micro free tier)

This guide provisions an EC2 host and runs the n8n + pipeline stack from
`deploy/docker-compose.yml`. Steps marked **(manual)** are done in the AWS
console or in the n8n UI — everything else is commands you can copy-paste.

> Time estimate: 20–30 minutes if you follow the steps in order.

---

## 1. Launch the EC2 instance (manual)

1. In the AWS Console, go to **EC2 → Launch instances**.
2. Name: `ai-task-execution`.
3. AMI: **Ubuntu Server 22.04 LTS** (free-tier eligible).
4. Instance type: **t3.micro** (or t2.micro).
5. Key pair: create/download one (e.g. `ai-task-exec.pem`) — you need this for SSH.
6. Network settings: allow SSH (22), HTTP (80), HTTPS (443), and add a
   **Custom TCP rule: port 5678 from your IP** (the n8n UI port).
   > ⚠️ For a production deployment, restrict 5678 to your IP and put the
   > host behind HTTPS; the default setup below is for a demo/dev host.
7. Storage: default 8 GiB gp3 is fine.
8. Launch, wait for status checks to pass, note the **public IPv4 address**.

## 2. Install Docker on the instance

SSH in and run:

```bash
# (manual) connect to your instance, replacing the IP and key path
ssh -i ai-task-exec.pem ubuntu@<EC2_PUBLIC_IP>

# --- everything below runs on the instance ---
sudo apt update && sudo apt upgrade -y
sudo apt install -y ca-certificates curl git
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
  https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER
newgrp docker   # apply the group without re-logging
docker --version && docker compose version
```

## 3. Clone this repo on the instance

```bash
# (manual) if the repo is private, use a deploy key or HTTPS token
git clone https://github.com/<YOUR_USERNAME>/autonomous-ai-task-execution.git
cd autonomous-ai-task-execution
```

## 4. Configure secrets (manual)

Create a `.env` file **next to** `deploy/docker-compose.yml` (i.e. in the
repo root — it is git-ignored):

```bash
cat > .env <<'EOF'
N8N_BASIC_AUTH_PASSWORD=<choose-a-strong-password>
OPENAI_API_KEY=<your-openai-api-key>   # used by the AI Classification node
EOF
```

## 5. Build and start the stack

```bash
docker compose -f deploy/docker-compose.yml up -d --build
docker compose -f deploy/docker-compose.yml ps
docker compose -f deploy/docker-compose.yml logs -f n8n   # Ctrl+C to exit logs
```

When the log shows `Editor is now accessible via: http://0.0.0.0:5678`,
open **http://\<EC2_PUBLIC_IP\>:5678** in your browser and sign in with
`admin` / your password from `.env`.

## 6. Import the workflow (manual)

1. In n8n: **Workflows → ⋯ → Import from File**.
2. Select `n8n/workflow.json` from this repo.
3. Set up credentials (all manual):
   - **OpenAI account** — paste your API key (or point the node at your
     preferred LLM provider).
   - **Google Sheets account** — OAuth2, and replace `YOUR_GOOGLE_SHEET_ID`
     with your reporting spreadsheet's ID.
   - **Tickets API** — the HTTP Request node expects a bearer token for
     `https://api.example.com/v1/tickets`; point it at your real ticketing
     API (Zendesk / Freshdesk / internal) and adjust the field mapping in
     the **Normalise Fields** node.
4. Review the **AI Classification** node — the custom system prompt there
   mirrors `pipeline/prompts.yaml` (classification template).
5. Activate the workflow (toggle in the top-right). The **Schedule Trigger**
   runs it hourly.

## 7. Verify end-to-end (manual)

1. Trigger the workflow manually once (**Execute Workflow** button).
2. Check the execution succeeded and the **Log to Spreadsheet** node
   appended rows to your sheet.
3. Inspect the container's CSV artefacts:
   ```bash
   docker exec n8n-ai-task-exec ls /data
   ```

## 8. Stopping / cleanup (manual)

```bash
docker compose -f deploy/docker-compose.yml down      # stop, keep data
docker compose -f deploy/docker-compose.yml down -v   # stop AND delete volumes
```

Terminate the EC2 instance from the console when you're done to avoid charges.

---

### Security notes (read before exposing this publicly)

- Change the default n8n credentials immediately; the compose file forces
  basic auth for the editor.
- Never commit `.env` — it is listed in `.gitignore`.
- The pipeline code itself uses only the Python standard library, so the
  attack surface on the container is minimal; keep the `n8nio/n8n` base
  image updated (`docker compose pull` periodically).
