# The Case File — AI Investigator Command Center
### WeMakeDevs × Cognee Hackathon Submission

> A stateful, multi-session AI detective powered by **Cognee v1.0** hybrid graph-vector memory and **LangGraph** multi-agent orchestration. Runs free on **Google Gemini** (no billing needed).

---

## What It Does

Standard AI agents suffer from **digital amnesia** — every conversation starts from zero. The Case File solves this by framing the problem as a detective interrogation:

- Every subject statement is permanently logged via **`cognee.remember()`**
- The Analyst node cross-references all prior sessions via **`cognee.recall()`**
- Contradictions are detected, logged back, and the graph is enriched via **`cognee.improve()`**
- Cleared suspects are pruned from the graph via **`cognee.forget()`**
- The Chief compiles a live case report from the evolving knowledge graph

---

## Architecture

```
User Input (chat)
      │
      ▼
┌─────────────────┐   cognee.remember()
│ Detective Agent  │──────────────────────► Cognee Knowledge Graph
│   (node_01)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐   cognee.recall()
│  Analyst Agent  │◄──────────────────────  Cognee Knowledge Graph
│   (node_02)     │──► Contradiction Alert + Δ credibility
│                 │   cognee.improve()
└────────┬────────┘──────────────────────► Cognee Knowledge Graph
         │
         ▼
┌─────────────────┐   cognee.recall()
│ Chief of Police │◄──────────────────────  Cognee Knowledge Graph
│   (node_03)     │──► Live Case Report + Graph Citations
└─────────────────┘
```

### Cognee v1.0 API surface used

| Operation | Cognee call | Purpose |
|-----------|-------------|---------|
| **remember()** | `cognee.remember(fact, dataset_name, run_in_background=True)` | Log every statement permanently |
| **recall()** | `cognee.recall(query_text, datasets=[case_id])` | Cross-reference history for contradictions |
| **improve()** | `cognee.improve(dataset=case_id, run_in_background=True)` | Enrich graph after contradiction is found |
| **forget()** | `cognee.forget(dataset=name)` | Prune cleared suspects from active graph |

---

## Project Structure

```
agentic/
├── index.html          # 3-column dashboard UI
├── styles.css          # Light-mode premium stylesheet
├── app.js              # Live SSE wiring + 5s poller + graph canvas
├── README.md
└── backend/
    ├── investigator.py  # LangGraph pipeline (v1.0 Cognee API + Gemini)
    ├── server.py        # FastAPI REST + SSE server
    ├── requirements.txt
    ├── .env             # Your keys go here (not committed)
    └── .env.example     # Template
```

---

## Quick Start

### Step 1 — Get a free Google AI Studio key
Go to **https://aistudio.google.com/app/apikey** → Create API key → Copy it.

### Step 2 — Configure `.env`
```
cd backend
cp .env.example .env
```
Open `.env` and replace `your_google_ai_studio_api_key_here` with your key in all three places.

### Step 3 — Install dependencies
```bash
cd backend
pip install -r requirements.txt
```

### Step 4 — Run the server
```bash
python server.py
```
Server starts at `http://127.0.0.1:8000`

### Step 5 — Open the dashboard
Open `index.html` directly in your browser. No build step needed.

---

## Frontend-only mode (no server)

Open `index.html` directly in a browser. The app detects that the backend is offline and runs fully in **local simulation mode** — all three agent flows work with hardcoded patterns.

---

## Hackathon Criteria Alignment

| Criterion | How this project addresses it |
|-----------|-------------------------------|
| **Uses Cognee** | All four v1.0 operations: `remember()`, `recall()`, `improve()`, `forget()` |
| **Potential Impact** | Solves digital amnesia for fraud detection, compliance, journalism fact-checking |
| **Technical Excellence** | LangGraph multi-agent SSE streaming + Cognee hybrid graph-vector memory |
| **UI/UX** | Premium 3-column light-mode dashboard — not a basic chatbot |
| **Innovation** | "Detective that never forgets a lie" — stateful multi-session interrogation |
| **Working Demo** | Runs free on Gemini Flash; frontend works standalone without a server |
