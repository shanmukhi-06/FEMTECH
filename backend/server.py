"""
The Case File — FastAPI Server v2.1
=====================================
Exposes the LangGraph + Cognee pipeline as a REST + true-streaming SSE API.

Endpoints
─────────
  GET  /api/health                   — Liveness probe
  POST /api/interrogate              — Full pipeline, single JSON response
  POST /api/interrogate/stream       — True SSE: one event per pipeline node
  GET  /api/case/{case_id}           — Accumulated case state
  GET  /api/case/{case_id}/report    — Latest compiled Markdown report
  POST /api/case/{case_id}/forget    — cognee.forget() — prune cleared dataset
  GET  /api/cases                    — List all cases

SSE stream contract (POST /api/interrogate/stream)
───────────────────────────────────────────────────
  The client sends a JSON body identical to POST /api/interrogate.
  The server emits events in this exact order:

    event: node_start
    data: {"node": "Detective", "stage": 1, "message": "…", "ts": "…"}

    event: node_done
    data: {"node": "Detective", "detective_reply": "…", "statement_id": "…",
           "stage": 1, "ts": "…"}

    event: node_start
    data: {"node": "Analyst", "stage": 2, "message": "…", "ts": "…"}

    event: node_done
    data: {"node": "Analyst", "has_contradiction": bool,
           "contradiction": {…} | null, "credibility_scores": {…},
           "stage": 2, "ts": "…"}

    event: contradiction          ← only if has_contradiction is true
    data: {"graph_edge": "…", "explanation": "…",
           "recall_confidence": 0.94, "credibility_scores": {…}, "ts": "…"}

    event: node_start
    data: {"node": "ChiefOfPolice", "stage": 3, "message": "…", "ts": "…"}

    event: node_done
    data: {"node": "ChiefOfPolice", "case_report_snippet": "…",
           "graph_citations": […], "stage": 3, "ts": "…"}

    event: complete
    data: {"statement_count": n, "session_count": n, "ts": "…"}

    event: error                  ← only on exception
    data: {"message": "…", "ts": "…"}

Design notes
────────────
  • The SSE stream runs the three LangGraph nodes sequentially using the
    lower-level node-by-node runner (run_node_by_node) instead of the
    compiled graph.ainvoke().  This lets us yield an SSE event after each
    node finishes rather than waiting for the whole pipeline.
  • State is accumulated in-memory keyed by case_id.  In production,
    swap _store for a Redis / PostgreSQL backend.
  • All credibility scores and contradictions accumulate across turns
    within the same case_id.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from langchain_core.messages import AIMessage, BaseMessage
from pydantic import BaseModel, Field

from investigator import (
    InvestigatorBureau,
    InvestigatorState,
    cognee_setup,
    memory_forget,
    memory_improve,
    run_session,
)

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("Starting The Case File API — running Cognee setup…")
    try:
        await cognee_setup()
        logger.info("Cognee configured successfully.")
    except Exception as exc:
        logger.error("Cognee setup failed (continuing anyway): %s", exc)
    yield
    logger.info("Shutting down The Case File API.")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="The Case File API",
    description="LangGraph + Cognee AI Investigator — stateful multi-session pipeline",
    version="2.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# In-memory case store
# ---------------------------------------------------------------------------

class CaseStore:
    """Per-case accumulated state. Replace with Redis/Postgres for production."""

    def __init__(self) -> None:
        self._cases: Dict[str, Dict[str, Any]] = {}

    # ── helpers ─────────────────────────────────────────────────────────
    @staticmethod
    def _default(case_id: str) -> Dict[str, Any]:
        return {
            "case_id":                case_id,
            "created_at":             _now_iso(),
            "statement_count":        0,
            "session_count":          0,
            "_seen_sessions":         set(),
            "active_suspects":        ["Marcus Harlow", "Renata Voss"],
            "credibility_scores":     {"harlow": 100.0, "voss": 100.0},
            "detected_contradictions": [],
            "graph_citations":        [],
            "case_report":            "",
            "last_updated":           None,
            "status":                 "active",
        }

    def get(self, case_id: str) -> Optional[Dict[str, Any]]:
        """Return existing case or None."""
        return self._cases.get(case_id)

    def init(self, case_id: str) -> Dict[str, Any]:
        """Return existing case or create a fresh one."""
        if case_id not in self._cases:
            self._cases[case_id] = self._default(case_id)
        return self._cases[case_id]

    def update(self, case_id: str, result: InvestigatorState,
               session_id: str = "") -> Dict[str, Any]:
        """Accumulate a pipeline result into the case store."""
        store = self.init(case_id)

        store["statement_count"] += 1

        # Track unique sessions and update session_count accordingly
        if session_id:
            store["_seen_sessions"].add(session_id)
            store["session_count"] = len(store["_seen_sessions"])

        # Overwrite with latest credibility (pipeline carries full dict)
        if result.get("credibility_scores"):
            store["credibility_scores"] = result["credibility_scores"]

        # Append unique contradictions
        for item in result.get("detected_contradictions", []):
            if item not in store["detected_contradictions"]:
                store["detected_contradictions"].append(item)

        # Append unique citations
        for item in result.get("graph_citations", []):
            if item not in store["graph_citations"]:
                store["graph_citations"].append(item)

        # Overwrite report if one was produced
        if result.get("case_report"):
            store["case_report"] = result["case_report"]

        store["last_updated"] = _now_iso()
        return store

    def all_cases(self) -> List[Dict[str, Any]]:
        return list(self._cases.values())


_store = CaseStore()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sse(event: str, data: Any) -> str:
    """Format a single Server-Sent Event frame."""
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


def _extract_detective_reply(messages: List[BaseMessage]) -> str:
    for msg in reversed(messages):
        if isinstance(msg, AIMessage):
            return msg.content
    return ""


def _contradiction_payload(result: InvestigatorState) -> Optional[Dict[str, Any]]:
    """Build the contradiction dict from pipeline result, or None."""
    if not result.get("detected_contradictions"):
        return None
    meta = result.get("pipeline_metadata", {})
    return {
        "has_contradiction":  True,
        "description":        result["detected_contradictions"][-1],
        "graph_edge":         meta.get("analyst_graph_edge", "—"),
        "recall_confidence":  meta.get("analyst_recall_confidence", 0.0),
        "credibility_delta":  meta.get("analyst_confidence_delta", 0.0),
    }


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class InterrogateRequest(BaseModel):
    input:        str = Field(..., min_length=1, max_length=2000,
                              description="The subject's statement.")
    case_id:      str = Field("case-2024-alpha-7",
                              description="Cognee dataset name / case namespace.")
    session_id:   str = Field("s1",
                              description="Session label, e.g. 's1', 's2'.")
    subject_name: str = Field("Subject",
                              description="Display name of the person being interrogated.")


class ContradictionInfo(BaseModel):
    has_contradiction: bool
    description:       str
    graph_edge:        str
    recall_confidence: float
    credibility_delta: float


class InterrogateResponse(BaseModel):
    detective_reply:     str
    statement_id:        str
    contradiction:       Optional[ContradictionInfo] = None
    credibility_scores:  Dict[str, float]
    statement_count:     int
    case_report_snippet: str
    graph_citations:     List[str]
    pipeline_metadata:   Dict[str, Any]
    processing_ms:       int


class ForgetRequest(BaseModel):
    dataset_name: str = Field(...,
        description="Exact Cognee dataset name to prune (usually equals case_id).")
    reason: str = Field("suspect_cleared",
        description="Human-readable reason, logged for audit.")


class CaseStateResponse(BaseModel):
    case_id:                 str
    created_at:              str
    last_updated:            Optional[str]
    status:                  str
    statement_count:         int
    session_count:           int
    active_suspects:         List[str]
    credibility_scores:      Dict[str, float]
    contradiction_count:     int
    detected_contradictions: List[str]
    graph_citations:         List[str]
    has_report:              bool


# ---------------------------------------------------------------------------
# Routes — system
# ---------------------------------------------------------------------------

@app.get("/api/health", tags=["system"])
async def health() -> Dict[str, Any]:
    """Liveness probe."""
    return {
        "status":       "ok",
        "service":      "case-file-api",
        "version":      "2.1.0",
        "active_cases": len(_store.all_cases()),
        "timestamp":    _now_iso(),
    }


# ---------------------------------------------------------------------------
# Routes — pipeline (REST)
# ---------------------------------------------------------------------------

@app.post("/api/interrogate", response_model=InterrogateResponse, tags=["pipeline"],
          summary="Full pipeline — single JSON response")
async def interrogate(req: InterrogateRequest) -> InterrogateResponse:
    """
    Run one subject statement through Detective → Analyst → ChiefOfPolice.
    Returns a single JSON response once all three nodes have completed.
    Use /api/interrogate/stream for real-time per-node updates.
    """
    case_state = _store.init(req.case_id)
    t0 = time.monotonic()

    try:
        result: InvestigatorState = await run_session(
            user_input         = req.input,
            case_id            = req.case_id,
            session_id         = req.session_id,
            subject_name       = req.subject_name,
            active_suspects    = case_state["active_suspects"],
            credibility_scores = case_state["credibility_scores"],
        )
    except Exception as exc:
        logger.exception("Pipeline error case=%s", req.case_id)
        raise HTTPException(status_code=500, detail=f"Pipeline error: {exc}")

    updated    = _store.update(req.case_id, result, session_id=req.session_id)
    elapsed_ms = int((time.monotonic() - t0) * 1000)

    cp         = _contradiction_payload(result)
    full_report = result.get("case_report", "")
    snippet     = full_report[:600] + "…" if len(full_report) > 600 else full_report

    logger.info("REST interrogate done case=%s stmt=%s contradiction=%s ms=%d",
                req.case_id, result.get("last_statement_id"), bool(cp), elapsed_ms)

    return InterrogateResponse(
        detective_reply     = _extract_detective_reply(result.get("messages", [])),
        statement_id        = result.get("last_statement_id", ""),
        contradiction       = ContradictionInfo(**cp) if cp else None,
        credibility_scores  = result.get("credibility_scores", {}),
        statement_count     = updated["statement_count"],
        case_report_snippet = snippet,
        graph_citations     = result.get("graph_citations", []),
        pipeline_metadata   = result.get("pipeline_metadata", {}),
        processing_ms       = elapsed_ms,
    )


# ---------------------------------------------------------------------------
# Routes — pipeline (true SSE stream)
# ---------------------------------------------------------------------------

@app.post("/api/interrogate/stream", tags=["pipeline"],
          summary="True SSE stream — one event per pipeline node")
async def interrogate_stream(req: InterrogateRequest) -> StreamingResponse:
    """
    Streams pipeline progress in real time using Server-Sent Events.

    The three LangGraph nodes run sequentially; after each node finishes
    its async work a node_done event is emitted immediately, so the
    frontend can update the UI without waiting for the full pipeline.

    Event sequence:
      node_start  → Detective begins
      node_done   → Detective reply available
      node_start  → Analyst begins
      node_done   → Analyst result available
      contradiction  (only when flagged)
      node_start  → Chief begins
      node_done   → Case report available
      complete    → Store updated; final stats
    """
    case_state = _store.init(req.case_id)
    bureau     = InvestigatorBureau(model=os.getenv("LLM_MODEL", "llama-3.3-70b-versatile"))

    async def generate() -> AsyncGenerator[str, None]:

        # ── Build initial state ──────────────────────────────────────
        from langchain_core.messages import HumanMessage

        state: InvestigatorState = {
            "messages":               [HumanMessage(content=req.input)],
            "current_input":          req.input,
            "case_id":                req.case_id,
            "session_id":             req.session_id,
            "subject_name":           req.subject_name,
            "detected_contradictions": [],
            "graph_citations":        [],
            "active_suspects":        case_state["active_suspects"],
            "credibility_scores":     dict(case_state["credibility_scores"]),
            "case_report":            "",
            "last_statement_id":      "",
            "pipeline_metadata":      {},
        }

        def _merge_state(base: InvestigatorState, patch: dict) -> InvestigatorState:
            """Merge a node's partial return dict into the running state."""
            updated = dict(base)
            for key, val in patch.items():
                if key == "messages":
                    updated["messages"] = list(base.get("messages", [])) + list(val)
                elif key == "detected_contradictions":
                    updated["detected_contradictions"] = (
                        list(base.get("detected_contradictions", [])) + list(val)
                    )
                elif key == "graph_citations":
                    updated["graph_citations"] = (
                        list(base.get("graph_citations", [])) + list(val)
                    )
                elif key == "pipeline_metadata":
                    # Merge dicts rather than overwrite
                    merged_meta = dict(base.get("pipeline_metadata") or {})
                    merged_meta.update(val)
                    updated["pipeline_metadata"] = merged_meta
                else:
                    updated[key] = val
            return updated  # type: ignore[return-value]

        # ── NODE 01: Detective ───────────────────────────────────────
        yield _sse("node_start", {
            "node":    "Detective",
            "stage":   1,
            "message": "Detective node active — logging statement via cognee.remember()…",
            "ts":      _now_iso(),
        })
        await asyncio.sleep(0)  # flush to client

        try:
            detective_patch = await bureau.detective_node(state)
        except Exception as exc:
            yield _sse("error", {"message": f"Detective node failed: {exc}", "ts": _now_iso()})
            return

        state = _merge_state(state, detective_patch)
        detective_reply = _extract_detective_reply(state["messages"])

        yield _sse("node_done", {
            "node":           "Detective",
            "stage":          1,
            "detective_reply": detective_reply,
            "statement_id":   state["last_statement_id"],
            "ts":             _now_iso(),
        })
        await asyncio.sleep(0)

        # ── NODE 02: Analyst ─────────────────────────────────────────
        yield _sse("node_start", {
            "node":    "Analyst",
            "stage":   2,
            "message": "Analyst node active — cognee.recall() + CoT contradiction analysis…",
            "ts":      _now_iso(),
        })
        await asyncio.sleep(0)

        try:
            analyst_patch = await bureau.analyst_node(state)
        except Exception as exc:
            yield _sse("error", {"message": f"Analyst node failed: {exc}", "ts": _now_iso()})
            return

        state = _merge_state(state, analyst_patch)
        cp    = _contradiction_payload(state)

        yield _sse("node_done", {
            "node":              "Analyst",
            "stage":             2,
            "has_contradiction": bool(cp),
            "contradiction":     cp,
            "credibility_scores": state.get("credibility_scores", {}),
            "ts":                _now_iso(),
        })
        await asyncio.sleep(0)

        # Emit dedicated contradiction event so frontend can fire alert immediately
        if cp:
            yield _sse("contradiction", {
                "graph_edge":        cp["graph_edge"],
                "explanation":       cp["description"],
                "recall_confidence": cp["recall_confidence"],
                "credibility_delta": cp["credibility_delta"],
                "credibility_scores": state.get("credibility_scores", {}),
                "ts":               _now_iso(),
            })
            await asyncio.sleep(0)

        # ── NODE 03: ChiefOfPolice ───────────────────────────────────
        yield _sse("node_start", {
            "node":    "ChiefOfPolice",
            "stage":   3,
            "message": "ChiefOfPolice node active — compiling case report via cognee.recall()…",
            "ts":      _now_iso(),
        })
        await asyncio.sleep(0)

        try:
            chief_patch = await bureau.chief_node(state)
        except Exception as exc:
            yield _sse("error", {"message": f"ChiefOfPolice node failed: {exc}", "ts": _now_iso()})
            return

        state = _merge_state(state, chief_patch)

        full_report = state.get("case_report", "")
        snippet     = full_report[:600] + "…" if len(full_report) > 600 else full_report

        yield _sse("node_done", {
            "node":               "ChiefOfPolice",
            "stage":              3,
            "case_report_snippet": snippet,
            "graph_citations":    state.get("graph_citations", []),
            "ts":                 _now_iso(),
        })
        await asyncio.sleep(0)

        # ── Persist & emit complete ──────────────────────────────────
        updated = _store.update(req.case_id, state, session_id=req.session_id)

        yield _sse("complete", {
            "statement_count": updated["statement_count"],
            "session_count":   updated["session_count"],
            "ts":              _now_iso(),
        })

        logger.info("SSE stream complete case=%s stmt=%s contradiction=%s",
                    req.case_id, state.get("last_statement_id"), bool(cp))

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache, no-transform",
            "Connection":        "keep-alive",
            "X-Accel-Buffering": "no",       # disable nginx buffering
            "Transfer-Encoding": "chunked",
        },
    )


# ---------------------------------------------------------------------------
# Routes — case management
# ---------------------------------------------------------------------------

@app.get("/api/case/{case_id}", response_model=CaseStateResponse, tags=["case"],
         summary="Return accumulated state for a case")
async def get_case(case_id: str) -> CaseStateResponse:
    state = _store.init(case_id)
    return CaseStateResponse(
        case_id                 = state["case_id"],
        created_at              = state["created_at"],
        last_updated            = state["last_updated"],
        status                  = state["status"],
        statement_count         = state["statement_count"],
        session_count           = state["session_count"],
        active_suspects         = state["active_suspects"],
        credibility_scores      = state["credibility_scores"],
        contradiction_count     = len(state["detected_contradictions"]),
        detected_contradictions = state["detected_contradictions"],
        graph_citations         = state["graph_citations"],
        has_report              = bool(state["case_report"]),
    )


@app.get("/api/case/{case_id}/report", tags=["case"],
         summary="Return the latest compiled Markdown case report")
async def get_report(case_id: str) -> Dict[str, str]:
    state  = _store.get(case_id)
    if not state:
        raise HTTPException(status_code=404,
            detail=f"Case '{case_id}' not found. Run at least one interrogation turn first.")
    report = state.get("case_report", "")
    if not report:
        raise HTTPException(status_code=404,
            detail=f"No report generated yet for case '{case_id}'.")
    return {
        "case_id":      case_id,
        "report":       report,
        "generated_at": state.get("last_updated", ""),
    }


@app.post("/api/case/{case_id}/forget", tags=["case"],
          summary="cognee.forget() — prune a cleared Cognee dataset")
async def forget(case_id: str, req: ForgetRequest) -> Dict[str, str]:
    """
    Prunes the specified Cognee dataset from the knowledge graph.
    This is cognee.forget() — irreversible for the named dataset.
    """
    logger.info("cognee.forget() case=%s dataset=%s reason=%s",
                case_id, req.dataset_name, req.reason)
    try:
        await memory_forget(req.dataset_name)
        return {
            "status":    "pruned",
            "dataset":   req.dataset_name,
            "reason":    req.reason,
            "pruned_at": _now_iso(),
        }
    except Exception as exc:
        logger.error("cognee.forget() failed dataset=%s: %s", req.dataset_name, exc)
        raise HTTPException(status_code=500, detail=f"Prune failed: {exc}")


@app.get("/api/cases", tags=["case"], summary="List all active cases")
async def list_cases() -> Dict[str, Any]:
    cases = _store.all_cases()
    return {
        "total": len(cases),
        "cases": [
            {
                "case_id":            c["case_id"],
                "status":             c["status"],
                "statement_count":    c["statement_count"],
                "session_count":      c["session_count"],
                "contradiction_count": len(c["detected_contradictions"]),
                "last_updated":       c["last_updated"],
            }
            for c in cases
        ],
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=os.getenv("DEV_RELOAD", "true").lower() == "true",
        log_level="info",
    )
