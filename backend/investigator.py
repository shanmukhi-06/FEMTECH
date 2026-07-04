"""
The Case File — LangGraph + Cognee v1.0 Investigator Pipeline
==============================================================
Three-node multi-agent pipeline powered by Groq (free tier, ultra-fast):

  node_01  Detective      — cognee.remember()  → logs every statement
  node_02  Analyst        — cognee.recall()    → cross-references for contradictions
                            cognee.improve()   → enriches graph on contradiction
  node_03  ChiefOfPolice  — cognee.recall()    → compiles case report
                            cognee.forget()    → prunes cleared suspects

Cognee v1.0 API surface used:
  cognee.remember(text, dataset_name, run_in_background=True)
  cognee.recall(query_text, datasets=[...])
  cognee.improve(dataset, run_in_background=True)
  cognee.forget(dataset=name)
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Annotated, Any, Dict, List, Optional

import cognee
from dotenv import load_dotenv
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_groq import ChatGroq
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field
from typing_extensions import TypedDict

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)


# ---------------------------------------------------------------------------
# List-field reducers for LangGraph Annotated state
# ---------------------------------------------------------------------------

def _append_list(existing: List[str], new: List[str]) -> List[str]:
    return existing + new


def _append_messages(
    existing: List[BaseMessage], new: List[BaseMessage]
) -> List[BaseMessage]:
    return existing + new


# ---------------------------------------------------------------------------
# Shared LangGraph State
# ---------------------------------------------------------------------------

class InvestigatorState(TypedDict):
    messages:                Annotated[List[BaseMessage], _append_messages]
    current_input:           str
    case_id:                 str
    session_id:              str
    subject_name:            str
    detected_contradictions: Annotated[List[str], _append_list]
    graph_citations:         Annotated[List[str], _append_list]
    active_suspects:         List[str]
    credibility_scores:      Dict[str, float]
    case_report:             str
    last_statement_id:       str
    pipeline_metadata:       Dict[str, Any]


# ---------------------------------------------------------------------------
# Pydantic structured-output schemas (LLM outputs)
# ---------------------------------------------------------------------------

class ContradictionAnalysis(BaseModel):
    has_contradiction:   bool  = Field(description="True if the new statement contradicts prior logged statements.")
    explanation:         str   = Field(description="Clear explanation of the discrepancy, or 'No contradiction found' if none.")
    confidence_delta:    float = Field(ge=0.0, le=1.0, description="Credibility penalty 0.0–1.0. Use 0.0 if no contradiction.")
    prior_statement_ref: str   = Field(default="", description="Reference tag of conflicting prior statement, or empty string if none.")
    graph_edge:          str   = Field(default="", description="Graph edge notation e.g. 'stmt_017 ⇔ stmt_004', or empty string if none.")
    recall_confidence:   float = Field(ge=0.0, le=1.0, description="Recall confidence 0.0–1.0. Use 0.0 if no prior context.")


class TimelineEvent(BaseModel):
    date:        str       = Field(description="Event date/time, e.g. 'Mar 14 · 20:47'")
    description: str       = Field(description="What happened, citing statement IDs.")
    tag:         str       = Field(description="CONTRADICTION | FLAG | VERIFIED | CLEARED | ORIGIN")
    refs:        List[str] = Field(description="Statement IDs referenced.")


class SuspectAssessment(BaseModel):
    name:               str
    role:               str   = Field(description="Primary | Associate | Cleared")
    credibility_pct:    float = Field(ge=0.0, le=100.0)
    contradiction_count: int
    status:             str   = Field(description="WATCH | MONITOR | CLEARED")
    summary:            str


class CaseReport(BaseModel):
    executive_summary:   str                    = Field(description="One-paragraph investigation summary.")
    timeline:            List[TimelineEvent]    = Field(description="Chronological events with evidence refs.")
    contradiction_log:   List[str]              = Field(description="All contradictions detected.")
    suspect_assessments: List[SuspectAssessment]= Field(description="Per-suspect credibility assessment.")
    recommendation:      str                    = Field(description="Chief's recommended next action.")
    graph_citations:     List[str]              = Field(description="Direct graph node/edge references.")
    overall_confidence:  str                    = Field(description="HIGH | MEDIUM | LOW")


# ---------------------------------------------------------------------------
# Cognee v1.0 Setup
# ---------------------------------------------------------------------------

async def cognee_setup() -> None:
    """
    Configure Cognee — connects to Cognee Cloud if credentials are present,
    otherwise runs locally with Groq as the LLM backend.
    """
    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key or groq_key.startswith("your_"):
        raise RuntimeError("GROQ_API_KEY not configured in .env")

    llm_model    = os.getenv("LLM_MODEL", "llama-3.3-70b-versatile")
    cognee_model = f"groq/{llm_model}" if not llm_model.startswith("groq/") else llm_model

    # ── Cognee Cloud connection (optional but recommended) ─────────────
    cognee_url = os.getenv("COGNEE_SERVICE_URL", "")
    cognee_key = os.getenv("COGNEE_API_KEY", "")

    if cognee_url and cognee_key and not cognee_url.startswith("your_"):
        logger.info("Connecting to Cognee Cloud → %s", cognee_url)
        try:
            await cognee.serve(url=cognee_url, api_key=cognee_key)
            logger.info("Cognee Cloud connected successfully.")
        except Exception as exc:
            logger.warning("Cognee Cloud connection failed (%s) — falling back to local.", exc)
            _setup_local_cognee(groq_key, cognee_model)
    else:
        logger.info("No Cognee Cloud credentials — using local storage.")
        _setup_local_cognee(groq_key, cognee_model)


def _setup_local_cognee(groq_key: str, cognee_model: str) -> None:
    """Configure Cognee to run locally with Groq via OpenAI-compatible endpoint."""
    os.environ["LLM_PROVIDER"] = "openai"
    os.environ["LLM_MODEL"]    = cognee_model
    os.environ["LLM_API_KEY"]  = groq_key
    os.environ["LLM_ENDPOINT"] = "https://api.groq.com/openai/v1"
    os.environ.setdefault("EMBEDDING_PROVIDER", "local")
    logger.info("Cognee configured → local mode | model=%s", cognee_model)


# ---------------------------------------------------------------------------
# Cognee v1.0 Memory Layer
# ---------------------------------------------------------------------------

async def memory_write(fact: str, case_id: str) -> None:
    """
    cognee.remember() — Persist a statement to the knowledge graph.

    Runs SYNCHRONOUSLY (run_in_background=False) so the graph is fully
    built before the Analyst node calls recall(). This ensures every
    statement is indexed and searchable immediately.

    self_improvement=True lets Cognee auto-enrich the graph after ingestion.
    """
    logger.info("cognee.remember() → dataset=%s | %.80s…", case_id, fact)
    try:
        await asyncio.wait_for(
            cognee.remember(
                fact,
                dataset_name=case_id,
                run_in_background=False,  # BLOCKING — ensures graph is ready for recall()
                self_improvement=False,   # skip auto-improve to keep latency low
            ),
            timeout=20.0,  # hard cap so a slow graph build never stalls forever
        )
    except asyncio.TimeoutError:
        logger.warning("cognee.remember() timed out for dataset=%s — continuing.", case_id)
    except Exception as exc:
        logger.warning("cognee.remember() failed (continuing): %s", exc)


async def memory_read(query: str, case_id: str) -> str:
    """
    cognee.recall() — Retrieve cross-session context from the graph.

    Returns a plain string of the most relevant results.
    RecallResponse items are Pydantic objects — use .text not .get("text").
    """
    logger.info("cognee.recall() → dataset=%s | %.80s…", case_id, query)
    try:
        results = await asyncio.wait_for(
            cognee.recall(
                query_text=query,
                datasets=[case_id],
                top_k=10,
            ),
            timeout=10.0,
        )
        if not results:
            return "No prior context found in memory."
        return _format_recall_results(results)
    except asyncio.TimeoutError:
        logger.warning("cognee.recall() timed out — no historical context available.")
        return "Memory retrieval timed out — proceeding without history."
    except Exception as exc:
        logger.error("cognee.recall() failed: %s", exc)
        return "Memory retrieval unavailable — proceeding without historical context."


def _format_recall_results(results: list) -> str:
    """
    Format cognee.recall() results into a plain string.
    RecallResponse items are Pydantic models:
      - result.source == "graph"   → use result.text
      - result.source == "session" → use result.answer
    """
    parts: List[str] = []
    for result in results:
        try:
            source = getattr(result, "source", None)
            if source == "graph":
                text = getattr(result, "text", None) or str(result)
            elif source == "session":
                text = getattr(result, "answer", None) or str(result)
            else:
                text = str(result)
            if text:
                parts.append(text)
        except Exception:
            parts.append(str(result))
    return "\n---\n".join(parts) if parts else "No results found."


async def memory_improve(case_id: str) -> None:
    """
    cognee.improve() — Enrich the knowledge graph for a dataset.
    Runs in background so it never blocks the pipeline.
    """
    logger.info("cognee.improve() [background] → dataset=%s", case_id)
    try:
        await cognee.improve(dataset=case_id, run_in_background=True)
    except Exception as exc:
        logger.warning("cognee.improve() failed (non-critical): %s", exc)


async def memory_forget(dataset_name: str) -> None:
    """
    cognee.forget() — Prune an entire dataset from the graph.
    Used when a suspect is fully cleared (implements forget()).
    """
    logger.info("cognee.forget() → dataset=%s", dataset_name)
    try:
        await cognee.forget(dataset=dataset_name)
    except Exception as exc:
        logger.error("cognee.forget() failed: %s", exc)
        raise


# ---------------------------------------------------------------------------
# Statement ID generator
# ---------------------------------------------------------------------------

_stmt_counter: Dict[str, int] = {}


def _next_stmt_id(case_id: str) -> str:
    _stmt_counter[case_id] = _stmt_counter.get(case_id, 0) + 1
    return f"stmt_{_stmt_counter[case_id]:03d}"


# ---------------------------------------------------------------------------
# Reliable in-memory statement log
# ---------------------------------------------------------------------------
# Cognee's graph build (cognify) can fail if LiteLLM can't route the model.
# This dict is the GUARANTEED source of truth for contradiction detection.
# Cognee still runs in background for the hackathon demo, but the Analyst
# always gets prior statements from here — it never returns empty.

_statement_log: Dict[str, List[Dict]] = {}   # case_id → [{id, session, text}]


def _log_statement(case_id: str, stmt_id: str, session_id: str, text: str) -> None:
    _statement_log.setdefault(case_id, []).append(
        {"id": stmt_id, "session": session_id, "text": text}
    )


def _get_prior_statements(case_id: str, current_stmt_id: str) -> str:
    """All prior statements for this case, formatted for the Analyst prompt."""
    prior = [
        s for s in _statement_log.get(case_id, [])
        if s["id"] != current_stmt_id
    ]
    if not prior:
        return "No prior statements recorded yet — this is the first statement."
    return "\n".join(
        f"[{s['id']} · {s['session']}] \"{s['text']}\""
        for s in prior
    )


# ---------------------------------------------------------------------------
# LLM factory — Groq via langchain-groq
# ---------------------------------------------------------------------------

def _make_llm(model: str | None = None) -> ChatGroq:
    # Strip "groq/" prefix if present — langchain-groq uses bare model names
    raw_model = model or os.getenv("LLM_MODEL", "llama-3.3-70b-versatile")
    clean_model = raw_model.removeprefix("groq/")
    return ChatGroq(
        model=clean_model,
        api_key=os.environ["GROQ_API_KEY"],
        temperature=0.2,
        max_retries=2,
    )


# ---------------------------------------------------------------------------
# Agent node implementations
# ---------------------------------------------------------------------------

class InvestigatorBureau:
    """Container for all three LangGraph agent node callables."""

    def __init__(self, model: str | None = None) -> None:
        self.llm = _make_llm(model)
        clean = (model or os.getenv("LLM_MODEL", "llama-3.3-70b-versatile")).removeprefix("groq/")
        logger.info("InvestigatorBureau ready → %s", clean)

    # ── NODE 01: Detective ──────────────────────────────────────────────
    async def detective_node(self, state: InvestigatorState) -> dict:
        """
        Receives the subject's raw statement.
        1. Assigns a statement ID and calls cognee.remember() (background).
        2. Generates a probing detective reply (25 s hard timeout).
        """
        user_input   = state["current_input"]
        case_id      = state["case_id"]
        session_id   = state["session_id"]
        subject_name = state.get("subject_name", "Subject")
        stmt_id      = _next_stmt_id(case_id)

        # ── cognee.remember() + in-memory log ─────────────────────────
        fact = (
            f"[{session_id}] [{stmt_id}] [{datetime.now(timezone.utc).isoformat()}] "
            f"Subject {subject_name} stated: {user_input}"
        )
        # Log to reliable in-memory store FIRST (always works)
        _log_statement(case_id, stmt_id, session_id, user_input)
        # Also send to Cognee in background (best-effort, may fail)
        await memory_write(fact, case_id)

        # ── LLM: detective reply (25 s timeout) ────────────────────────
        try:
            response = await asyncio.wait_for(
                self.llm.ainvoke([
                    {
                        "role": "system",
                        "content": (
                            "You are a sharp, experienced criminal detective running a structured "
                            "multi-session interrogation. Stay calm and methodical. Never reveal "
                            "your evidence directly — press for elaboration and let contradictions "
                            "surface naturally. Keep your response under 3 sentences. "
                            "Always end with a specific follow-up question."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Statement ID {stmt_id} logged. The subject just said:\n\n"
                            f'"{user_input}"\n\nRespond as the detective.'
                        ),
                    },
                ]),
                timeout=25.0,
            )
            reply = response.content
        except asyncio.TimeoutError:
            logger.warning("Detective LLM timed out for %s — using fallback.", stmt_id)
            reply = (
                "That's noted. We'll return to that claim when we have more context. "
                "Can you walk me through your exact movements that evening?"
            )

        logger.info("Detective node complete → %s", stmt_id)
        return {
            "messages":          [AIMessage(content=reply, name="Detective")],
            "last_statement_id": stmt_id,
            "pipeline_metadata": {
                "detective_stmt_id": stmt_id,
                "detective_ts":      datetime.now(timezone.utc).isoformat(),
            },
        }

    # ── NODE 02: Analyst ────────────────────────────────────────────────
    async def analyst_node(self, state: InvestigatorState) -> dict:
        """
        Out-of-band cognitive engine.
        1. cognee.recall() — cross-references memory (10 s timeout).
        2. Structured CoT contradiction analysis via Gemini (30 s timeout).
        3. cognee.improve() if contradiction found — enriches graph.
        """
        case_id      = state["case_id"]
        latest_input = state["current_input"]
        session_id   = state["session_id"]
        stmt_id      = state.get("last_statement_id", "stmt_???")

        # ── PRIMARY: in-memory statement log (always reliable) ─────────
        # This is the guaranteed source — works even when Cognee's graph
        # build fails due to LiteLLM routing issues.
        primary_context = _get_prior_statements(case_id, stmt_id)
        logger.info("Analyst in-memory context: %d prior statements",
                    len(_statement_log.get(case_id, [])) - 1)

        # ── SECONDARY: cognee.recall() (best-effort enrichment) ────────
        # Run in background — if it returns something, merge it in.
        # If it times out or fails, we already have primary_context.
        cognee_context = ""
        try:
            cognee_context = await asyncio.wait_for(
                memory_read("All prior statements made by the subject", case_id),
                timeout=8.0,
            )
            if "No prior context" in cognee_context or "timed out" in cognee_context:
                cognee_context = ""
        except Exception:
            cognee_context = ""

        # Build the final context for the LLM
        if cognee_context:
            historical_context = (
                f"=== STATEMENT LOG (in-memory, reliable) ===\n{primary_context}\n\n"
                f"=== COGNEE GRAPH CONTEXT (additional enrichment) ===\n{cognee_context}"
            )
        else:
            historical_context = primary_context

        logger.info("Analyst total context length: %d chars", len(historical_context))

        # ── LLM: structured contradiction analysis (30 s timeout) ──────
        structured_llm = self.llm.with_structured_output(ContradictionAnalysis)
        try:
            analysis: ContradictionAnalysis = await asyncio.wait_for(
                structured_llm.ainvoke([
                    {
                        "role": "system",
                        "content": (
                            "You are a sharp forensic analyst for a criminal interrogation. "
                            "Your ONLY job is to find factual contradictions between a new "
                            "statement and everything the subject has said before. "
                            "A contradiction means: the subject said X before and now says NOT-X. "
                            "EXAMPLES OF CONTRADICTIONS YOU MUST CATCH:\n"
                            "- 'I never left home' → later says 'I stepped out'\n"
                            "- 'I don't travel for work' → later says 'I flew to Chicago'\n"
                            "- 'I don't know Renata Voss' → later says 'she is my project director'\n"
                            "- 'I have no criminal record' → later admits 'I was arrested in 2018'\n"
                            "RULES:\n"
                            "1. If ANY contradiction exists, set has_contradiction=true\n"
                            "2. confidence_delta should be 0.15-0.25 for clear contradictions\n"
                            "3. prior_statement_ref: use the stmt ID if visible, else 'prior_statement'\n"
                            "4. graph_edge: format as 'new_stmt ⇔ prior_stmt'\n"
                            "5. NEVER return null — use empty string '' if no value\n"
                            "6. recall_confidence: how certain you are (0.0-1.0)"
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"=== ALL PRIOR STATEMENTS (reliable in-memory log) ===\n"
                            f"{historical_context}\n\n"
                            f"=== NEW STATEMENT [{session_id} · {stmt_id}] ===\n"
                            f'"{latest_input}"\n\n'
                            "TASK: Does the NEW STATEMENT directly contradict anything in the "
                            "prior statements above?\n"
                            "Look for: location claims, travel claims, relationship claims, "
                            "timeline claims, work habits, access claims, criminal history.\n"
                            "Be precise — quote the conflicting phrases. Flag it if the conflict is clear."
                        ),
                    },
                ]),
                timeout=30.0,
            )
        except asyncio.TimeoutError:
            logger.warning("Analyst LLM timed out for %s — no contradiction.", stmt_id)
            analysis = ContradictionAnalysis(
                has_contradiction=False, explanation="Analysis timed out.",
                confidence_delta=0.0, prior_statement_ref="",
                graph_edge="", recall_confidence=0.0,
            )

        new_contradictions: List[str] = []
        new_citations:      List[str] = []
        credibility = dict(state.get("credibility_scores", {}))

        if analysis.has_contradiction:
            entry = (
                f"[{stmt_id}] {analysis.explanation} "
                f"| edge: {analysis.graph_edge} "
                f"| recall: {analysis.recall_confidence:.0%} "
                f"| Δ: −{analysis.confidence_delta * 100:.0f}%"
            )
            new_contradictions.append(entry)
            if analysis.graph_edge:
                new_citations.append(analysis.graph_edge)

            # Write contradiction back to memory and enrich graph
            await memory_write(
                f"[{session_id}] [CONTRADICTION] [{stmt_id}] {analysis.explanation} "
                f"Graph edge: {analysis.graph_edge}",
                case_id,
            )
            await memory_improve(case_id)

            # Update primary suspect credibility
            suspect_key = _resolve_suspect_key(state.get("active_suspects", []))
            credibility[suspect_key] = max(
                0.0,
                credibility.get(suspect_key, 100.0) - (analysis.confidence_delta * 100),
            )
            logger.info("Contradiction → %s | credibility[%s]=%.1f",
                        analysis.graph_edge, suspect_key, credibility[suspect_key])
        else:
            logger.info("Analyst: no contradiction for %s.", stmt_id)

        return {
            "detected_contradictions": new_contradictions,
            "graph_citations":         new_citations,
            "credibility_scores":      credibility,
            "pipeline_metadata": {
                "analyst_has_contradiction":  analysis.has_contradiction,
                "analyst_recall_confidence":  analysis.recall_confidence,
                "analyst_graph_edge":         analysis.graph_edge,
                "analyst_confidence_delta":   analysis.confidence_delta,
            },
        }

    # ── NODE 03: ChiefOfPolice ──────────────────────────────────────────
    async def chief_node(self, state: InvestigatorState) -> dict:
        """
        Final node — compiles the official case report.
        1. cognee.recall() — pulls full timeline (10 s timeout).
        2. Generates structured CaseReport via Gemini (45 s timeout).
        3. Formats as Markdown for the Chief's Briefing panel.
        """
        case_id = state["case_id"]

        # ── In-memory log (primary) + cognee.recall() (enrichment) ────
        all_stmts = _get_prior_statements(case_id, "none")
        try:
            cognee_ctx = await asyncio.wait_for(
                memory_read("All statements, contradictions, and evidence in this case", case_id),
                timeout=8.0,
            )
            if not cognee_ctx or "No prior context" in cognee_ctx or "timed out" in cognee_ctx:
                full_context = all_stmts
            else:
                full_context = f"STATEMENT LOG:\n{all_stmts}\n\nCOGNEE GRAPH:\n{cognee_ctx}"
        except Exception:
            full_context = all_stmts

        contradictions_text = "\n".join(
            f"  • {c}" for c in state.get("detected_contradictions", [])
        ) or "  None logged in this session."

        credibility_text = "\n".join(
            f"  • {k}: {v:.1f}%" for k, v in state.get("credibility_scores", {}).items()
        ) or "  No credibility data."

        # ── LLM: structured case report (45 s timeout) ─────────────────
        structured_llm = self.llm.with_structured_output(CaseReport)
        try:
            report: CaseReport = await asyncio.wait_for(
                structured_llm.ainvoke([
                    {
                        "role": "system",
                        "content": (
                            "You are the Chief of Police compiling an official investigative "
                            "report. Be factual, precise, and professional. Cite statement IDs. "
                            "Set overall_confidence HIGH for multiple verified contradictions, "
                            "MEDIUM if partially confirmed, LOW if mostly unverified."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Case ID: {case_id}\n\n"
                            f"Knowledge graph context (cognee.recall):\n"
                            f"{'─' * 60}\n{full_context}\n{'─' * 60}\n\n"
                            f"Contradictions:\n{contradictions_text}\n\n"
                            f"Credibility scores:\n{credibility_text}\n\n"
                            "Generate the full official case report."
                        ),
                    },
                ]),
                timeout=45.0,
            )
        except asyncio.TimeoutError:
            logger.warning("ChiefOfPolice LLM timed out — returning minimal report.")
            report = CaseReport(
                executive_summary=(
                    f"Report generation timed out. "
                    f"{len(state.get('detected_contradictions', []))} contradiction(s) detected."
                ),
                timeline=[], contradiction_log=list(state.get("detected_contradictions", [])),
                suspect_assessments=[], recommendation="Re-run report when LLM is available.",
                graph_citations=list(state.get("graph_citations", [])),
                overall_confidence="LOW",
            )

        md = _render_markdown_report(report, state)
        logger.info("ChiefOfPolice complete → %d timeline events.", len(report.timeline))

        return {
            "case_report":     md,
            "graph_citations": report.graph_citations,
            "pipeline_metadata": {
                "chief_overall_confidence": report.overall_confidence,
                "chief_suspects_count":     len(report.suspect_assessments),
            },
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_suspect_key(active_suspects: List[str]) -> str:
    if not active_suspects:
        return "primary"
    return active_suspects[0].split()[-1].lower()


def _render_markdown_report(report: CaseReport, state: InvestigatorState) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    tag_emoji = {"CONTRADICTION": "🔴", "FLAG": "🟡", "VERIFIED": "🟢",
                 "CLEARED": "⚪", "ORIGIN": "🔵"}

    lines = [
        "# OFFICIAL CASE REPORT", "",
        "| Field | Value |",
        "|-------|-------|",
        f"| **Case ID** | `{state['case_id']}` |",
        f"| **Generated** | {now} |",
        f"| **Contradictions** | {len(state.get('detected_contradictions', []))} |",
        f"| **Overall Confidence** | **{report.overall_confidence}** |",
        "", "---", "",
        "## Executive Summary", "", report.executive_summary,
        "", "---", "", "## Event Timeline", "",
    ]

    for event in report.timeline:
        emoji    = tag_emoji.get(event.tag.upper(), "⬜")
        refs_str = " · ".join(f"`{r}`" for r in event.refs) if event.refs else ""
        lines.append(
            f"- **{event.date}** {emoji} `{event.tag}` — {event.description}"
            + (f"  *(refs: {refs_str})*" if refs_str else "")
        )

    lines += ["", "---", "", "## Contradiction Log", ""]
    if report.contradiction_log:
        for c in report.contradiction_log:
            lines.append(f"- ⚠ {c}")
    else:
        lines.append("*No contradictions recorded in this session.*")

    lines += [
        "", "---", "",
        "## Suspect Assessments", "",
        "| Suspect | Role | Credibility | Contradictions | Status |",
        "|---------|------|-------------|----------------|--------|",
    ]
    for s in report.suspect_assessments:
        lines.append(
            f"| **{s.name}** | {s.role} | {s.credibility_pct:.0f}% "
            f"| {s.contradiction_count} | `{s.status}` |"
        )

    lines += [
        "", "---", "", "## Chief's Recommendation", "", report.recommendation,
        "", "---", "", "## Direct Graph Citations", "",
    ]
    for citation in report.graph_citations:
        lines.append(f"- `{citation}`")

    lines += ["", "---",
              "*Generated by ChiefOfPolice · cognee.recall() + LangGraph + Groq*"]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# LangGraph — build and compile
# ---------------------------------------------------------------------------

def build_investigator_graph() -> Any:
    """
    Detective → Analyst → ChiefOfPolice → END
    """
    bureau   = InvestigatorBureau()
    workflow = StateGraph(InvestigatorState)

    workflow.add_node("Detective",     bureau.detective_node)
    workflow.add_node("Analyst",       bureau.analyst_node)
    workflow.add_node("ChiefOfPolice", bureau.chief_node)

    workflow.set_entry_point("Detective")
    workflow.add_edge("Detective",     "Analyst")
    workflow.add_edge("Analyst",       "ChiefOfPolice")
    workflow.add_edge("ChiefOfPolice", END)

    compiled = workflow.compile()
    logger.info("LangGraph compiled: Detective → Analyst → ChiefOfPolice → END")
    return compiled


# ---------------------------------------------------------------------------
# Public run entrypoint
# ---------------------------------------------------------------------------

async def run_session(
    user_input:         str,
    case_id:            str                  = "case-2024-alpha-7",
    session_id:         str                  = "s1",
    subject_name:       str                  = "Subject",
    active_suspects:    Optional[List[str]]  = None,
    credibility_scores: Optional[Dict[str, float]] = None,
) -> InvestigatorState:
    graph = build_investigator_graph()

    initial: InvestigatorState = {
        "messages":               [HumanMessage(content=user_input)],
        "current_input":          user_input,
        "case_id":                case_id,
        "session_id":             session_id,
        "subject_name":           subject_name,
        "detected_contradictions": [],
        "graph_citations":        [],
        "active_suspects":        active_suspects or ["Subject"],
        "credibility_scores":     credibility_scores or {"subject": 100.0},
        "case_report":            "",
        "last_statement_id":      "",
        "pipeline_metadata":      {},
    }
    return await graph.ainvoke(initial)


# ---------------------------------------------------------------------------
# CLI smoke test  —  python investigator.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json

    async def _smoke_test() -> None:
        await cognee_setup()

        statements = [
            ("s1", "I was at home all evening on March 14th. Never left the house."),
            ("s2", "Oh — I forgot. I stepped out briefly for takeout that night."),
            ("s3", "I don't travel for work. I handle everything remotely."),
        ]

        cred: Dict[str, float] = {"harlow": 100.0, "voss": 100.0}
        suspects = ["Marcus Harlow", "Renata Voss"]

        result = None
        for sid, stmt in statements:
            print(f"\n{'═'*60}")
            print(f"SESSION {sid} | {stmt[:60]}…")
            print("═" * 60)

            result = await run_session(
                user_input=stmt, case_id="smoke-test-001",
                session_id=sid, subject_name="Marcus Harlow",
                active_suspects=suspects, credibility_scores=cred,
            )
            cred = result["credibility_scores"]

            for msg in result["messages"]:
                if isinstance(msg, AIMessage) and msg.name == "Detective":
                    print(f"\n[Detective]\n{msg.content}")
                    break

            if result["detected_contradictions"]:
                print("\n[Contradictions]")
                for c in result["detected_contradictions"]:
                    print(f"  ⚠  {c}")

            print(f"\n[Credibility] {json.dumps(cred, indent=2)}")

        if result:
            print(f"\n{'═'*60}\n[CASE REPORT]\n{'═'*60}")
            print(result["case_report"])

    asyncio.run(_smoke_test())
