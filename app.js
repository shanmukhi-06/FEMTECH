/* ================================================================
   THE CASE FILE — AI Investigator Command Center
   Application Logic v3.0  —  Live Backend Edition
   ================================================================
   Send button → POST /api/interrogate/stream (SSE)
   Each SSE event updates the UI as the pipeline node completes:

     node_start        → stream bar label + op-chip highlight
     node_done[Det.]   → detective reply bubble in chat
     node_done[Analyst]→ credibility bars + analyst note in chat
     contradiction     → Contradiction Alert banner + feed item
     node_done[Chief]  → report snippet in Chief's Briefing
     complete          → stat counters, stream bar idle
     error             → inline error bubble in chat

   Falls back to POST /api/interrogate (REST) if EventSource fails
   or the backend is unreachable, then falls back to local simulation
   so the demo still runs without a server.
   ================================================================ */
'use strict';

// ── Config ──────────────────────────────────────────────────
const API_BASE    = 'http://127.0.0.1:8000';
const CASE_ID     = 'case-2024-alpha-7';
const SUBJECT     = 'Marcus Harlow';

// ── Application State ────────────────────────────────────────
const APP = {
  // counters that start from the pre-seeded demo data in the HTML
  statements:     17,
  sessions:       4,
  contradictions: 3,
  graphNodes:     42,

  // credibility mirrors (kept in sync with backend responses)
  harlowCred:     42,
  vossCred:       67,

  // runtime flags
  isProcessing:   false,
  currentSession: 1,
  sessionId:      's1',
  lockdown:       false,
  filterMode:     'all',
  backendOnline:  null,    // null = not checked yet, true/false after health ping

  // full message log (used by export)
  messageLog:     [],

  // polling
  pollIntervalId: null,       // handle returned by setInterval
  pollLastStmtCount:  0,      // last statement_count seen from the server
  pollLastContraCount: 0,     // last contradiction_count seen from the server
  pollLastHarlow: null,       // last harlow credibility seen from the server
  pollLastVoss:   null,       // last voss credibility seen from the server
  pollLastReport: '',         // last case_report seen from the server
};

// ── DOM Cache ────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const DOM = {
  chatHistory:      $('chatHistory'),
  chatInput:        $('chatInput'),
  sendBtn:          $('sendBtn'),
  streamDot:        $('streamDot'),
  streamText:       $('streamText'),
  streamBytes:      $('streamBytes'),
  inputCounter:     $('inputCounter'),
  typingIndicator:  $('typingIndicator'),
  factFeed:         $('factFeed'),

  // top-bar counters
  statStmt:         $('statementCount'),
  statContra:       $('contradictionCount'),
  statNodes:        $('graphNodes'),
  statSessions:     $('sessionCount'),

  // analyst panel — credibility
  harlowBar:        $('harlowBar'),
  harlowScore:      $('harlowScore'),
  vossBar:          $('vossBar'),
  vossScore:        $('vossScore'),

  // suspects table (right panel)
  tableHarlow:      $('tableHarlowScore'),
  tableVoss:        $('tableVossScore'),

  // contradiction alert fields
  caAlert:          $('contradictionAlert'),
  caCount:          $('caCount'),
  caSession:        $('caSession'),
  caLabelA:         $('caLabelA'),
  caTextA:          $('caTextA'),
  caLabelB:         $('caLabelB'),
  caTextB:          $('caTextB'),
  caCredDelta:      $('caCredDelta'),
  caRecallMatch:    $('caRecallMatch'),
  caEdge:           $('caEdge'),
};

// ════════════════════════════════════════════════════════════
//  INIT
// ════════════════════════════════════════════════════════════
document.addEventListener('DOMContentLoaded', () => {
  DOM.chatHistory.scrollTop = DOM.chatHistory.scrollHeight;
  DOM.chatInput.focus();
  setStream('idle', 'Idle · cognee.memory synced · Graph namespace stable', null);
  pingBackend();
  startLiveSimulation();
  initGraphCanvas();
});

// ── Health-check the backend once on load ────────────────────
async function pingBackend() {
  try {
    const res = await fetch(`${API_BASE}/api/health`, { signal: AbortSignal.timeout(3000) });
    APP.backendOnline = res.ok;
    if (res.ok) {
      const data = await res.json();
      console.info('[CaseFile] Backend online — version', data.version);
      // Sync initial case state from server, then start the poller
      await syncCaseState();
      startCasePoller();
    }
  } catch {
    APP.backendOnline = false;
    console.warn('[CaseFile] Backend offline — running in simulation mode.');
    showSystemMsg('Backend not reachable. Running in local simulation mode.');
  }
}

// ── Pull accumulated case state from server ──────────────────
async function syncCaseState() {
  try {
    const res  = await fetch(`${API_BASE}/api/case/${CASE_ID}`);
    if (!res.ok) return;
    const data = await res.json();

    // Override stat counters with server-authoritative values
    if (data.statement_count > 0) {
      APP.statements = data.statement_count;
      DOM.statStmt.textContent = APP.statements;
    }
    if (data.contradiction_count > 0) {
      APP.contradictions = data.contradiction_count;
      DOM.statContra.textContent = APP.contradictions;
    }
    if (data.session_count > 0) {
      APP.sessions = data.session_count;
      DOM.statSessions.textContent = APP.sessions;
    }

    // Sync credibility scores
    applyCreditScores(data.credibility_scores || {});

    console.info('[CaseFile] Case state synced from server:', data);
  } catch (err) {
    console.warn('[CaseFile] syncCaseState failed:', err.message);
  }
}

// ── 5-second case poller ─────────────────────────────────────
/**
 * startCasePoller()
 * -----------------
 * Polls GET /api/case/{CASE_ID} every 5 seconds while the backend
 * is online. On each tick it diffs the response against the last
 * known state and only triggers UI updates when something actually
 * changed, preventing unnecessary re-renders during a quiet interval.
 *
 * Three areas are updated from the polled data:
 *
 *  1. Suspect Credibility Index (analyst panel + suspects table)
 *     Driven by credibility_scores dict. Calls applyCreditScores()
 *     which animates bars, updates percentage labels, adds delta chips
 *     if a score dropped, and syncs the suspects table.
 *
 *  2. Contradiction Alert banner (analyst panel)
 *     Triggered when contradiction_count increases. The latest entry
 *     from detected_contradictions is parsed and used to fill every
 *     field of the alert card: count badge, session label, description,
 *     graph edge, recall confidence estimate. The alert becomes visible
 *     and the flashing border animation restarts.
 *
 *  3. Chief's Official Briefing (right panel)
 *     Triggered when case_report changes. The new report is passed to
 *     updateReportSnippet() which writes the executive summary text,
 *     refreshes the timestamp footer, and calls updateGraphCitations()
 *     to prepend any new graph edges to the Direct Citations section.
 *
 * The poller is paused (skips its work, not stopped) while a pipeline
 * request is in-flight (APP.isProcessing) to avoid race conditions
 * between the SSE stream and the polling response.
 *
 * stopCasePoller() clears the interval and is called automatically
 * when the tab becomes hidden (Page Visibility API) to avoid
 * unnecessary network traffic.
 */
function startCasePoller() {
  if (APP.pollIntervalId !== null) return;   // already running

  // Seed the "last seen" baseline from the current APP state so the
  // first tick only fires updates for genuinely new data.
  APP.pollLastStmtCount   = APP.statements;
  APP.pollLastContraCount = APP.contradictions;
  APP.pollLastHarlow      = APP.harlowCred;
  APP.pollLastVoss        = APP.vossCred;
  APP.pollLastReport      = '';

  APP.pollIntervalId = setInterval(pollCaseState, 5000);
  console.info('[CaseFile] Poller started — interval: 5 s');

  // Pause polling while tab is hidden, resume when visible again
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      stopCasePoller();
    } else if (APP.backendOnline) {
      startCasePoller();
    }
  });
}

function stopCasePoller() {
  if (APP.pollIntervalId === null) return;
  clearInterval(APP.pollIntervalId);
  APP.pollIntervalId = null;
  console.info('[CaseFile] Poller stopped.');
}

async function pollCaseState() {
  // Skip this tick if a pipeline turn is still in-flight to avoid
  // the poller and the SSE stream fighting over the same DOM nodes.
  if (APP.isProcessing) return;

  let data;
  try {
    const res = await fetch(`${API_BASE}/api/case/${CASE_ID}`, {
      signal: AbortSignal.timeout(4000),   // must resolve before next tick
    });
    if (!res.ok) {
      // 404 just means the case hasn't had any turns yet — not an error.
      if (res.status !== 404) console.warn('[Poller] Unexpected status', res.status);
      return;
    }
    data = await res.json();
  } catch (err) {
    // Network failure — mark backend offline so the next Send falls back
    // to local simulation rather than hanging.
    console.warn('[Poller] Fetch failed:', err.message);
    APP.backendOnline = false;
    stopCasePoller();
    return;
  }

  // ── 1. Stat counters ────────────────────────────────────────────────
  if (data.statement_count > APP.pollLastStmtCount) {
    APP.statements           = data.statement_count;
    APP.pollLastStmtCount    = data.statement_count;
    DOM.statStmt.textContent = APP.statements;
    bumpCounter(DOM.statStmt);
    // A new statement means a new pipeline turn — the Chief will produce
    // a fresh report, so allow the report fetch to run again.
    APP.pollLastReport = '';
  }
  if (data.session_count > 0 && data.session_count !== APP.sessions) {
    APP.sessions                  = data.session_count;
    DOM.statSessions.textContent  = APP.sessions;
  }

  // ── 2. Credibility Index ─────────────────────────────────────────────
  // applyCreditScores already diffs against APP.harlowCred / APP.vossCred
  // so calling it unconditionally is safe — it only animates on change.
  const scores = data.credibility_scores || {};
  applyCreditScores(scores);

  // Update our poller baseline after applying
  if (scores.harlow !== undefined) APP.pollLastHarlow = Math.round(scores.harlow);
  if (scores.voss   !== undefined) APP.pollLastVoss   = Math.round(scores.voss);

  // ── 3. Contradiction Alert ───────────────────────────────────────────
  if (data.contradiction_count > APP.pollLastContraCount) {
    const newCount  = data.contradiction_count;
    const newDelta  = newCount - APP.pollLastContraCount;

    APP.contradictions           = newCount;
    APP.pollLastContraCount      = newCount;
    DOM.statContra.textContent   = newCount;
    bumpCounter(DOM.statContra);

    // Use the latest contradiction string from detected_contradictions.
    // Format is:  "[stmt_XXX] <explanation> | edge: <edge> | recall: XX% | Δ: −XX%"
    const latestRaw = (data.detected_contradictions || []).at(-1) || '';
    _applyContradictionFromPoll(latestRaw, newCount, scores);
  }

  // ── 4. Chief's Briefing — report & citations ─────────────────────────
  if (data.has_report && APP.pollLastReport !== 'fetched') {
    // The case endpoint returns has_report:bool but not the full report
    // text (it would bloat every poll response).  Fetch the dedicated
    // report endpoint only when has_report flips to true or changes.
    _pollFetchReport();
  }

  // ── 5. Graph citations ───────────────────────────────────────────────
  if ((data.graph_citations || []).length) {
    updateGraphCitations(data.graph_citations);
  }
}

/**
 * Parse the raw contradiction string from detected_contradictions and
 * populate the Contradiction Alert banner without requiring a stmtId
 * (we only have the accumulated list here, not the live turn context).
 *
 * Raw format from investigator.py:
 *   "[stmt_003] <explanation> | edge: stmt_017 ⇔ stmt_004 | recall: 94% | Δ: −18%"
 */
function _applyContradictionFromPoll(raw, totalCount, credScores) {
  if (!raw) return;

  // ── Parse fields from the raw string ────────────────────────────────
  const stmtMatch     = raw.match(/^\[([^\]]+)\]/);
  const edgeMatch     = raw.match(/edge:\s*([^|]+)/);
  const recallMatch   = raw.match(/recall:\s*([\d.]+)%/);
  const deltaMatch    = raw.match(/Δ:\s*[−-]([\d.]+)%/);

  const stmtId        = stmtMatch  ? stmtMatch[1].trim()        : '—';
  const graphEdge     = edgeMatch  ? edgeMatch[1].trim()         : '—';
  const recallPct     = recallMatch ? parseFloat(recallMatch[1]) : null;
  const deltaPct      = deltaMatch  ? parseFloat(deltaMatch[1])  : null;

  // Strip the metadata tokens to get the human-readable explanation
  const explanation   = raw
    .replace(/^\[[^\]]+\]\s*/, '')
    .replace(/\s*\|.*$/, '')
    .trim();

  // ── Fill alert banner ─────────────────────────────────────────────
  DOM.caCount.textContent       = `${totalCount} flagged`;
  DOM.caSession.textContent     = `stmt ${stmtId} · polled`;
  DOM.caLabelA.textContent      = 'Prior statement · Cognee graph memory';
  DOM.caTextA.innerHTML         = escHtml(explanation);
  DOM.caLabelB.textContent      = `Contradicting statement · ${stmtId}`;
  DOM.caTextB.innerHTML         = `Graph edge: <code>${escHtml(graphEdge)}</code>`;
  DOM.caCredDelta.textContent   = deltaPct  !== null ? `−${deltaPct.toFixed(0)}%`     : '—';
  DOM.caRecallMatch.textContent = recallPct !== null ? `${recallPct.toFixed(1)}%`     : '—';
  DOM.caEdge.textContent        = graphEdge;

  // ── Show & animate ───────────────────────────────────────────────
  // Force a CSS re-trigger so the blink-border animation restarts
  DOM.caAlert.classList.remove('active');
  void DOM.caAlert.offsetWidth;                    // reflow
  DOM.caAlert.classList.add('active');
  DOM.caAlert.scrollIntoView({ behavior: 'smooth', block: 'nearest' });

  // ── Live feed entry ──────────────────────────────────────────────
  addFeedItem(
    'contradiction',
    explanation || 'Contradiction flagged by Analyst node (polled).',
    stmtId,
    graphEdge !== '—' ? graphEdge : null,
    deltaPct ? Math.round(deltaPct) : 0,
  );

  // ── Credibility from the same poll tick ──────────────────────────
  if (credScores && Object.keys(credScores).length) {
    applyCreditScores(credScores);
  }
}

/**
 * Fetch the full case report from /api/case/{CASE_ID}/report and
 * apply it to the Chief's Briefing panel.
 * Uses a flag to avoid hammering the endpoint on every tick once
 * the report has already been fetched and applied.
 */
async function _pollFetchReport() {
  // Guard: only fetch once per report version
  if (APP.pollLastReport === 'fetched') return;

  try {
    const res = await fetch(`${API_BASE}/api/case/${CASE_ID}/report`, {
      signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) return;
    const data = await res.json();
    if (!data.report) return;

    updateReportSnippet(data.report);
    APP.pollLastReport = 'fetched';
    console.info('[Poller] Case report applied to Chief\'s Briefing.');
  } catch (err) {
    console.warn('[Poller] Report fetch failed:', err.message);
  }
}

// ════════════════════════════════════════════════════════════
//  INPUT HANDLING
// ════════════════════════════════════════════════════════════
DOM.chatInput.addEventListener('input', () => {
  const len = DOM.chatInput.value.length;
  DOM.inputCounter.textContent = `${len} / 500`;
  DOM.inputCounter.style.color = len > 450 ? 'var(--crimson)' : '';
});

DOM.chatInput.addEventListener('keydown', e => {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); }
});

// ── Detective shortcut: prefix [D] to inject as detective turn ─
function injectPrompt(role) {
  DOM.chatInput.value = role === 'detective' ? '[D] ' : '';
  DOM.chatInput.focus();
}

// ════════════════════════════════════════════════════════════
//  SEND MESSAGE  (main entry point)
// ════════════════════════════════════════════════════════════
function sendMessage() {
  const raw = DOM.chatInput.value.trim();
  if (!raw || APP.isProcessing) return;

  DOM.chatInput.value = '';
  DOM.inputCounter.textContent = '0 / 500';

  // Detective-side messages (prefixed [D]) are displayed only, not sent to pipeline
  if (raw.startsWith('[D]')) {
    const text = raw.slice(3).trim();
    appendMsg('detective', 'Detective', text, null, 'node_01');
    return;
  }

  // Subject statement
  const stmtId = `stmt_${String(APP.statements + 1).padStart(3, '0')}`;
  appendMsg('subject', 'Harlow', raw, stmtId, null);
  APP.statements++;
  DOM.statStmt.textContent = APP.statements;
  bumpCounter(DOM.statStmt);
  APP.messageLog.push({ stmtId, text: raw, session: APP.currentSession, ts: new Date() });

  // Route to backend or local sim
  if (APP.backendOnline) {
    runBackendPipeline(raw, stmtId);
  } else {
    runLocalSimulation(raw, stmtId);
  }
}

// ════════════════════════════════════════════════════════════
//  BACKEND PIPELINE  (SSE stream)
// ════════════════════════════════════════════════════════════
function runBackendPipeline(text, stmtId) {
  APP.isProcessing = true;
  DOM.sendBtn.disabled = true;
  setStream('active', `cognee.remember() → writing ${stmtId} to graph namespace…`, 'remember');

  // ── Hard client-side timeout (90 s) ─────────────────────────────────
  // If the SSE stream hasn't sent a 'complete' event within 90 seconds
  // the UI unlocks itself so the user isn't stuck forever.
  const PIPELINE_TIMEOUT_MS = 90_000;
  let pipelineTimedOut = false;
  const pipelineTimeoutId = setTimeout(() => {
    pipelineTimedOut = true;
    console.warn('[CaseFile] Pipeline timeout — unlocking UI after 90 s.');
    showTyping(false);
    appendErrorBubble(
      'The pipeline is taking longer than expected (>90 s). ' +
      'The backend is still running — results will appear via the poller when ready.'
    );
    setStream('idle', 'Waiting for backend… results will sync via poller.', null);
    _unlockInput();
  }, PIPELINE_TIMEOUT_MS);

  // POST body — matches InterrogateRequest schema
  const body = JSON.stringify({
    input:        text,
    case_id:      CASE_ID,
    session_id:   APP.sessionId,
    subject_name: SUBJECT,
  });

  // Use fetch with ReadableStream to consume SSE from a POST body
  fetch(`${API_BASE}/api/interrogate/stream`, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json', 'Accept': 'text/event-stream' },
    body,
  })
  .then(res => {
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`);
    if (!res.body) throw new Error('No ReadableStream in response');
    return consumeSSEStream(res.body.getReader(), stmtId, text, pipelineTimeoutId, () => pipelineTimedOut);
  })
  .catch(err => {
    clearTimeout(pipelineTimeoutId);
    console.error('[CaseFile] SSE stream error:', err);
    appendErrorBubble(`Pipeline error: ${err.message}. Falling back to simulation.`);
    APP.backendOnline = false;
    showTyping(false);
    // Finish the turn with local sim so UI isn't stuck
    runLocalSimulation(text, stmtId);
  });
}

// ── SSE stream consumer ──────────────────────────────────────
async function consumeSSEStream(reader, stmtId, originalText, timeoutId, isTimedOut) {
  const decoder = new TextDecoder();
  let   buffer  = '';

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;

      // If the client timeout already fired, drain without updating UI
      if (isTimedOut && isTimedOut()) break;

      buffer += decoder.decode(value, { stream: true });

      // SSE frames are separated by \n\n
      const frames = buffer.split('\n\n');
      buffer = frames.pop();               // keep incomplete last chunk

      for (const frame of frames) {
        if (!frame.trim()) continue;
        parseSSEFrame(frame, stmtId, originalText, timeoutId);
      }
    }
  } finally {
    reader.cancel();
  }
}

// ── Parse one SSE frame and dispatch to UI handlers ──────────
function parseSSEFrame(frame, stmtId, originalText, timeoutId) {
  let eventName = 'message';
  let dataStr   = '';

  for (const line of frame.split('\n')) {
    if (line.startsWith('event:')) eventName = line.slice(6).trim();
    if (line.startsWith('data:'))  dataStr   = line.slice(5).trim();
  }

  let data;
  try { data = JSON.parse(dataStr); }
  catch { return; }

  switch (eventName) {
    case 'node_start':
      onNodeStart(data);
      break;
    case 'node_done':
      onNodeDone(data, stmtId, originalText);
      break;
    case 'contradiction':
      onContradiction(data, stmtId, originalText);
      break;
    case 'complete':
      clearTimeout(timeoutId);
      onPipelineComplete(data);
      break;
    case 'error':
      clearTimeout(timeoutId);
      appendErrorBubble(data.message || 'Unknown pipeline error.');
      onPipelineComplete({});
      break;
    default:
      break;
  }
}

// ── node_start handler ───────────────────────────────────────
function onNodeStart(data) {
  const opMap = { Detective: 'remember', Analyst: 'recall', ChiefOfPolice: 'improve' };
  const msgMap = {
    Detective:    'cognee.remember() → writing statement to graph namespace…',
    Analyst:      'cognee.recall() → GRAPH_COMPLETION_COT traversal active…',
    ChiefOfPolice:'cognee.recall() → compiling case report…',
  };
  setStream('active', msgMap[data.node] || data.message, opMap[data.node] || null);

  if (data.node === 'Detective') showTyping(true);
}

// ── node_done handler ────────────────────────────────────────
function onNodeDone(data, stmtId, originalText) {

  if (data.node === 'Detective') {
    showTyping(false);
    setStream('active', 'cognee.improve() → updating graph linkages & edge weights…', 'improve');

    if (data.detective_reply) {
      appendMsg('detective', 'Detective', data.detective_reply, null, 'node_01');
      // Tag the subject bubble if server confirms a contradiction exists
    }
    if (data.statement_id) {
      // Update stmtId badge retroactively if server assigned a different ID
      _updateLastStmtBadge(data.statement_id);
    }
  }

  if (data.node === 'Analyst') {
    setStream('active', 'cognee.recall() → cross-referencing complete…', 'recall');

    if (data.credibility_scores) {
      applyCreditScores(data.credibility_scores);
    }

    const note = data.has_contradiction
      ? `Graph traversal complete. Contradiction detected — credibility updated.`
      : `Graph traversal complete. No contradictions detected. Linkages stable.`;
    appendAnalystNote(note);

    // Increment graph nodes counter
    APP.graphNodes += Math.floor(Math.random() * 2) + 1;
    DOM.statNodes.textContent = APP.graphNodes;
    bumpCounter(DOM.statNodes);
  }

  if (data.node === 'ChiefOfPolice') {
    setStream('active', 'ChiefOfPolice node complete — case report compiled.', null);

    if (data.case_report_snippet) {
      updateReportSnippet(data.case_report_snippet);
    }
    if (data.graph_citations && data.graph_citations.length) {
      updateGraphCitations(data.graph_citations);
    }
  }
}

// ── contradiction handler ────────────────────────────────────
function onContradiction(data, stmtId, originalText) {
  APP.contradictions++;
  DOM.statContra.textContent = APP.contradictions;
  bumpCounter(DOM.statContra);

  // Contradiction Alert banner
  DOM.caCount.textContent       = `${APP.contradictions} flagged`;
  DOM.caSession.textContent     = `${APP.sessionId} — current`;
  DOM.caLabelA.textContent      = `Prior statement · graph memory`;
  DOM.caTextA.innerHTML         = escHtml(data.explanation || '');
  DOM.caLabelB.textContent      = `Current · ${stmtId}`;
  DOM.caTextB.innerHTML         = `"${escHtml(originalText.slice(0, 120))}${originalText.length > 120 ? '…' : ''}"`;
  DOM.caCredDelta.textContent   = data.credibility_delta
    ? `−${(data.credibility_delta * 100).toFixed(0)}%`
    : '—';
  DOM.caRecallMatch.textContent = data.recall_confidence
    ? `${(data.recall_confidence * 100).toFixed(1)}%`
    : '—';
  DOM.caEdge.textContent        = data.graph_edge || '—';

  DOM.caAlert.classList.add('active');
  DOM.caAlert.scrollIntoView({ behavior: 'smooth', block: 'nearest' });

  // Flag the last subject bubble as a contradiction
  _flagLastSubjectBubble();

  // Live feed entry
  const deltaLabel = data.credibility_delta
    ? `Δ −${(data.credibility_delta * 100).toFixed(0)}%`
    : '';
  addFeedItem(
    'contradiction',
    data.explanation || 'Contradiction detected by Analyst node.',
    stmtId,
    data.graph_edge,
    data.credibility_delta ? Math.round(data.credibility_delta * 100) : 0,
  );

  // Credibility update from server scores
  if (data.credibility_scores) {
    applyCreditScores(data.credibility_scores);
  }
}

// ── complete handler ─────────────────────────────────────────
function onPipelineComplete(data) {
  if (data.statement_count > APP.statements) {
    APP.statements = data.statement_count;
    DOM.statStmt.textContent = APP.statements;
  }
  if (data.session_count) {
    DOM.statSessions.textContent = data.session_count;
  }

  setStream('idle', `Idle · statement committed · Graph stable · ${APP.graphNodes} nodes`, null);
  updateSuspectsTable();
  _unlockInput();
}

// Centralised input unlock so both onPipelineComplete and the
// 90-second timeout use the exact same code path.
function _unlockInput() {
  showTyping(false);
  APP.isProcessing  = false;
  DOM.sendBtn.disabled = false;
}

// ════════════════════════════════════════════════════════════
//  APPLY CREDIBILITY SCORES  (server → UI)
// ════════════════════════════════════════════════════════════
/**
 * Accepts the credibility_scores dict from the server:
 *   { harlow: 42.0, voss: 67.0, … }
 * and syncs every credibility bar/score in the analyst panel
 * and suspects table.
 */
function applyCreditScores(scores) {
  if (scores.harlow !== undefined) {
    const pct = Math.max(5, Math.round(scores.harlow));
    if (pct !== APP.harlowCred) {
      const prev = APP.harlowCred;
      APP.harlowCred = pct;
      _animateCredBar('harlowBar', 'harlowScore', 'credHarlow', pct, prev);
      if (DOM.tableHarlow) {
        DOM.tableHarlow.textContent = pct + '%';
        DOM.tableHarlow.className = `ts-score ts-${pct < 40 ? 'red' : 'amber'}`;
      }
    }
  }
  if (scores.voss !== undefined) {
    const pct = Math.max(5, Math.round(scores.voss));
    if (pct !== APP.vossCred) {
      const prev = APP.vossCred;
      APP.vossCred = pct;
      _animateCredBar('vossBar', 'vossScore', 'credVoss', pct, prev);
      if (DOM.tableVoss) {
        DOM.tableVoss.textContent = pct + '%';
        DOM.tableVoss.className = `ts-score ts-${pct < 40 ? 'red' : 'amber'}`;
      }
    }
  }
}

function _animateCredBar(barId, scoreId, rowId, pct, prev) {
  const bar   = $(barId);
  const score = $(scoreId);
  if (bar)   bar.style.width = pct + '%';
  if (score) {
    score.textContent = pct + '%';
    score.className   = `cred-pct ${pct < 35 ? 'critical' : pct < 55 ? 'warning' : 'ok'}`;
  }

  // Only add a delta chip if the score actually dropped
  if (prev > pct) {
    const delta = prev - pct;
    addCredChip(rowId, delta);
  }
}

// ════════════════════════════════════════════════════════════
//  UPDATE CHIEF'S BRIEFING PANEL
// ════════════════════════════════════════════════════════════
function updateReportSnippet(snippet) {
  // Write the server-generated snippet into the investigative summary block
  const summaryEl = $('reportSummary');
  if (!summaryEl) return;

  const existing = summaryEl.querySelector('.summary-text');
  if (existing) {
    // Replace first paragraph with server content, keep recommendation block
    existing.textContent = snippet.replace(/^#.*\n?/gm, '').trim().slice(0, 400);
  }

  // Timestamp refresh
  const sfEls = document.querySelectorAll('.sf-mono');
  if (sfEls[1]) {
    sfEls[1].textContent = `Case #2024-ALPHA-7 · ${new Date().toISOString().slice(0,16).replace('T',' ')} UTC`;
  }
}

function updateGraphCitations(citations) {
  const list = $('citationList');
  if (!list || !citations.length) return;

  citations.forEach(cite => {
    // Don't add duplicates
    if (list.querySelector(`[data-cite="${CSS.escape(cite)}"]`)) return;

    const card = document.createElement('div');
    card.className = 'citation-card';
    card.setAttribute('data-cite', cite);

    // Parse "stmt_X ⇔ stmt_Y" style edge notation
    const parts   = cite.split(/\s*[⇔←→⊘]\s*/);
    const nodeA   = parts[0]?.trim() || cite;
    const nodeB   = parts[1]?.trim() || '—';
    const edgeSym = cite.includes('⇔') ? '⇔ CONFLICT'
                  : cite.includes('←') ? '← CONFIRMS'
                  : cite.includes('⊘') ? '⊘ PRUNED'
                  : '→';

    card.innerHTML = `
      <div class="cit-node-row">
        <span class="cit-node">${escHtml(nodeA)}</span>
        <span class="cit-edge-type cit-bidirectional">${escHtml(edgeSym)}</span>
        <span class="cit-node">${escHtml(nodeB)}</span>
      </div>
      <span class="cit-badge cit-contradiction">LIVE</span>
      <p class="cit-desc">Graph edge from Cognee memory · cognee.recall() · GRAPH_COMPLETION_COT</p>`;

    list.prepend(card);
  });
}

// ════════════════════════════════════════════════════════════
//  LOCAL SIMULATION FALLBACK
//  Runs when the backend is unreachable — keeps the demo alive.
// ════════════════════════════════════════════════════════════
function runLocalSimulation(text, stmtId) {
  APP.isProcessing = true;
  DOM.sendBtn.disabled = true;

  setStream('active', `cognee.remember() → writing ${stmtId} to graph namespace…`, 'remember');

  setTimeout(() => {
    setStream('active', 'cognee.improve() → updating graph linkages & edge weights…', 'improve');
    showTyping(true);
  }, 600);

  setTimeout(() => {
    showTyping(false);
    appendMsg('detective', 'Detective', _localDetectiveReply(text), null, 'node_01');
  }, 1800);

  setTimeout(() => {
    setStream('active', 'cognee.recall() → GRAPH_COMPLETION_COT traversal active…', 'recall');
    const found = _localContradictionCheck(text);

    if (found) {
      appendAnalystNote(found.note);

      // Simulate contradiction payload shape matching the server contract
      onContradiction({
        graph_edge:        found.alertData?.edge || 'stmt_current ⇔ history',
        explanation:       found.note,
        recall_confidence: parseFloat(found.alertData?.match) / 100 || 0.88,
        credibility_delta: found.delta / 100,
        credibility_scores: {
          harlow: Math.max(5, APP.harlowCred - found.delta),
          voss:   APP.vossCred,
        },
      }, stmtId, text);

    } else {
      appendAnalystNote('Graph traversal complete. No contradictions detected. Linkages stable.');
      addFeedItem('verified', `Statement ${stmtId} cross-referenced — no anomaly detected.`, stmtId, null, 0);
    }

    APP.graphNodes += Math.floor(Math.random() * 2) + 1;
    DOM.statNodes.textContent = APP.graphNodes;
    bumpCounter(DOM.statNodes);
  }, 2800);

  setTimeout(() => {
    onPipelineComplete({});
  }, 3600);
}

// ── Local detective replies (simulation only) ────────────────
const _DETECTIVE_POOL = [
  "That's a specific claim. Our cross-referenced records tell a different story. Elaborate.",
  "Interesting. Walk me through the exact sequence of events that evening, step by step.",
  "That's not consistent with what we have on file. Are you certain about that timeline?",
  "We have documentation suggesting otherwise. Reconsider your last answer.",
  "Who can independently corroborate that claim? Someone outside your immediate circle.",
  "The evidence graph is painting a different picture. I need specifics, not generalities.",
  "Noted. How does that reconcile with the records from March 12th we've already verified?",
  "You're aware we've cross-referenced all prior statements across every session, correct?",
  "Your credibility index is dropping, Mr. Harlow. Each inconsistency is logged permanently.",
  "Let's return to the evening of March 14th. Once more, from the beginning.",
];
let _replyIdx = 0;

function _localDetectiveReply(input) {
  const lower = input.toLowerCase();
  if (lower.includes('i was') || lower.includes('i went') || lower.includes('i am'))
    return "That's a specific claim. Our records from that period tell a different story. Can you elaborate?";
  if (lower.includes("don't know") || lower.includes('i forgot') || lower.includes('not sure'))
    return "Convenient. You seem to forget a great deal for someone with so little to hide.";
  if (lower.includes('promise') || lower.includes('swear') || lower.includes('truth'))
    return "Everyone in this room has promised the truth. The graph doesn't lie, Mr. Harlow.";
  if (lower.includes('lawyer') || lower.includes('attorney'))
    return "That's your right. Though it does rather confirm the direction this conversation is heading.";
  return _DETECTIVE_POOL[_replyIdx++ % _DETECTIVE_POOL.length];
}

// ── Local contradiction rules (simulation only) ──────────────
const _LOCAL_CONTRADICTIONS = [
  {
    keywords: ['alone', 'by myself', 'nobody', 'no one', 'just me'],
    note: 'Café Noireau seating record indicates table for 2. Companion presence implied in stmt_021. Flagging for verification via cognee.recall().',
    edge: 'stmt_021 ⇔ stmt_current', delta: 9,
    alertData: { session: 'Session 2 → Current', labelA: 'Session 2 · stmt_021',
      textA: '"Just needed some air." — table booked for 2.', labelB: 'Current', match: '91.4',
      edge: 'stmt_021 ⇔ stmt_current' },
  },
  {
    keywords: ['never', 'always', 'every time', 'all the time', 'not once'],
    note: 'Absolute qualifier detected. Running cognee.recall() sweep for counter-evidence nodes.',
    edge: 'stmt_current → history_scan', delta: 6, alertData: null,
  },
  {
    keywords: ['work', 'office', 'meeting', 'business', 'travel', 'chicago', 'trip', 'client'],
    note: 'Work claim conflicts with confirmed SFO→ORD flight, Mar-12 (stmt_013 ⇔ stmt_019). Edge weight: 0.88.',
    edge: 'stmt_013 ⇔ stmt_019', delta: 12,
    alertData: { session: 'Session 3 → Current', labelA: 'Session 3 · stmt_013',
      textA: '"I don\'t travel for work. I handle everything remotely."', labelB: 'Current', match: '88.3',
      edge: 'stmt_013 ⇔ stmt_current' },
  },
  {
    keywords: ['home', 'house', 'apartment', 'stayed', 'nowhere'],
    note: 'Location claim cross-referenced against Café Noireau receipt 20:47 Mar-14. Conflict with stmt_004.',
    edge: 'stmt_004 ⇔ stmt_current', delta: 18,
    alertData: { session: 'Session 1 → Current', labelA: 'Session 1 · stmt_004',
      textA: '"I was at home all evening. Never left the house."', labelB: 'Current', match: '96.1',
      edge: 'stmt_004 ⇔ stmt_current' },
  },
];

function _localContradictionCheck(text) {
  const lower = text.toLowerCase();
  for (const c of _LOCAL_CONTRADICTIONS) {
    if (c.keywords.some(k => lower.includes(k))) return c;
  }
  return null;
}

// ════════════════════════════════════════════════════════════
//  MESSAGE RENDERING
// ════════════════════════════════════════════════════════════
function appendMsg(type, author, text, stmtId, nodeTag) {
  const row  = document.createElement('div');
  row.className = `msg-row ${type}-row`;

  const now          = new Date().toTimeString().slice(0, 5);
  const avatarClass  = type === 'detective' ? 'detective-avatar' : 'subject-avatar';
  const avatarLetter = type === 'detective' ? 'D' : 'H';

  row.innerHTML = `
    <div class="msg-meta">
      <span class="msg-avatar ${avatarClass}">${avatarLetter}</span>
      <span class="msg-author">${escHtml(author)}</span>
      <span class="msg-time">${now}</span>
      ${nodeTag ? `<span class="msg-node">${escHtml(nodeTag)}</span>` : ''}
      ${stmtId ? `<span class="stmt-badge" id="badge-${escHtml(stmtId)}">${escHtml(stmtId)}</span>` : ''}
    </div>
    <div class="msg-bubble ${type}-bubble">${escHtml(text)}</div>`;

  DOM.chatHistory.appendChild(row);
  DOM.chatHistory.scrollTop = DOM.chatHistory.scrollHeight;
  return row;
}

function appendAnalystNote(text) {
  const row = document.createElement('div');
  row.className = 'msg-row analyst-row';
  row.innerHTML = `
    <div class="analyst-bubble">
      <span class="analyst-chip">Analyst Node · out-of-band</span>
      <span class="analyst-text">${escHtml(text)}</span>
    </div>`;
  DOM.chatHistory.appendChild(row);
  DOM.chatHistory.scrollTop = DOM.chatHistory.scrollHeight;
}

function appendErrorBubble(text) {
  const row = document.createElement('div');
  row.className = 'msg-row system-row';
  row.innerHTML = `
    <div class="msg-bubble system-bubble" style="border-color:var(--crimson-bdr);background:var(--crimson-bg);color:var(--crimson)">
      <span class="system-icon">⚠</span> ${escHtml(text)}
    </div>`;
  DOM.chatHistory.appendChild(row);
  DOM.chatHistory.scrollTop = DOM.chatHistory.scrollHeight;
}

function showSystemMsg(text) {
  const row = document.createElement('div');
  row.className = 'msg-row system-row';
  row.innerHTML = `
    <div class="msg-bubble system-bubble">
      <span class="system-icon">⬡</span> ${escHtml(text)}
    </div>`;
  DOM.chatHistory.appendChild(row);
  DOM.chatHistory.scrollTop = DOM.chatHistory.scrollHeight;
}

// Tag the most recent subject bubble as a contradiction
function _flagLastSubjectBubble() {
  const bubbles = DOM.chatHistory.querySelectorAll('.subject-bubble');
  const last    = bubbles[bubbles.length - 1];
  if (!last) return;
  last.classList.add('contradiction-bubble');

  // Also inject the ⚠ CONTRADICTION badge into its meta row
  const meta = last.previousElementSibling;
  if (meta && !meta.querySelector('.contradiction-badge')) {
    const badge = document.createElement('span');
    badge.className = 'contradiction-badge';
    badge.textContent = '⚠ CONTRADICTION';
    meta.appendChild(badge);
  }
}

// Update the stmt badge if the server assigned a different ID
function _updateLastStmtBadge(serverId) {
  const badges = DOM.chatHistory.querySelectorAll('.stmt-badge');
  const last   = badges[badges.length - 1];
  if (last && last.textContent !== serverId) {
    last.textContent = serverId;
    last.id = `badge-${serverId}`;
  }
}

// ════════════════════════════════════════════════════════════
//  STREAM BAR
// ════════════════════════════════════════════════════════════
function setStream(mode, text, activeOp) {
  DOM.streamText.textContent = text;

  if (mode === 'active') {
    DOM.streamDot.className = 'stream-dot active';
    DOM.streamBytes.textContent = `+${(Math.random() * 3 + 0.5).toFixed(1)} KB`;
  } else {
    DOM.streamDot.className = 'stream-dot';
    DOM.streamBytes.textContent = 'synced';
  }

  document.querySelectorAll('.op-chip').forEach(chip => {
    chip.classList.remove('active');
    if (activeOp && chip.classList.contains(`op-${activeOp}`)) {
      chip.classList.add('active');
    }
  });
}

function showTyping(show) {
  DOM.typingIndicator.style.display = show ? 'flex' : 'none';
}

// ════════════════════════════════════════════════════════════
//  CREDIBILITY HELPERS
// ════════════════════════════════════════════════════════════
function addCredChip(rowId, delta) {
  const row = $(rowId);
  if (!row) return;
  const chips = row.querySelector('.cred-events');
  if (!chips) return;
  const chip = document.createElement('span');
  chip.className = 'cred-event-chip red';
  chip.textContent = `▼ ${delta}% — stmt_${String(APP.statements).padStart(3,'0')}`;
  chip.style.animation = 'slide-down 0.3s ease';
  chips.appendChild(chip);
}

function updateSuspectsTable() {
  if (DOM.tableHarlow) DOM.tableHarlow.textContent = APP.harlowCred + '%';
  if (DOM.tableVoss)   DOM.tableVoss.textContent   = APP.vossCred   + '%';
}

// ════════════════════════════════════════════════════════════
//  LIVE FEED
// ════════════════════════════════════════════════════════════
function addFeedItem(type, text, stmtId, edge, delta) {
  const now       = new Date().toTimeString().slice(0, 5);
  const typeClass = `fact-${type}`;
  const iconClass = type === 'contradiction' ? 'fi-red' : type === 'verified' ? 'fi-green' : 'fi-grey';
  const icon      = type === 'contradiction' ? '⚡' : type === 'verified' ? '✓' : '◌';
  const tagClass  = `tag-${type}`;

  const edgeHtml = edge
    ? `<div class="fact-graph-ref">
         <span class="gref">${escHtml(stmtId)}</span>
         <span class="garrow">${type === 'contradiction' ? '⇔' : '→'}</span>
         <span class="gref">${escHtml(String(edge).split(/\s+/).at(-1))}</span>
         ${delta ? `<span class="gdelta red-delta">Δ −${delta}%</span>` : ''}
       </div>`
    : `<div class="fact-graph-ref">
         <span class="gref">${escHtml(stmtId)}</span>
         <span class="gwt">logged</span>
       </div>`;

  const item = document.createElement('div');
  item.className = `fact-item ${typeClass}`;
  item.setAttribute('data-type', type);
  item.style.animation = 'slide-up 0.25s ease';
  item.innerHTML = `
    <div class="fact-left">
      <span class="fact-icon-badge ${iconClass}">${icon}</span>
    </div>
    <div class="fact-body">
      <div class="fact-type-row">
        <span class="fact-tag ${tagClass}">${type.toUpperCase()}</span>
        <span class="fact-ts">${now}</span>
      </div>
      <p class="fact-text">${escHtml(String(text).slice(0, 80))}${String(text).length > 80 ? '…' : ''}</p>
      ${edgeHtml}
    </div>`;

  DOM.factFeed.prepend(item);
  applyFeedFilter();
}

function filterFeed(mode, el) {
  APP.filterMode = mode;
  document.querySelectorAll('.feed-filter').forEach(b => b.classList.remove('active-filter'));
  el.classList.add('active-filter');
  applyFeedFilter();
}

function applyFeedFilter() {
  document.querySelectorAll('.fact-item').forEach(item => {
    const t = item.getAttribute('data-type');
    item.style.display = (APP.filterMode === 'all' || t === APP.filterMode) ? '' : 'none';
  });
}

// ════════════════════════════════════════════════════════════
//  CONTRADICTION ALERT — dismiss
// ════════════════════════════════════════════════════════════
function dismissAlert() {
  DOM.caAlert.classList.remove('active');
}

// ════════════════════════════════════════════════════════════
//  SESSION TABS
// ════════════════════════════════════════════════════════════
function switchSession(btn, num) {
  document.querySelectorAll('.stab:not(.stab-add)').forEach(t => t.classList.remove('active'));
  btn.classList.add('active');
  APP.currentSession = num;
  APP.sessionId      = `s${num}`;
  appendDateDivider(`SESSION ${num} resumed`);
}

function addSession() {
  APP.sessions++;
  APP.currentSession = APP.sessions;
  APP.sessionId      = `s${APP.sessions}`;

  const tabs   = $('sessionTabs');
  const addBtn = tabs.querySelector('.stab-add');
  const newTab = document.createElement('button');
  newTab.className = 'stab';
  newTab.setAttribute('data-session', APP.sessions);
  newTab.onclick = function () { switchSession(this, APP.sessions); };
  newTab.innerHTML = `<span class="stab-dot grey"></span>Session ${APP.sessions}`;
  tabs.insertBefore(newTab, addBtn);
  switchSession(newTab, APP.sessions);

  DOM.statSessions.textContent = APP.sessions;
  bumpCounter(DOM.statSessions);
  appendDateDivider(`SESSION ${APP.sessions} opened · ${new Date().toLocaleDateString()}`);
}

function appendDateDivider(label) {
  const div = document.createElement('div');
  div.className = 'chat-date-divider';
  div.innerHTML = `<span>${escHtml(label)}</span>`;
  DOM.chatHistory.appendChild(div);
  DOM.chatHistory.scrollTop = DOM.chatHistory.scrollHeight;
}

// ════════════════════════════════════════════════════════════
//  REPORT PANEL
// ════════════════════════════════════════════════════════════
function toggleSec(id) {
  const body = $(`body-${id}`);
  const chev = $(`chev-${id}`);
  const open = !body.classList.contains('collapsed');
  body.classList.toggle('collapsed');
  chev.textContent  = open ? '▸' : '▾';
  chev.style.transform = open ? 'rotate(-90deg)' : 'rotate(0deg)';
}

async function refreshReport() {
  const btn = document.querySelector('.report-btn');
  btn.textContent = '↻ Regenerating…';
  btn.disabled    = true;

  if (APP.backendOnline) {
    try {
      const res = await fetch(`${API_BASE}/api/case/${CASE_ID}/report`);
      if (res.ok) {
        const data = await res.json();
        if (data.report) updateReportSnippet(data.report);
      }
    } catch { /* fall through */ }
  }

  setTimeout(() => {
    btn.textContent = '↻ Regenerate';
    btn.disabled    = false;
    const sfEls = document.querySelectorAll('.sf-mono');
    if (sfEls[1]) sfEls[1].textContent = `Case #2024-ALPHA-7 · ${new Date().toISOString().slice(0,16).replace('T',' ')} UTC`;
  }, 1200);
}

// ════════════════════════════════════════════════════════════
//  EXPORT
// ════════════════════════════════════════════════════════════
function exportReport() {
  const lines = [
    'THE CASE FILE — OFFICIAL INVESTIGATIVE CASE REPORT',
    '====================================================',
    `Case ID:      ${CASE_ID}`,
    `Generated:    ${new Date().toISOString()}`,
    `Statements:   ${APP.statements}`,
    `Sessions:     ${APP.sessions}`,
    `Contradictions: ${APP.contradictions}`,
    '',
    'SUSPECT CREDIBILITY',
    `Marcus Harlow  ${APP.harlowCred}%`,
    `Renata Voss    ${APP.vossCred}%`,
    `D. Chen        CLEARED`,
    '',
    'STATEMENT LOG',
    ...APP.messageLog.map(m =>
      `[${m.stmtId}] S${m.session} ${m.ts.toTimeString().slice(0,5)} — ${m.text}`
    ),
    '',
    'Generated by ChiefOfPolice · cognee.recall() + LangGraph',
  ];

  const blob = new Blob([lines.join('\n')], { type: 'text/plain' });
  const url  = URL.createObjectURL(blob);
  Object.assign(document.createElement('a'), {
    href: url, download: `case-report-${CASE_ID}.txt`,
  }).click();
  URL.revokeObjectURL(url);
}

// ════════════════════════════════════════════════════════════
//  LOCKDOWN
// ════════════════════════════════════════════════════════════
function toggleLockdown() {
  APP.lockdown = !APP.lockdown;
  const btn  = document.querySelector('.top-btn-danger');
  const pill = document.querySelector('.case-pill');
  if (APP.lockdown) {
    btn.textContent = '🔒 LOCKED';
    btn.style.cssText = 'background:var(--crimson);color:white;border-color:var(--crimson)';
    pill.innerHTML = `<span class="status-dot" style="background:var(--crimson)"></span>
      CASE #2024-ALPHA-7 &nbsp;·&nbsp; <span class="pill-tag">LOCKDOWN</span>`;
  } else {
    btn.textContent  = '⬡ Lockdown';
    btn.style.cssText = '';
    pill.innerHTML = `<span class="status-dot green"></span>
      CASE #2024-ALPHA-7 &nbsp;·&nbsp; <span class="pill-tag">ACTIVE INTERROGATION</span>`;
  }
}

// ════════════════════════════════════════════════════════════
//  GRAPH MODAL
// ════════════════════════════════════════════════════════════
function showGraphModal() {
  const modal = $('graphModal');
  modal.classList.toggle('open');
  if (modal.classList.contains('open')) drawGraph();
}
function closeGraphModal(e) {
  if (e.target === $('graphModal')) showGraphModal();
}

function drawGraph() {
  const canvas = $('graphCanvas');
  const ctx    = canvas.getContext('2d');
  const W = canvas.width, H = canvas.height;
  ctx.clearRect(0, 0, W, H);

  const nodes = [
    { id: 'stmt_004', x: 160, y: 120, color: '#DC2626', label: 'stmt_004', type: 'contradiction' },
    { id: 'stmt_017', x: 340, y: 80,  color: '#DC2626', label: 'stmt_017', type: 'contradiction' },
    { id: 'stmt_013', x: 500, y: 150, color: '#D97706', label: 'stmt_013', type: 'pending' },
    { id: 'stmt_019', x: 640, y: 100, color: '#D97706', label: 'stmt_019', type: 'pending' },
    { id: 'stmt_008', x: 260, y: 260, color: '#16A34A', label: 'stmt_008', type: 'verified' },
    { id: 'stmt_002', x: 100, y: 300, color: '#16A34A', label: 'stmt_002', type: 'verified' },
    { id: 'stmt_021', x: 430, y: 300, color: '#D97706', label: 'stmt_021', type: 'pending' },
    { id: 'stmt_003', x: 620, y: 310, color: '#94A3B8', label: 'stmt_003 ⊘', type: 'pruned' },
    { id: 'harlow',   x: 260, y: 160, color: '#1E3A5F', label: 'Harlow',    type: 'entity', r: 18 },
    { id: 'voss',     x: 520, y: 250, color: '#4C1D95', label: 'Voss',      type: 'entity', r: 16 },
    { id: 'nexar',    x: 390, y: 200, color: '#475569', label: 'Nexar Corp', type: 'entity', r: 15 },
    { id: 'stmt_005', x: 160, y: 360, color: '#D97706', label: 'stmt_005',  type: 'pending' },
  ];
  const edges = [
    { from: 'stmt_004', to: 'stmt_017', color: '#DC2626', dash: false },
    { from: 'stmt_013', to: 'stmt_019', color: '#D97706', dash: false },
    { from: 'stmt_008', to: 'stmt_002', color: '#16A34A', dash: false },
    { from: 'stmt_021', to: 'stmt_003', color: '#94A3B8', dash: true },
    { from: 'harlow',   to: 'stmt_004', color: '#1E3A5F', dash: false },
    { from: 'harlow',   to: 'stmt_017', color: '#1E3A5F', dash: false },
    { from: 'harlow',   to: 'stmt_008', color: '#1E3A5F', dash: false },
    { from: 'harlow',   to: 'nexar',    color: '#475569', dash: false },
    { from: 'voss',     to: 'nexar',    color: '#475569', dash: false },
    { from: 'voss',     to: 'stmt_013', color: '#4C1D95', dash: true },
    { from: 'nexar',    to: 'stmt_005', color: '#475569', dash: true },
  ];

  // Grid
  ctx.strokeStyle = 'rgba(226,232,240,0.6)'; ctx.lineWidth = 1;
  for (let x = 0; x < W; x += 40) { ctx.beginPath(); ctx.moveTo(x,0); ctx.lineTo(x,H); ctx.stroke(); }
  for (let y = 0; y < H; y += 40) { ctx.beginPath(); ctx.moveTo(0,y); ctx.lineTo(W,y); ctx.stroke(); }

  const nMap = Object.fromEntries(nodes.map(n => [n.id, n]));

  edges.forEach(e => {
    const a = nMap[e.from], b = nMap[e.to];
    if (!a || !b) return;
    ctx.beginPath(); ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y);
    ctx.strokeStyle = e.color + '80';
    ctx.lineWidth   = e.dash ? 1.5 : 2;
    ctx.setLineDash(e.dash ? [5,4] : []);
    ctx.stroke(); ctx.setLineDash([]);
  });

  nodes.forEach(n => {
    const r = n.r || 13;
    const g = ctx.createRadialGradient(n.x, n.y, r*0.3, n.x, n.y, r*2);
    g.addColorStop(0, n.color + '30'); g.addColorStop(1, 'transparent');
    ctx.fillStyle = g; ctx.beginPath(); ctx.arc(n.x, n.y, r*2, 0, Math.PI*2); ctx.fill();

    ctx.beginPath(); ctx.arc(n.x, n.y, r, 0, Math.PI*2);
    ctx.fillStyle   = n.type === 'entity' ? n.color : '#FFFFFF'; ctx.fill();
    ctx.strokeStyle = n.color;
    ctx.lineWidth   = n.type === 'pruned' ? 1 : 2;
    ctx.setLineDash(n.type === 'pruned' ? [4,3] : []); ctx.stroke(); ctx.setLineDash([]);

    ctx.fillStyle    = n.type === 'entity' ? '#FFFFFF' : n.color;
    ctx.font         = `${n.type === 'entity' ? 600 : 500} ${r < 16 ? 9 : 10}px 'Inter',sans-serif`;
    ctx.textAlign    = 'center'; ctx.textBaseline = 'middle';
    ctx.fillText(n.label, n.x, r >= 14 ? n.y : n.y + r + 9);
  });
}

// ════════════════════════════════════════════════════════════
//  BACKGROUND SIMULATION  (ambient activity regardless of mode)
// ════════════════════════════════════════════════════════════
function startLiveSimulation() {
  // Only fire ambient events when backend is NOT being used for them
  setTimeout(() => {
    if (APP.backendOnline) return;  // server will drive these naturally
    applyCreditScores({ voss: APP.vossCred - 4 });
    addFeedItem('contradiction',
      'Phone metadata shows Voss contact at 21:15 Mar-14 — conflicts with isolation claim',
      'voss_rec', 'voss_001 ⇔ stmt_021', 7);
    APP.contradictions++;
    DOM.statContra.textContent = APP.contradictions;
    bumpCounter(DOM.statContra);
    DOM.caCount.textContent = `${APP.contradictions} flagged`;
  }, 8000);

  setTimeout(() => {
    if (APP.backendOnline) return;
    APP.graphNodes += 3;
    DOM.statNodes.textContent = APP.graphNodes;
    bumpCounter(DOM.statNodes);
    addFeedItem('verified',
      'Nexar Corp badge access log — confirms Mar 10 visit (cross-ref stmt_008)',
      'nexar_log', null, 0);
  }, 14000);

  setTimeout(() => {
    if (APP.backendOnline) return;
    applyCreditScores({ harlow: APP.harlowCred - 7 });
    addFeedItem('contradiction',
      'Bank transaction at SFO airport lounge Mar-12 — contradicts remote-work claim',
      'bank_001', 'stmt_013 ⇔ bank_001', 7);
    APP.contradictions++;
    DOM.statContra.textContent = APP.contradictions;
    bumpCounter(DOM.statContra);
    DOM.caCount.textContent = `${APP.contradictions} flagged`;
  }, 20000);

  setTimeout(() => {
    if (APP.backendOnline) return;
    APP.graphNodes += 2;
    DOM.statNodes.textContent = APP.graphNodes;
  }, 28000);

  animateMemoryRing();
}

function animateMemoryRing() {
  let usage  = 71;
  const arc  = document.querySelector('.mem-arc');
  const pctEl = document.querySelector('.mem-pct');
  if (!arc || !pctEl) return;
  setInterval(() => {
    usage = Math.min(99, usage + (Math.random() > 0.6 ? 1 : 0));
    arc.setAttribute('stroke-dasharray', `${Math.round(usage / 100 * 88)} 88`);
    pctEl.textContent = usage + '%';
    if (usage > 85) {
      arc.setAttribute('stroke', '#DC2626');
      pctEl.style.color = 'var(--crimson)';
    }
  }, 3000);
}

// ════════════════════════════════════════════════════════════
//  UTILITIES
// ════════════════════════════════════════════════════════════
function bumpCounter(el) {
  if (!el) return;
  el.style.animation = 'none';
  el.offsetHeight;
  el.style.animation = 'counter-pop 0.3s ease';
}

function escHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function initGraphCanvas() {
  const canvas = $('graphCanvas');
  if (!canvas) return;
  new ResizeObserver(() => {
    const w = (canvas.parentElement?.clientWidth ?? 760) - 32;
    canvas.style.width = w + 'px';
  }).observe(canvas.parentElement || document.body);
}
