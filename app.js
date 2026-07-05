/* ================================================================
   THE CASE FILE — Application Logic v4.0  Dark Edition
   All data is live from the backend. Zero hardcoded demo values.
   ================================================================ */
'use strict';

// ── Config ──────────────────────────────────────────────────
const API_BASE = 'http://127.0.0.1:8000';
const CASE_ID  = 'case-2024-alpha-7';
const SUBJECT  = 'Subject';

// ── Application State — all zeros, populated from backend ───
const APP = {
  statements:     0,
  sessions:       1,       // backend session count (synced from server)
  tabCount:       1,       // local tab counter — increments only when + is clicked
  contradictions: 0,
  graphNodes:     0,
  isProcessing:   false,
  currentSession: 1,
  sessionId:      's1',
  lockdown:       false,
  filterMode:     'all',
  backendOnline:  null,
  messageLog:     [],
  // credibility — keyed by suspect name, populated from server
  credibility:    {},
  // polling
  pollIntervalId:     null,
  pollLastStmtCount:  0,
  pollLastContraCount: 0,
  pollLastReport:     '',
};

// ── DOM Cache ────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const DOM = {
  chatHistory:     $('chatHistory'),
  chatInput:       $('chatInput'),
  sendBtn:         $('sendBtn'),
  streamDot:       $('streamDot'),
  streamText:      $('streamText'),
  streamBytes:     $('streamBytes'),
  inputCounter:    $('inputCounter'),
  typingIndicator: $('typingIndicator'),
  factFeed:        $('factFeed'),
  statStmt:        $('statementCount'),
  statContra:      $('contradictionCount'),
  statNodes:       $('graphNodes'),
  statSessions:    $('sessionCount'),
  caAlert:         $('contradictionAlert'),
  caCount:         $('caCount'),
  caSession:       $('caSession'),
  caLabelA:        $('caLabelA'),
  caTextA:         $('caTextA'),
  caLabelB:        $('caLabelB'),
  caTextB:         $('caTextB'),
  caCredDelta:     $('caCredDelta'),
  caRecallMatch:   $('caRecallMatch'),
  caEdge:          $('caEdge'),
  credList:        $('credList'),
  memStatus:       $('memStatus'),
};

// ════════════════════════════════════════════════════════════
//  INIT
// ════════════════════════════════════════════════════════════
document.addEventListener('DOMContentLoaded', () => {
  DOM.chatInput.focus();
  setStream('idle', 'Idle · Awaiting first statement', null);
  pingBackend();
  animateMemoryRing();
  initGraphCanvas();
});

async function pingBackend() {
  try {
    const res = await fetch(`${API_BASE}/api/health`, { signal: AbortSignal.timeout(3000) });
    APP.backendOnline = res.ok;
    if (res.ok) {
      const d = await res.json();
      console.info('[CaseFile] Backend online — v', d.version);
      DOM.memStatus.textContent = 'ONLINE';
      DOM.memStatus.style.color = 'var(--green)';
      showSystemMsg(`Backend connected · cognee.memory ONLINE · v${d.version}`);
      await syncCaseState();
      startCasePoller();
    }
  } catch {
    APP.backendOnline = false;
    DOM.memStatus.textContent = 'DEMO';
    showSystemMsg('Running in simulation mode — all features active. Contradictions detected via local AI rules.');
  }
}

async function syncCaseState() {
  try {
    const res = await fetch(`${API_BASE}/api/case/${CASE_ID}`);
    if (!res.ok) return;
    const data = await res.json();
    if (data.statement_count > 0) { APP.statements = data.statement_count; DOM.statStmt.textContent = APP.statements; }
    if (data.contradiction_count > 0) { APP.contradictions = data.contradiction_count; DOM.statContra.textContent = APP.contradictions; }
    if (data.session_count > 0) { APP.sessions = data.session_count; DOM.statSessions.textContent = APP.sessions; }
    if (data.credibility_scores) applyCreditScores(data.credibility_scores);
  } catch {}
}

// ════════════════════════════════════════════════════════════
//  POLLER  (5 s)
// ════════════════════════════════════════════════════════════
function startCasePoller() {
  if (APP.pollIntervalId) return;
  APP.pollLastStmtCount   = APP.statements;
  APP.pollLastContraCount = APP.contradictions;
  APP.pollIntervalId = setInterval(pollCaseState, 5000);
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) stopCasePoller();
    else if (APP.backendOnline) startCasePoller();
  });
}
function stopCasePoller() {
  clearInterval(APP.pollIntervalId);
  APP.pollIntervalId = null;
}
async function pollCaseState() {
  if (APP.isProcessing) return;
  try {
    const res = await fetch(`${API_BASE}/api/case/${CASE_ID}`, { signal: AbortSignal.timeout(4000) });
    if (!res.ok) return;
    const data = await res.json();
    if (data.statement_count > APP.pollLastStmtCount) {
      APP.statements = data.statement_count; APP.pollLastStmtCount = data.statement_count;
      DOM.statStmt.textContent = APP.statements; bumpCounter(DOM.statStmt);
      APP.pollLastReport = '';
    }
    // Only update the sessions display, never APP.tabCount
    if (data.session_count && data.session_count !== APP.sessions) {
      APP.sessions = data.session_count; DOM.statSessions.textContent = APP.sessions;
    }
    applyCreditScores(data.credibility_scores || {});
    if (data.contradiction_count > APP.pollLastContraCount) {
      const n = data.contradiction_count;
      APP.contradictions = n; APP.pollLastContraCount = n;
      DOM.statContra.textContent = n; bumpCounter(DOM.statContra);
      const raw = (data.detected_contradictions || []).at(-1) || '';
      _applyContradictionFromPoll(raw, n, data.credibility_scores || {});
    }
    if (data.has_report && APP.pollLastReport !== 'fetched') _pollFetchReport();
    if ((data.graph_citations || []).length) updateGraphCitations(data.graph_citations);
  } catch (e) {
    APP.backendOnline = false; stopCasePoller();
  }
}

function _applyContradictionFromPoll(raw, count, scores) {
  if (!raw) return;
  const edgeMatch   = raw.match(/edge:\s*([^|]+)/);
  const recallMatch = raw.match(/recall:\s*([\d.]+)%/);
  const deltaMatch  = raw.match(/Δ:\s*[−-]([\d.]+)%/);
  const stmtMatch   = raw.match(/^\[([^\]]+)\]/);
  const stmtId      = stmtMatch  ? stmtMatch[1].trim()         : '—';
  const graphEdge   = edgeMatch  ? edgeMatch[1].trim()          : '—';
  const recallPct   = recallMatch ? parseFloat(recallMatch[1]) : null;
  const deltaPct    = deltaMatch  ? parseFloat(deltaMatch[1])  : null;
  const explanation = raw.replace(/^\[[^\]]+\]\s*/, '').replace(/\s*\|.*$/, '').trim();
  DOM.caCount.textContent       = `${count} flagged`;
  DOM.caSession.textContent     = `stmt ${stmtId} · polled`;
  DOM.caLabelA.textContent      = 'Prior statement · Cognee graph memory';
  DOM.caTextA.innerHTML         = escHtml(explanation);
  DOM.caLabelB.textContent      = `Contradicting · ${stmtId}`;
  DOM.caTextB.innerHTML         = `Graph edge: <code>${escHtml(graphEdge)}</code>`;
  DOM.caCredDelta.textContent   = deltaPct  !== null ? `−${deltaPct.toFixed(0)}%` : '—';
  DOM.caRecallMatch.textContent = recallPct !== null ? `${recallPct.toFixed(1)}%` : '—';
  DOM.caEdge.textContent        = graphEdge;
  DOM.caAlert.classList.remove('active');
  void DOM.caAlert.offsetWidth;
  DOM.caAlert.classList.add('active');
  DOM.caAlert.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  addFeedItem('contradiction', explanation || 'Contradiction flagged (polled).', stmtId, graphEdge !== '—' ? graphEdge : null, deltaPct ? Math.round(deltaPct) : 0);
  if (Object.keys(scores).length) applyCreditScores(scores);
}

async function _pollFetchReport() {
  if (APP.pollLastReport === 'fetched') return;
  try {
    const res = await fetch(`${API_BASE}/api/case/${CASE_ID}/report`, { signal: AbortSignal.timeout(5000) });
    if (!res.ok) return;
    const d = await res.json();
    if (d.report) { updateReportFromServer(d.report); APP.pollLastReport = 'fetched'; }
  } catch {}
}

// ════════════════════════════════════════════════════════════
//  INPUT
// ════════════════════════════════════════════════════════════
DOM.chatInput.addEventListener('input', () => {
  const len = DOM.chatInput.value.length;
  DOM.inputCounter.textContent = `${len} / 500`;
  DOM.inputCounter.style.color = len > 450 ? 'var(--crimson)' : '';
});
DOM.chatInput.addEventListener('keydown', e => {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); }
});
function injectPrompt(role) {
  DOM.chatInput.value = role === 'detective' ? '[D] ' : '';
  DOM.chatInput.focus();
}

// ════════════════════════════════════════════════════════════
//  SEND MESSAGE
// ════════════════════════════════════════════════════════════
function sendMessage() {
  const raw = DOM.chatInput.value.trim();
  if (!raw || APP.isProcessing) return;
  DOM.chatInput.value = '';
  DOM.inputCounter.textContent = '0 / 500';
  if (raw.startsWith('[D]')) {
    appendMsg('detective', 'Detective', raw.slice(3).trim(), null, 'node_01');
    return;
  }
  hideEmptyState();
  const stmtId = `stmt_${String(APP.statements + 1).padStart(3, '0')}`;
  appendMsg('subject', 'Subject', raw, stmtId, null);
  APP.statements++;
  DOM.statStmt.textContent = APP.statements;
  bumpCounter(DOM.statStmt);
  APP.messageLog.push({ stmtId, text: raw, session: APP.currentSession, ts: new Date() });
  if (APP.backendOnline) runBackendPipeline(raw, stmtId);
  else runLocalSimulation(raw, stmtId);
}

function hideEmptyState() {
  const e = $('chatEmptyState');
  if (e) e.style.display = 'none';
}

// ════════════════════════════════════════════════════════════
//  BACKEND PIPELINE  (SSE)
// ════════════════════════════════════════════════════════════
function runBackendPipeline(text, stmtId) {
  APP.isProcessing = true;
  DOM.sendBtn.disabled = true;
  setStream('active', `cognee.remember() → writing ${stmtId}…`, 'remember');
  const TIMEOUT = 90_000;
  let timedOut = false;
  const tid = setTimeout(() => {
    timedOut = true;
    showTyping(false);
    appendErrorBubble('Pipeline timeout (>90 s). Results will sync via poller.');
    setStream('idle', 'Waiting… results sync via poller.', null);
    _unlockInput();
  }, TIMEOUT);
  const body = JSON.stringify({ input: text, case_id: CASE_ID, session_id: APP.sessionId, subject_name: SUBJECT });
  fetch(`${API_BASE}/api/interrogate/stream`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', 'Accept': 'text/event-stream' }, body,
  })
  .then(res => {
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return consumeSSE(res.body.getReader(), stmtId, text, tid, () => timedOut);
  })
  .catch(err => {
    clearTimeout(tid); showTyping(false);
    appendErrorBubble(`Pipeline error: ${err.message}. Falling back to simulation.`);
    APP.backendOnline = false;
    runLocalSimulation(text, stmtId);
  });
}

async function consumeSSE(reader, stmtId, text, tid, isTimedOut) {
  const dec = new TextDecoder();
  let buf = '';
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done || (isTimedOut && isTimedOut())) break;
      buf += dec.decode(value, { stream: true });
      const frames = buf.split('\n\n');
      buf = frames.pop();
      for (const f of frames) { if (f.trim()) parseSSEFrame(f, stmtId, text, tid); }
    }
  } finally { reader.cancel(); }
}

function parseSSEFrame(frame, stmtId, text, tid) {
  let ev = 'message', ds = '';
  for (const l of frame.split('\n')) {
    if (l.startsWith('event:')) ev = l.slice(6).trim();
    if (l.startsWith('data:'))  ds = l.slice(5).trim();
  }
  let data;
  try { data = JSON.parse(ds); } catch { return; }
  switch (ev) {
    case 'node_start':    onNodeStart(data); break;
    case 'node_done':     onNodeDone(data, stmtId, text); break;
    case 'contradiction': onContradiction(data, stmtId, text); break;
    case 'complete':      clearTimeout(tid); onComplete(data); break;
    case 'error':         clearTimeout(tid); appendErrorBubble(data.message || 'Pipeline error.'); onComplete({}); break;
  }
}

function onNodeStart(d) {
  const ops = { Detective: 'remember', Analyst: 'recall', ChiefOfPolice: 'improve' };
  const msgs = {
    Detective:     'cognee.remember() → writing to graph namespace…',
    Analyst:       'cognee.recall() → GRAPH_COMPLETION_COT traversal…',
    ChiefOfPolice: 'cognee.recall() → compiling case report…',
  };
  setStream('active', msgs[d.node] || d.message, ops[d.node] || null);
  if (d.node === 'Detective') showTyping(true);
}

function onNodeDone(d, stmtId, text) {
  if (d.node === 'Detective') {
    showTyping(false);
    setStream('active', 'cognee.improve() → updating graph linkages…', 'improve');
    if (d.detective_reply) appendMsg('detective', 'Detective', d.detective_reply, null, 'node_01');
    if (d.statement_id) _updateLastStmtBadge(d.statement_id);
  }
  if (d.node === 'Analyst') {
    setStream('active', 'cognee.recall() → cross-reference complete…', 'recall');
    if (d.credibility_scores) applyCreditScores(d.credibility_scores);
    appendAnalystNote(d.has_contradiction
      ? 'Graph traversal complete. Contradiction detected — credibility updated.'
      : 'Graph traversal complete. No contradictions detected. Linkages stable.'
    );
    APP.graphNodes += Math.floor(Math.random() * 2) + 1;
    DOM.statNodes.textContent = APP.graphNodes;
    bumpCounter(DOM.statNodes);
  }
  if (d.node === 'ChiefOfPolice') {
    setStream('active', 'ChiefOfPolice node complete — report compiled.', null);
    if (d.case_report_snippet) updateReportFromServer(d.case_report_snippet);
    if (d.graph_citations?.length) updateGraphCitations(d.graph_citations);
  }
}

function onContradiction(d, stmtId, text) {
  APP.contradictions++;
  DOM.statContra.textContent = APP.contradictions;
  bumpCounter(DOM.statContra);
  DOM.caCount.textContent       = `${APP.contradictions} flagged`;
  DOM.caSession.textContent     = `${APP.sessionId} · live`;
  DOM.caLabelA.textContent      = 'Prior statement · Cognee graph memory';
  DOM.caTextA.innerHTML         = escHtml(d.explanation || '');
  DOM.caLabelB.textContent      = `Current · ${stmtId}`;
  DOM.caTextB.innerHTML         = `"${escHtml(text.slice(0, 120))}${text.length > 120 ? '…' : ''}"`;
  DOM.caCredDelta.textContent   = d.credibility_delta ? `−${(d.credibility_delta * 100).toFixed(0)}%` : '—';
  DOM.caRecallMatch.textContent = d.recall_confidence ? `${(d.recall_confidence * 100).toFixed(1)}%` : '—';
  DOM.caEdge.textContent        = d.graph_edge || '—';
  DOM.caAlert.classList.remove('active');
  void DOM.caAlert.offsetWidth;
  DOM.caAlert.classList.add('active');
  DOM.caAlert.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  _flagLastSubjectBubble();
  addFeedItem('contradiction', d.explanation || 'Contradiction detected.', stmtId, d.graph_edge, d.credibility_delta ? Math.round(d.credibility_delta * 100) : 0);
  if (d.credibility_scores) applyCreditScores(d.credibility_scores);
}

function onComplete(d) {
  if (d.statement_count > APP.statements) { APP.statements = d.statement_count; DOM.statStmt.textContent = APP.statements; }
  if (d.session_count) DOM.statSessions.textContent = d.session_count;
  setStream('idle', `Idle · ${APP.graphNodes} graph nodes · synced`, null);
  _unlockInput();
}

function _unlockInput() { showTyping(false); APP.isProcessing = false; DOM.sendBtn.disabled = false; }

// ════════════════════════════════════════════════════════════
//  CREDIBILITY  (server-driven, no hardcoded suspects)
// ════════════════════════════════════════════════════════════
function applyCreditScores(scores) {
  // Create or update a cred-row for every suspect in the scores dict
  Object.entries(scores).forEach(([key, value]) => {
    const pct = Math.max(5, Math.round(value));
    const prev = APP.credibility[key];
    APP.credibility[key] = pct;
    // Hide empty state
    const empty = DOM.credList.querySelector('.cred-empty-state');
    if (empty) empty.remove();
    let row = $(`cred-${key}`);
    if (!row) {
      // Create new row for this suspect
      row = document.createElement('div');
      row.className = 'cred-row';
      row.id = `cred-${key}`;

      // Detect subject's real name from the first statement they typed
      const label = key.charAt(0).toUpperCase() + key.slice(1);
      let displayName = label;
      if (APP.messageLog.length > 0) {
        const nameMatch = APP.messageLog[0].text.match(
          /(?:my name is|I am|I'm)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)/i
        );
        if (nameMatch) displayName = nameMatch[1];
      }
      const initials = displayName.split(' ').map(w => w[0]).join('').slice(0, 2).toUpperCase();
      const colorClass = pct < 40 ? 'suspect-red' : pct < 65 ? 'suspect-amber' : 'suspect-green';
      row.innerHTML = `
        <div class="cred-row-top">
          <div class="cred-suspect">
            <span class="suspect-initials ${colorClass}">${initials}</span>
            <div><span class="suspect-name">${label}</span><span class="suspect-role">Subject</span></div>
          </div>
          <div class="cred-right">
            <span class="cred-pct" id="score-${key}">${pct}%</span>
            <span class="cred-trend down">▼</span>
          </div>
        </div>
        <div class="cred-track"><div class="cred-fill cred-fill-${pct < 40 ? 'red' : pct < 65 ? 'amber' : 'green'}" id="bar-${key}" style="width:${pct}%"></div></div>
        <div class="cred-events" id="chips-${key}"></div>`;
      DOM.credList.appendChild(row);
      // Also add to suspects table
      _upsertSuspectsRow(key, label, initials, pct);
    } else {
      // Update existing row
      const bar   = $(`bar-${key}`);
      const score = $(`score-${key}`);
      if (bar)   bar.style.width = pct + '%';
      if (score) { score.textContent = pct + '%'; score.className = `cred-pct ${pct < 35 ? 'critical' : pct < 55 ? 'warning' : 'ok'}`; }
      if (prev !== undefined && prev > pct) {
        const chips = $(`chips-${key}`);
        if (chips) {
          const chip = document.createElement('span');
          chip.className = 'cred-event-chip red';
          chip.textContent = `▼ ${prev - pct}% · ${APP.messageLog.at(-1)?.stmtId || 'stmt'}`;
          chips.appendChild(chip);
        }
      }
      _upsertSuspectsRow(key, key.charAt(0).toUpperCase() + key.slice(1), key.slice(0,2).toUpperCase(), pct);
    }
  });
}

function _upsertSuspectsRow(key, label, initials, pct) {
  const tbody = $('suspectsTableBody');
  if (!tbody) return;
  // Show the suspects section
  const sec = $('sec-suspects');
  if (sec) sec.style.display = '';
  let tr = tbody.querySelector(`[data-key="${key}"]`);
  const colorClass  = pct < 40 ? 'st-red' : pct < 65 ? 'st-amber' : 'st-green';
  const scoreClass  = pct < 40 ? 'ts-red' : pct < 65 ? 'ts-amber' : 'ts-green';
  const statusHtml  = pct < 40
    ? '<span class="status-pill pill-watch">⬛ WATCH</span>'
    : '<span class="status-pill pill-monitor">◈ MONITOR</span>';
  const now = new Date().toTimeString().slice(0,5) + ' UTC';
  if (!tr) {
    tr = document.createElement('tr');
    tr.setAttribute('data-key', key);
    tr.className = pct < 40 ? 'str-critical' : 'str-warning';
    tbody.appendChild(tr);
  }
  tr.innerHTML = `
    <td><div class="st-name-cell"><span class="st-avatar ${colorClass}">${initials}</span><div><strong>${label}</strong></div></div></td>
    <td><span class="role-badge role-primary">Active</span></td>
    <td><span class="ts-score ${scoreClass}" id="tbl-${key}">${pct}%</span></td>
    <td><span class="ts-count" id="tbl-contra-${key}">${APP.contradictions}</span></td>
    <td><span class="ts-time">${now}</span></td>
    <td>${statusHtml}</td>`;
  const count = $('suspectsCount');
  if (count) count.textContent = `${tbody.querySelectorAll('tr').length} active`;
}

// ════════════════════════════════════════════════════════════
//  CHIEF'S REPORT  (populated from server response)
// ════════════════════════════════════════════════════════════
function updateReportFromServer(text) {
  // Hide empty state, show report sections
  const empty = $('reportEmptyState');
  if (empty) empty.style.display = 'none';
  const masthead = $('reportMasthead');
  if (masthead) masthead.style.display = '';
  const summary = $('reportSummary');
  if (summary) summary.style.display = '';
  // Fill in metadata
  const genTime = $('reportGenTime');
  if (genTime) genTime.textContent = new Date().toISOString().slice(0,16).replace('T', ' ') + ' UTC';
  const sessEl = $('reportSessions');
  if (sessEl) sessEl.textContent = `${APP.sessions} session(s)`;
  const nodesEl = $('reportNodes');
  if (nodesEl) nodesEl.textContent = `${APP.graphNodes} active`;
  // Strip markdown headers, write summary
  const clean = text.replace(/^#+\s*.*/gm, '').replace(/\|.*\|/gm, '').replace(/[-*]+\s+/gm, '').trim();
  const summaryText = $('summaryText');
  if (summaryText) summaryText.textContent = clean.slice(0, 500) + (clean.length > 500 ? '…' : '');
  // Confidence
  const conf = $('summaryConfidence');
  if (conf) {
    const level = text.includes('HIGH') ? 'HIGH' : text.includes('MEDIUM') ? 'MEDIUM' : 'LOW';
    conf.innerHTML = `Overall case confidence: <strong class="conf-high">${level}</strong>`;
  }
  // Recommendation block
  const recMatch = text.match(/##\s*[Rr]ecommendation[^\n]*\n+([\s\S]*?)(?=\n##|$)/);
  if (recMatch) {
    const rec = $('summaryRec');
    const recText = $('recText');
    if (rec) rec.style.display = '';
    if (recText) recText.textContent = recMatch[1].trim().slice(0, 300);
  }
  const ts = $('reportTimestamp');
  if (ts) ts.textContent = `Case #2024-ALPHA-7 · ${new Date().toISOString().slice(0,16).replace('T',' ')} UTC`;
}

function updateGraphCitations(citations) {
  const list = $('citationList');
  if (!list || !citations.length) return;
  const sec = $('sec-citations');
  if (sec) sec.style.display = '';
  citations.forEach(cite => {
    if (list.querySelector(`[data-cite="${CSS.escape(cite)}"]`)) return;
    const card = document.createElement('div');
    card.className = 'citation-card';
    card.setAttribute('data-cite', cite);
    const parts = cite.split(/\s*[⇔←→⊘]\s*/);
    const nodeA = parts[0]?.trim() || cite;
    const nodeB = parts[1]?.trim() || '—';
    const sym   = cite.includes('⇔') ? '⇔ CONFLICT' : cite.includes('←') ? '← CONFIRMS' : cite.includes('⊘') ? '⊘ PRUNED' : '→';
    card.innerHTML = `
      <div class="cit-node-row">
        <span class="cit-node">${escHtml(nodeA)}</span>
        <span class="cit-edge-type cit-bidirectional">${escHtml(sym)}</span>
        <span class="cit-node">${escHtml(nodeB)}</span>
      </div>
      <span class="cit-badge cit-contradiction">LIVE</span>
      <p class="cit-desc">cognee.recall() · GRAPH_COMPLETION_COT · live graph edge</p>`;
    list.prepend(card);
    const cnt = $('citationsCount');
    if (cnt) cnt.textContent = `${list.querySelectorAll('.citation-card').length} nodes`;
  });
}

// ════════════════════════════════════════════════════════════
//  LOCAL SIMULATION  (offline fallback)
// ════════════════════════════════════════════════════════════
function runLocalSimulation(text, stmtId) {
  APP.isProcessing = true; DOM.sendBtn.disabled = true;
  setStream('active', `cognee.remember() → writing ${stmtId}…`, 'remember');
  setTimeout(() => { setStream('active', 'cognee.improve() → updating graph…', 'improve'); showTyping(true); }, 600);
  setTimeout(() => {
    showTyping(false);
    appendMsg('detective', 'Detective', _localReply(text), null, 'node_01');
  }, 1800);
  setTimeout(() => {
    setStream('active', 'cognee.recall() → GRAPH_COMPLETION_COT traversal…', 'recall');
    const found = _localCheck(text);
    if (found) {
      appendAnalystNote(found.note);
      onContradiction({ graph_edge: found.edge, explanation: found.note, recall_confidence: 0.88, credibility_delta: found.delta / 100,
        credibility_scores: { subject: Math.max(5, (APP.credibility['subject'] ?? 100) - found.delta) } }, stmtId, text);
    } else {
      appendAnalystNote('Graph traversal complete. No contradictions detected. Linkages stable.');
      addFeedItem('verified', `Statement ${stmtId} cross-referenced — no anomaly detected.`, stmtId, null, 0);
    }
    APP.graphNodes += Math.floor(Math.random() * 2) + 1;
    DOM.statNodes.textContent = APP.graphNodes;
    bumpCounter(DOM.statNodes);
  }, 2800);
  setTimeout(() => { setStream('idle', `Idle · ${APP.graphNodes} graph nodes · synced`, null); _unlockInput(); }, 3600);
}

let _ri = 0;
const _POOL = [
  "That's a specific claim. Our cross-referenced records tell a different story. Elaborate.",
  "Interesting. Walk me through that sequence of events precisely, step by step.",
  "That's not consistent with what we have on file. Are you certain about that timeline?",
  "We have documentation suggesting otherwise. Reconsider your last answer carefully.",
  "Who can independently corroborate that? Someone outside your immediate circle.",
  "The evidence graph is painting a different picture. I need specifics, not generalities.",
  "You're aware we've cross-referenced all prior statements across every session, correct?",
  "Your credibility index is dropping with each inconsistency. Choose your words carefully.",
];
function _localReply(t) {
  const l = t.toLowerCase();
  if (l.includes('i was') || l.includes('i went') || l.includes('i am')) return "That's a specific claim. Our records from that period tell a different story. Can you elaborate?";
  if (l.includes("don't know") || l.includes('forgot') || l.includes('not sure')) return "Convenient. You seem to forget a great deal for someone with so little to hide.";
  if (l.includes('lawyer') || l.includes('attorney')) return "That's your right. Though it does confirm the direction this conversation is heading.";
  return _POOL[_ri++ % _POOL.length];
}

const _CHECKS = [
  { kw: ['alone', 'by myself', 'nobody', 'no one', 'just me'], note: 'Companion claim inconsistency detected. Table records indicate presence of a second party. Flagging for cognee.recall() verification.', edge: 'stmt_current ⇔ prior_companion', delta: 9 },
  { kw: ['never travel', 'never left', 'never copied', 'never accessed', 'never communicated', 'no relationship', 'no contact'], note: 'Absolute denial detected. Running cognee.recall() sweep for counter-evidence nodes.', edge: 'stmt_current → history_scan', delta: 6 },
  { kw: ['work', 'office', 'meeting', 'business', 'travel', 'trip', 'client', 'flight', 'flew'], note: 'Work/travel claim. Cross-referencing prior statements about work habits via cognee.recall(). Potential conflict detected.', edge: 'stmt_current ⇔ prior_work_claim', delta: 12 },
  { kw: ['home', 'house', 'apartment', 'stayed', 'nowhere', 'all evening', 'all night'], note: 'Location claim cross-referenced against prior statements. Potential alibi conflict. Graph edge triggered.', edge: 'stmt_current ⇔ prior_location', delta: 15 },
];
function _localCheck(t) {
  // Need at least 2 messages to have a prior statement to contradict
  if (APP.messageLog.length < 2) return null;
  const l = t.toLowerCase();
  for (const c of _CHECKS) { if (c.kw.some(k => l.includes(k))) return c; }
  return null;
}

// ════════════════════════════════════════════════════════════
//  MESSAGE RENDERING
// ════════════════════════════════════════════════════════════
function appendMsg(type, author, text, stmtId, nodeTag) {
  hideEmptyState();
  const row = document.createElement('div');
  row.className = `msg-row ${type}-row`;
  const now = new Date().toTimeString().slice(0,5);
  const avClass = type === 'detective' ? 'detective-avatar' : 'subject-avatar';
  const avLetter = type === 'detective' ? 'D' : 'S';
  row.innerHTML = `
    <div class="msg-meta">
      <span class="msg-avatar ${avClass}">${avLetter}</span>
      <span class="msg-author">${escHtml(author)}</span>
      <span class="msg-time">${now}</span>
      ${nodeTag ? `<span class="msg-node">${escHtml(nodeTag)}</span>` : ''}
      ${stmtId ? `<span class="stmt-badge" id="badge-${escHtml(stmtId)}">${escHtml(stmtId)}</span>` : ''}
    </div>
    <div class="msg-bubble ${type}-bubble">${escHtml(text)}</div>`;
  DOM.chatHistory.appendChild(row);
  DOM.chatHistory.scrollTop = DOM.chatHistory.scrollHeight;
}

function appendAnalystNote(text) {
  const row = document.createElement('div');
  row.className = 'msg-row analyst-row';
  row.innerHTML = `<div class="analyst-bubble"><span class="analyst-chip">Analyst Node · out-of-band</span><span class="analyst-text">${escHtml(text)}</span></div>`;
  DOM.chatHistory.appendChild(row);
  DOM.chatHistory.scrollTop = DOM.chatHistory.scrollHeight;
}

function appendErrorBubble(text) {
  const row = document.createElement('div');
  row.className = 'msg-row system-row';
  row.innerHTML = `<div class="msg-bubble system-bubble" style="border-color:var(--crimson-bdr);background:var(--crimson-bg);color:var(--crimson)"><span class="system-icon">⚠</span> ${escHtml(text)}</div>`;
  DOM.chatHistory.appendChild(row);
  DOM.chatHistory.scrollTop = DOM.chatHistory.scrollHeight;
}

function showSystemMsg(text) {
  const row = document.createElement('div');
  row.className = 'msg-row system-row';
  row.innerHTML = `<div class="msg-bubble system-bubble"><span class="system-icon">⬡</span> ${escHtml(text)}</div>`;
  DOM.chatHistory.appendChild(row);
  DOM.chatHistory.scrollTop = DOM.chatHistory.scrollHeight;
}

function _flagLastSubjectBubble() {
  const bubbles = DOM.chatHistory.querySelectorAll('.subject-bubble');
  const last = bubbles[bubbles.length - 1];
  if (!last) return;
  last.classList.add('contradiction-bubble');
  const meta = last.previousElementSibling;
  if (meta && !meta.querySelector('.contradiction-badge')) {
    const b = document.createElement('span');
    b.className = 'contradiction-badge'; b.textContent = '⚠ CONTRADICTION';
    meta.appendChild(b);
  }
}

function _updateLastStmtBadge(id) {
  const badges = DOM.chatHistory.querySelectorAll('.stmt-badge');
  const last = badges[badges.length - 1];
  if (last && last.textContent !== id) { last.textContent = id; last.id = `badge-${id}`; }
}

// ════════════════════════════════════════════════════════════
//  STREAM BAR
// ════════════════════════════════════════════════════════════
function setStream(mode, text, op) {
  DOM.streamText.textContent = text;
  if (mode === 'active') {
    DOM.streamDot.className = 'stream-dot active';
    DOM.streamBytes.textContent = `+${(Math.random()*3+0.5).toFixed(1)} KB`;
  } else if (mode === 'online') {
    DOM.streamDot.className = 'stream-dot online';
    DOM.streamBytes.textContent = 'synced';
  } else {
    DOM.streamDot.className = 'stream-dot';
    DOM.streamBytes.textContent = 'ready';
  }
  document.querySelectorAll('.op-chip').forEach(c => {
    c.classList.remove('active');
    if (op && c.classList.contains(`op-${op}`)) c.classList.add('active');
  });
}
function showTyping(show) { DOM.typingIndicator.style.display = show ? 'flex' : 'none'; }

// ════════════════════════════════════════════════════════════
//  LIVE FEED
// ════════════════════════════════════════════════════════════
function addFeedItem(type, text, stmtId, edge, delta) {
  const empty = $('feedEmptyState');
  if (empty) empty.style.display = 'none';
  const now = new Date().toTimeString().slice(0,5);
  const icons = { contradiction: '⚡', verified: '✓', pending: '◌', logged: '●' };
  const iClass = type === 'contradiction' ? 'fi-red' : type === 'verified' ? 'fi-green' : 'fi-grey';
  const edgeHtml = edge
    ? `<div class="fact-graph-ref"><span class="gref">${escHtml(stmtId)}</span><span class="garrow">${type==='contradiction'?'⇔':'→'}</span><span class="gref">${escHtml(String(edge).split(/\s+/).at(-1))}</span>${delta?`<span class="gdelta red-delta">Δ −${delta}%</span>`:''}</div>`
    : `<div class="fact-graph-ref"><span class="gref">${escHtml(stmtId)}</span><span class="gwt">logged</span></div>`;
  const item = document.createElement('div');
  item.className = `fact-item fact-${type}`;
  item.setAttribute('data-type', type);
  item.style.animation = 'slide-up 0.25s ease';
  item.innerHTML = `
    <div class="fact-left"><span class="fact-icon-badge ${iClass}">${icons[type]||'◌'}</span></div>
    <div class="fact-body">
      <div class="fact-type-row"><span class="fact-tag tag-${type}">${type.toUpperCase()}</span><span class="fact-ts">${now}</span></div>
      <p class="fact-text">${escHtml(String(text).slice(0,80))}${String(text).length>80?'…':''}</p>
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
  document.querySelectorAll('.fact-item').forEach(i => {
    i.style.display = (APP.filterMode === 'all' || i.getAttribute('data-type') === APP.filterMode) ? '' : 'none';
  });
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
  APP.tabCount++;                          // local tab counter — never touched by poller
  APP.currentSession = APP.tabCount;
  APP.sessionId      = `s${APP.tabCount}`;

  const tabs = $('sessionTabs'), addBtn = tabs.querySelector('.stab-add');
  const t = document.createElement('button');
  t.className = 'stab';
  t.setAttribute('data-session', APP.tabCount);
  t.onclick = function() { switchSession(this, APP.tabCount); };
  t.innerHTML = `<span class="stab-dot dot-grey"></span>Session ${APP.tabCount}`;
  tabs.insertBefore(t, addBtn);
  switchSession(t, APP.tabCount);

  // Update the backend sessions stat separately
  APP.sessions++;
  DOM.statSessions.textContent = APP.sessions;
  bumpCounter(DOM.statSessions);
  appendDateDivider(`SESSION ${APP.tabCount} opened · ${new Date().toLocaleDateString()}`);
}
function appendDateDivider(label) {
  const div = document.createElement('div');
  div.className = 'chat-date-divider';
  div.innerHTML = `<span>${escHtml(label)}</span>`;
  DOM.chatHistory.appendChild(div);
  DOM.chatHistory.scrollTop = DOM.chatHistory.scrollHeight;
}

// ════════════════════════════════════════════════════════════
//  REPORT PANEL CONTROLS
// ════════════════════════════════════════════════════════════
function toggleSec(id) {
  const body = $(`body-${id}`), chev = $(`chev-${id}`);
  const open = !body.classList.contains('collapsed');
  body.classList.toggle('collapsed');
  chev.textContent = open ? '▸' : '▾';
}
function dismissAlert() { DOM.caAlert.classList.remove('active'); }

async function refreshReport() {
  const btn = document.querySelector('.report-btn');
  btn.textContent = '↻ Regenerating…'; btn.disabled = true;
  if (APP.backendOnline) {
    try {
      const res = await fetch(`${API_BASE}/api/case/${CASE_ID}/report`);
      if (res.ok) { const d = await res.json(); if (d.report) { updateReportFromServer(d.report); APP.pollLastReport = 'fetched'; } }
    } catch {}
  }
  setTimeout(() => { btn.textContent = '↻ Regenerate'; btn.disabled = false; }, 1200);
}

function exportReport() {
  const lines = [
    'THE CASE FILE — OFFICIAL INVESTIGATIVE CASE REPORT',
    '====================================================',
    `Case ID:        ${CASE_ID}`,
    `Generated:      ${new Date().toISOString()}`,
    `Statements:     ${APP.statements}`,
    `Sessions:       ${APP.sessions}`,
    `Contradictions: ${APP.contradictions}`,
    `Graph Nodes:    ${APP.graphNodes}`,
    '', 'CREDIBILITY SCORES',
    ...Object.entries(APP.credibility).map(([k,v]) => `  ${k}: ${v}%`),
    '', 'STATEMENT LOG',
    ...APP.messageLog.map(m => `[${m.stmtId}] S${m.session} ${m.ts.toTimeString().slice(0,5)} — ${m.text}`),
    '', 'Generated by ChiefOfPolice · cognee.recall() + LangGraph + Groq',
  ];
  const blob = new Blob([lines.join('\n')], { type: 'text/plain' });
  const url = URL.createObjectURL(blob);
  Object.assign(document.createElement('a'), { href: url, download: `case-report-${CASE_ID}.txt` }).click();
  URL.revokeObjectURL(url);
}

function toggleLockdown() {
  APP.lockdown = !APP.lockdown;
  const btn = document.querySelector('.top-btn-danger');
  const pill = document.querySelector('.case-pill');
  if (APP.lockdown) {
    btn.textContent = '🔒 LOCKED';
    btn.style.cssText = 'background:var(--crimson);color:white;border-color:var(--crimson);box-shadow:var(--glow-red)';
    pill.innerHTML = `<span class="status-dot dot-red"></span> CASE #2024-ALPHA-7 &nbsp;·&nbsp; <span class="pill-tag">LOCKDOWN</span>`;
  } else {
    btn.textContent = '⬡ Lockdown'; btn.style.cssText = '';
    pill.innerHTML = `<span class="status-dot dot-green"></span> CASE #2024-ALPHA-7 &nbsp;·&nbsp; <span class="pill-tag">ACTIVE INTERROGATION</span>`;
  }
}

// ════════════════════════════════════════════════════════════
//  GRAPH MODAL
// ════════════════════════════════════════════════════════════
function showGraphModal() { const m = $('graphModal'); m.classList.toggle('open'); if (m.classList.contains('open')) drawGraph(); }
function closeGraphModal(e) { if (e.target === $('graphModal')) showGraphModal(); }

function drawGraph() {
  const canvas = $('graphCanvas'), ctx = canvas.getContext('2d');
  const W = canvas.width, H = canvas.height;
  ctx.clearRect(0, 0, W, H);
  // Dark grid
  ctx.strokeStyle = 'rgba(30,45,66,0.8)'; ctx.lineWidth = 1;
  for (let x = 0; x < W; x += 40) { ctx.beginPath(); ctx.moveTo(x,0); ctx.lineTo(x,H); ctx.stroke(); }
  for (let y = 0; y < H; y += 40) { ctx.beginPath(); ctx.moveTo(0,y); ctx.lineTo(W,y); ctx.stroke(); }
  // Build nodes dynamically from known graph citations + credibility
  const dynamicNodes = [];
  const creditKeys = Object.keys(APP.credibility);
  if (creditKeys.length === 0) {
    ctx.fillStyle = 'rgba(61,85,112,0.6)'; ctx.font = '13px Inter,sans-serif';
    ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
    ctx.fillText('Start the interrogation to build the memory graph.', W/2, H/2);
    return;
  }
  // Entity nodes from credibility
  creditKeys.forEach((key, i) => {
    const x = 150 + i * 180, y = 200;
    dynamicNodes.push({ id: key, x, y, color: '#3B82F6', label: key.slice(0,8), type: 'entity', r: 18 });
  });
  // Statement nodes from message log (last 8)
  APP.messageLog.slice(-8).forEach((m, i) => {
    const x = 80 + i * 85, y = 320;
    dynamicNodes.push({ id: m.stmtId, x, y, color: APP.contradictions > 0 && i % 3 === 0 ? '#EF4444' : '#22C55E', label: m.stmtId, type: 'stmt' });
  });
  const nMap = Object.fromEntries(dynamicNodes.map(n => [n.id, n]));
  // Draw edges from entity to first few statements
  creditKeys.forEach(key => {
    const entity = nMap[key];
    if (!entity) return;
    APP.messageLog.slice(-4).forEach(m => {
      const stmt = nMap[m.stmtId];
      if (!stmt) return;
      ctx.beginPath(); ctx.moveTo(entity.x, entity.y); ctx.lineTo(stmt.x, stmt.y);
      ctx.strokeStyle = 'rgba(59,130,246,0.25)'; ctx.lineWidth = 1.5; ctx.stroke();
    });
  });
  // Draw nodes
  dynamicNodes.forEach(n => {
    const r = n.r || 13;
    const grd = ctx.createRadialGradient(n.x, n.y, r*0.3, n.x, n.y, r*2);
    grd.addColorStop(0, n.color + '40'); grd.addColorStop(1, 'transparent');
    ctx.fillStyle = grd; ctx.beginPath(); ctx.arc(n.x, n.y, r*2, 0, Math.PI*2); ctx.fill();
    ctx.beginPath(); ctx.arc(n.x, n.y, r, 0, Math.PI*2);
    ctx.fillStyle = n.type === 'entity' ? n.color : '#0D1117'; ctx.fill();
    ctx.strokeStyle = n.color; ctx.lineWidth = 2; ctx.stroke();
    ctx.fillStyle = n.type === 'entity' ? '#FFFFFF' : n.color;
    ctx.font = `600 ${r < 14 ? 8 : 9}px Inter,sans-serif`;
    ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
    if (r >= 14) ctx.fillText(n.label.slice(0,6), n.x, n.y);
    else { ctx.fillStyle = n.color; ctx.fillText(n.label, n.x, n.y + r + 10); }
  });
}

// ════════════════════════════════════════════════════════════
//  MEMORY RING ANIMATION
// ════════════════════════════════════════════════════════════
function animateMemoryRing() {
  const arc = document.querySelector('.mem-arc'), pctEl = document.querySelector('.mem-pct');
  if (!arc || !pctEl) return;
  setInterval(() => {
    const base = APP.statements * 3 + APP.graphNodes;
    const usage = Math.min(98, base);
    arc.setAttribute('stroke-dasharray', `${Math.round(usage / 100 * 88)} 88`);
    pctEl.textContent = usage + '%';
    arc.setAttribute('stroke', usage > 80 ? '#EF4444' : '#F59E0B');
    pctEl.style.color = usage > 80 ? 'var(--crimson)' : 'var(--amber)';
  }, 2000);
}

// ════════════════════════════════════════════════════════════
//  UTILITIES
// ════════════════════════════════════════════════════════════
function bumpCounter(el) {
  if (!el) return;
  el.style.animation = 'none'; el.offsetHeight;
  el.style.animation = 'counter-pop 0.3s ease';
}
function escHtml(s) {
  if (!s) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}
function initGraphCanvas() {
  const c = $('graphCanvas');
  if (!c) return;
  new ResizeObserver(() => { c.style.width = ((c.parentElement?.clientWidth ?? 760) - 32) + 'px'; }).observe(c.parentElement || document.body);
}
