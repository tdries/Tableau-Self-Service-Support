const http         = require('http');
const fs           = require('fs');
const path         = require('path');
const https        = require('https');
const { execFile } = require('child_process');

// Load .env manually (no external deps)
function loadEnv() {
  try {
    const raw = fs.readFileSync(path.join(__dirname, '.env'), 'utf8');
    return Object.fromEntries(
      raw.split('\n')
        .filter(l => l.trim() && !l.startsWith('#'))
        .map(l => { const i = l.indexOf('='); const v = l.slice(i+1).trim(); return [l.slice(0,i).trim(), v.replace(/^["']|["']$/g, '')]; })
    );
  } catch { return {}; }
}

const env          = loadEnv();
const e            = k => env[k] || process.env[k] || '';
const GITHUB_TOKEN = e('api') || e('GITHUB_TOKEN') || e('GH_TOKEN');
const GIPHY_KEY    = e('GIPHY_API_KEY');
const SMEE_URL     = e('SMEE_URL');
const REPO         = 'tdries/Tableau-Self-Service-Support';
const PORT         = parseInt(process.env.PORT || env.PORT || '8766');
const JIRA_TOKEN   = e('TAB_SUPPORT_AI_FULL') || e('TAB_SUPPORT_AI');
const JIRA_EMAIL   = e('JIRA_EMAIL');
const JIRA_HOST    = 'biztory.atlassian.net';
const JIRA_PROJECT = 'BTSA'; // locked — issues may only ever be created on this board

// ---- Request log (persistent JSON + XLS) ----
const XLSX     = require('xlsx');
const LOG_FILE = path.join(__dirname, 'request-log.json');
const XLS_FILE = path.join(__dirname, 'request-log.xlsx');

function readLog() {
  try { return JSON.parse(fs.readFileSync(LOG_FILE, 'utf8')); }
  catch { return []; }
}

function writeLog(entries) {
  fs.writeFileSync(LOG_FILE, JSON.stringify(entries, null, 2));
  syncXls(entries);
}

function syncXls(entries) {
  const rows = entries.map(e => ({
    'Issue ID':       e.issueId,
    'Summary':        e.summary,
    'Category':       e.category,
    'Destination':    e.destination,
    'Tableau Site':   e.tableauSite,
    'Workbook':       e.workbook,
    'Timestamp':      e.timestamp,
    'Fix Succeeded':  e.fixSucceeded === null ? '' : e.fixSucceeded ? 'Yes' : 'No',
    'Accepted':       e.accepted === null ? '' : e.accepted ? 'Accepted' : 'Restored'
  }));
  const ws = XLSX.utils.json_to_sheet(rows);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Request Log');
  XLSX.writeFile(wb, XLS_FILE);
}

function logRequest(entry) {
  const entries = readLog();
  entries.push(entry);
  writeLog(entries);
  return entry;
}

function updateLog(issueId, updates) {
  const entries = readLog();
  const entry = entries.find(e => e.issueId === issueId);
  if (entry) {
    Object.assign(entry, updates);
    writeLog(entries);
  }
  return entry;
}

// ---- smee.io webhook tunnel (forwards GitHub webhooks to localhost) ----
if (SMEE_URL) {
  const SmeeClient = require('smee-client');
  const smee = new SmeeClient({ source: SMEE_URL, target: `http://localhost:${PORT}/webhook/github`, logger: console });
  smee.start();
  console.log(`\n✓ Smee tunnel active: ${SMEE_URL} → /webhook/github`);
}

if (!GITHUB_TOKEN) {
  console.error('[ERROR] No GitHub token found in .env (expected key: api)');
  process.exit(1);
}

// ---- Agent trigger / restore queues ----
const pendingTriggers     = [];
const pendingRestores     = [];
const pendingJiraTriggers = [];

// ---- SSE progress hub ----
// Clients: issueNumber -> Set<res>
// Buffer:  issueNumber -> Array<event>  (so late subscribers catch up)
const sseClients = new Map();
const sseBuffer  = new Map();

function sseSubscribe(issueNumber, res) {
  res.writeHead(200, {
    'Content-Type':       'text/event-stream',
    'Cache-Control':      'no-cache',
    'Connection':         'keep-alive',
    'X-Accel-Buffering':  'no',
    'Access-Control-Allow-Origin': '*'
  });
  res.write(':\n\n');

  // Replay buffered events immediately so late subscribers catch up
  for (const event of (sseBuffer.get(String(issueNumber)) || [])) {
    try { res.write(`data: ${JSON.stringify(event)}\n\n`); } catch {}
  }

  if (!sseClients.has(issueNumber)) sseClients.set(issueNumber, new Set());
  sseClients.get(issueNumber).add(res);

  res.on('close', () => {
    const set = sseClients.get(issueNumber);
    if (set) { set.delete(res); if (!set.size) sseClients.delete(issueNumber); }
  });
}

function ssePush(issueNumber, payload) {
  const key = String(issueNumber);

  // Always buffer so late subscribers see the full history
  if (!sseBuffer.has(key)) sseBuffer.set(key, []);
  sseBuffer.get(key).push(payload);

  const set = sseClients.get(key);
  if (set && set.size) {
    const data = `data: ${JSON.stringify(payload)}\n\n`;
    for (const res of set) {
      try { res.write(data); } catch {}
    }
  }

  if (payload.pct >= 100) {
    setTimeout(() => {
      for (const res of (sseClients.get(key) || [])) {
        try { res.end(); } catch {}
      }
      sseClients.delete(key);
      sseBuffer.delete(key);
    }, 2000);
  }
}

// ---- Jira helpers ----
function jiraRequest(method, path, body, callback) {
  const auth = Buffer.from(`${JIRA_EMAIL}:${JIRA_TOKEN}`).toString('base64');
  const bodyStr = body ? JSON.stringify(body) : null;
  const req = https.request({
    hostname: JIRA_HOST,
    path,
    method,
    headers: {
      'Authorization':  `Basic ${auth}`,
      'Content-Type':   'application/json',
      'Accept':         'application/json',
      ...(bodyStr ? { 'Content-Length': Buffer.byteLength(bodyStr) } : {})
    }
  }, (res) => {
    let data = '';
    res.on('data', chunk => data += chunk);
    res.on('end', () => {
      try {
        const parsed = JSON.parse(data);
        if (res.statusCode >= 400) console.error(`[Jira] ${res.statusCode}:`, JSON.stringify(parsed));
        callback(null, res.statusCode, parsed);
      } catch (e) { callback(e); }
    });
  });
  req.on('error', err => callback(err));
  if (bodyStr) req.write(bodyStr);
  req.end();
}

// ---- Jira Issues proxy ----
function createJiraIssue(payload, callback) {
  const { title, description } = payload;
  if (JIRA_PROJECT !== 'BTSA') return callback(new Error('Jira writes are locked to project BTSA only'));
  jiraRequest('POST', '/rest/api/3/issue', {
    fields: {
      project:     { key: 'BTSA' },
      summary:     title,
      description: { type: 'doc', version: 1, content: [{ type: 'paragraph', content: [{ type: 'text', text: description }] }] },
      issuetype:   { name: 'Submit a request or incident' }
    }
  }, callback);
}

// ---- GitHub Issues proxy ----
function createIssue(payload, callback) {
  const body = JSON.stringify(payload);
  const req = https.request({
    hostname: 'api.github.com',
    path: `/repos/${REPO}/issues`,
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${GITHUB_TOKEN}`,
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
      'Accept': 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
      'User-Agent': 'Tableau-Self-Service-Support/1.0'
    }
  }, (res) => {
    let data = '';
    res.on('data', chunk => data += chunk);
    res.on('end', () => callback(null, res.statusCode, JSON.parse(data)));
  });
  req.on('error', err => callback(err));
  req.write(body);
  req.end();
}

// ---- Static file server ----
const MIME = {
  '.html': 'text/html',
  '.js':   'application/javascript',
  '.css':  'text/css',
  '.png':  'image/png',
  '.ico':  'image/x-icon',
  '.webp': 'image/webp'
};

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }

  const url = new URL(req.url, `http://localhost`);

  // --- POST /webhook/github  (GitHub Issues webhook) ---
  if (req.method === 'POST' && url.pathname === '/webhook/github') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      res.writeHead(204); res.end();
      try {
        const event = req.headers['x-github-event'];
        const payload = JSON.parse(body);
        if (event === 'issues' && payload.action === 'opened' && payload.issue?.number) {
          const issueNumber = payload.issue.number;
          console.log(`[Webhook] Issue #${issueNumber} opened — queuing for agent`);
          ssePush(issueNumber, { step: 'GitHub webhook received', pct: 8, type: 'info' });
          pendingTriggers.push({ issueNumber });
          ssePush(issueNumber, { step: 'Agent triggered', pct: 10, type: 'info' });
        }
      } catch {}
    });
    return;
  }

  // --- GET /api/progress/:issueNumber  (SSE) ---
  if (req.method === 'GET' && url.pathname.startsWith('/api/progress/')) {
    const issueNumber = url.pathname.split('/').pop();
    return sseSubscribe(issueNumber, res);
  }

  // --- POST /api/progress/:issueNumber  (agent → server → SSE clients) ---
  if (req.method === 'POST' && url.pathname.startsWith('/api/progress/')) {
    const issueNumber = url.pathname.split('/').pop();
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      try {
        ssePush(issueNumber, JSON.parse(body));
        res.writeHead(204); res.end();
      } catch {
        res.writeHead(400); res.end();
      }
    });
    return;
  }

  // --- GET /api/find-workbook?name=WorkbookName ---
  if (req.method === 'GET' && url.pathname === '/api/find-workbook') {
    const name = url.searchParams.get('name');
    if (!name) { res.writeHead(400); return res.end(JSON.stringify({ error: 'name required' })); }

    const candidates = [`${name}.twbx`, `${name}.twb`];
    let found = null;
    let remaining = candidates.length;
    const done = () => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ path: found }));
    };
    for (const filename of candidates) {
      execFile('mdfind', ['-name', filename, '-onlyin', __dirname], (err, stdout) => {
        if (!found && !err) {
          const match = stdout.trim().split('\n').filter(l => l.startsWith('/'))
            .find(l => !l.includes('.app/') && !l.includes('/Library/Caches'));
          if (match) found = match;
        }
        if (--remaining === 0) done();
      });
    }
    return;
  }

  // --- POST /api/submit-issue ---
  if (req.method === 'POST' && url.pathname === '/api/submit-issue') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      let parsed;
      try { parsed = JSON.parse(body); } catch {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ error: 'Invalid JSON' }));
      }

      const { title, category, description, context, workbookName, destination, tableauSite } = parsed;
      if (!title || !description) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ error: 'title and description are required' }));
      }

      const workbookLine = workbookName ? `\n\n**Workbook:** ${workbookName}` : '';
      const fullDescription = `${description}${workbookLine}\n\n---\n_Submitted via Tableau Self-Service Support extension_\n_${context || 'No Tableau context'}_`;

      if (destination === 'jira') {
        createJiraIssue({ title, description: fullDescription, category }, (err, status, data) => {
          if (err) { res.writeHead(500); return res.end(JSON.stringify({ error: err.message })); }
          if (status >= 400) {
            const msg = (data.errorMessages && data.errorMessages[0]) || JSON.stringify(data.errors || data);
            res.writeHead(status, { 'Content-Type': 'application/json' });
            return res.end(JSON.stringify({ error: msg }));
          }
          const issueKey = data.key;
          const issueUrl = `https://${JIRA_HOST}/browse/${issueKey}`;
          res.writeHead(201, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ key: issueKey, html_url: issueUrl }));

          ssePush(issueKey, { step: `Issue ${issueKey} logged in Jira`, pct: 5, type: 'ok' });
          pendingJiraTriggers.push({ issueKey, tableauSite: tableauSite || 'biztorypulse' });
          logRequest({ issueId: issueKey, summary: title, category: category || '', destination: 'jira', tableauSite: tableauSite || 'biztorypulse', workbook: workbookName || '', timestamp: new Date().toISOString(), fixSucceeded: null, accepted: null });
        });
        return;
      }

      createIssue({ title, body: fullDescription, labels: category ? [category] : [] }, (err, status, data) => {
        if (err) { res.writeHead(500); return res.end(JSON.stringify({ error: err.message })); }
        res.writeHead(status, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(data));

        if (data.number) {
          ssePush(data.number, { step: `Issue #${data.number} logged in GitHub`, pct: 5, type: 'ok' });
          pendingTriggers.push({ issueNumber: data.number, tableauSite: tableauSite || 'biztorypulse' });
          logRequest({ issueId: String(data.number), summary: title, category: category || '', destination: 'github', tableauSite: tableauSite || 'biztorypulse', workbook: workbookName || '', timestamp: new Date().toISOString(), fixSucceeded: null, accepted: null });
        }
      });
    });
    return;
  }

  // --- GET /api/next-trigger  (agent polls this) ---
  if (req.method === 'GET' && url.pathname === '/api/next-trigger') {
    const trigger = pendingTriggers.shift();
    if (trigger) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify(trigger));
    }
    res.writeHead(204); return res.end();
  }

  // --- GET /api/next-jira-trigger  (agent polls this) ---
  if (req.method === 'GET' && url.pathname === '/api/next-jira-trigger') {
    const trigger = pendingJiraTriggers.shift();
    if (trigger) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify(trigger));
    }
    res.writeHead(204); return res.end();
  }

  // --- GET /api/gif?q=...  (Giphy search proxy) ---
  if (req.method === 'GET' && url.pathname === '/api/gif') {
    const q = url.searchParams.get('q') || 'loading';
    if (!GIPHY_KEY) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ url: null }));
    }
    const giphyPath = `/v1/gifs/search?api_key=${GIPHY_KEY}&q=${encodeURIComponent(q)}&limit=10&rating=g&lang=en`;
    const giphyReq = https.request({ hostname: 'api.giphy.com', path: giphyPath }, (giphyRes) => {
      let data = '';
      giphyRes.on('data', c => data += c);
      giphyRes.on('end', () => {
        try {
          const results = JSON.parse(data).data || [];
          if (!results.length) { res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify({ url: null })); }
          const pick = results[Math.floor(Math.random() * results.length)];
          const gifUrl = pick.images?.fixed_height?.url || pick.images?.original?.url;
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ url: gifUrl }));
        } catch { res.writeHead(500); res.end(); }
      });
    });
    giphyReq.on('error', () => { res.writeHead(500); res.end(); });
    giphyReq.end();
    return;
  }

  // --- GET /api/next-restore  (agent polls this) ---
  if (req.method === 'GET' && url.pathname === '/api/next-restore') {
    const issueNumber = pendingRestores.shift();
    if (issueNumber) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ issueNumber }));
    }
    res.writeHead(204); return res.end();
  }

  // --- POST /api/restore  (queue a restore for the agent) ---
  if (req.method === 'POST' && url.pathname === '/api/restore') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      try {
        const { issueNumber } = JSON.parse(body);
        if (!issueNumber) throw new Error('Missing issueNumber');
        pendingRestores.push(issueNumber);
        console.log(`[Restore] Queued restore for issue #${issueNumber}`);
        res.writeHead(204); res.end();
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // --- PATCH /api/log/:issueId  (update fix/acceptance status) ---
  if (req.method === 'PATCH' && url.pathname.startsWith('/api/log/')) {
    const issueId = decodeURIComponent(url.pathname.split('/').pop());
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      try {
        const updates = JSON.parse(body);
        const entry = updateLog(issueId, updates);
        res.writeHead(entry ? 200 : 404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(entry || { error: 'not found' }));
      } catch {
        res.writeHead(400); res.end();
      }
    });
    return;
  }

  // --- GET /api/log  (retrieve full log for reporting) ---
  if (req.method === 'GET' && url.pathname === '/api/log') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify(readLog()));
  }

  // --- Static files ---
  const filePath = path.join(__dirname, url.pathname === '/' ? 'index.html' : url.pathname);
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(filePath)] || 'text/plain' });
    res.end(data);
  });
});

server.listen(PORT, () => {
  console.log(`\n✓ Tableau Support Extension running at http://localhost:${PORT}`);
  console.log(`  Token loaded: ${GITHUB_TOKEN.slice(0, 8)}…`);
  console.log(`  Repo: ${REPO}\n`);
  console.log('  Open Tableau, add an Extension object, and load:');
  console.log(`  manifest.trex  (points to http://localhost:${PORT})\n`);
});
