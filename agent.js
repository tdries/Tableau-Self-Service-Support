const Anthropic = require('@anthropic-ai/sdk');
const AdmZip    = require('adm-zip');
const https     = require('https');
const http      = require('http');
const path      = require('path');
const fs        = require('fs');

// ---- Config ----
function loadEnv() {
  try {
    const raw = fs.readFileSync(path.join(__dirname, '.env'), 'utf8');
    const result = {};
    for (const line of raw.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      const idx = trimmed.indexOf('=');
      if (idx === -1) continue;
      result[trimmed.slice(0, idx).trim()] = trimmed.slice(idx + 1).trim();
    }
    return result;
  } catch { return {}; }
}

const env = loadEnv();
const e = k => env[k] || process.env[k] || '';

const GITHUB_TOKEN       = e('api') || e('GITHUB_TOKEN') || e('GH_TOKEN');
const ANTHROPIC_KEY      = e('ANTHROPIC_API_KEY');
const JIRA_TOKEN         = (e('TAB_SUPPORT_AI_FULL') || e('TAB_SUPPORT_AI')).replace(/^["']|["']$/g, '');
const JIRA_EMAIL         = e('JIRA_EMAIL');
const JIRA_HOST          = 'biztory.atlassian.net';
const TABLEAU_URL        = (e('Tableau_URL') || 'https://10ax.online.tableau.com').replace(/\/$/, '');
const SERVER_URL         = e('RAILWAY_URL') || 'http://localhost:8766';
const REPO               = 'tdries/Tableau-Self-Service-Support';

// Site-specific Tableau credentials
const TABLEAU_SITES = {
  biztorypulse: {
    site:      e('Site') || 'biztorypulse',
    patName:   e('TABLEAU_PAT_NAME') || 'test2',
    patSecret: e('TABLEAU_PAT_SECRET') || ''
  },
  timdriesdev: {
    site:      'timdriesdev',
    patName:   e('TABLEAU_PAT_NAME_TIMDRIESDEV') || '',
    patSecret: e('TABLEAU_PAT_SECRET_TIMDRIESDEV') || ''
  }
};

const DEFAULT_SITE = 'timdriesdev';

function getTableauCreds(siteKey) {
  return TABLEAU_SITES[siteKey] || TABLEAU_SITES[DEFAULT_SITE];
}

if (!GITHUB_TOKEN)  { console.error('[ERROR] GitHub token missing'); process.exit(1); }
if (!ANTHROPIC_KEY) { console.error('[ERROR] ANTHROPIC_API_KEY missing'); process.exit(1); }

const anthropic = new Anthropic({ apiKey: ANTHROPIC_KEY });
const seen    = new Set();

// ---- Safe JSON parser — handles literal newlines inside string values ----
function safeParseJson(text) {
  const match = text.match(/\{[\s\S]*\}/);
  if (!match) throw new Error('No JSON object found in response');
  try {
    return JSON.parse(match[0]);
  } catch {
    // Escape unescaped newlines/tabs inside quoted strings
    const cleaned = match[0].replace(/"((?:[^"\\]|\\.)*)"/gs,
      (_, inner) => '"' + inner.replace(/\n/g, '\\n').replace(/\r/g, '\\r').replace(/\t/g, '\\t') + '"'
    );
    return JSON.parse(cleaned);
  }
}

// ---- Anthropic call with retry on 529 overload ----
async function claudeCreate(params, retries = 3) {
  for (let i = 0; i <= retries; i++) {
    try {
      return await anthropic.messages.create(params);
    } catch (err) {
      const is529 = err?.status === 529 || String(err?.message).includes('overloaded');
      if (is529 && i < retries) {
        const wait = 15000 * (i + 1); // 15s, 30s, 45s, 60s
        console.log(`  [Claude] 529 overloaded — retrying in ${wait / 1000}s (attempt ${i + 1}/${retries})`);
        await new Promise(r => setTimeout(r, wait));
      } else {
        throw err;
      }
    }
  }
}
const backups = new Map(); // issueNumber -> { workbookId, projectId, name, originalBuffer }

// ---- Generic HTTP/HTTPS helper ----
function httpReq(options, body) {
  return new Promise((resolve, reject) => {
    const mod = (options.hostname === 'localhost' || options.protocol === 'http:') ? http : https;
    const r = mod.request(options, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const buf = Buffer.concat(chunks);
        if (options._binary) {
          resolve({ status: res.statusCode, buffer: buf });
        } else {
          const text = buf.toString();
          try { resolve({ status: res.statusCode, body: JSON.parse(text) }); }
          catch { resolve({ status: res.statusCode, body: text }); }
        }
      });
    });
    r.on('error', reject);
    if (body) r.write(body);
    r.end();
  });
}

// ---- GitHub API ----
function gh(method, endpoint, body) {
  const data = body ? JSON.stringify(body) : null;
  return httpReq({
    hostname: 'api.github.com',
    path: endpoint,
    method,
    headers: {
      'Authorization': `Bearer ${GITHUB_TOKEN}`,
      'Accept': 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
      'User-Agent': 'TabServo/1.0',
      ...(data ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) } : {})
    }
  }, data);
}

// ---- Jira API ----
function jira(method, endpoint, body) {
  const auth = Buffer.from(`${JIRA_EMAIL}:${JIRA_TOKEN}`).toString('base64');
  const data = body ? JSON.stringify(body) : null;
  return httpReq({
    hostname: JIRA_HOST,
    path: endpoint,
    method,
    headers: {
      'Authorization': `Basic ${auth}`,
      'Accept': 'application/json',
      ...(data ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) } : {})
    }
  }, data);
}

function jiraComment(issueKey, adfContent) {
  // Accept either a plain string or pre-built ADF content array
  const content = typeof adfContent === 'string'
    ? adfContent.split('\n\n').map(p => ({ type: 'paragraph', content: [{ type: 'text', text: p }] }))
    : adfContent;
  return jira('POST', `/rest/api/3/issue/${issueKey}/comment`, {
    body: { type: 'doc', version: 1, content }
  });
}

// Build rich ADF content helpers
function adfParagraph(...parts) {
  return { type: 'paragraph', content: parts };
}
function adfText(text, bold = false) {
  const node = { type: 'text', text };
  if (bold) node.marks = [{ type: 'strong' }];
  return node;
}
function adfRule() {
  return { type: 'rule' };
}
function adfBulletList(items) {
  return { type: 'bulletList', content: items.map(text => ({
    type: 'listItem', content: [{ type: 'paragraph', content: [{ type: 'text', text }] }]
  }))};
}

function adfToText(node) {
  if (!node) return '';
  if (node.type === 'text') return node.text || '';
  if (node.content) return node.content.map(adfToText).join('\n');
  return '';
}

// ---- Tableau REST API ----
const TABLEAU_API = `${TABLEAU_URL}/api/3.22`;

async function tableauAuth(siteKey) {
  const creds = getTableauCreds(siteKey);
  const body = JSON.stringify({
    credentials: {
      personalAccessTokenName:   creds.patName,
      personalAccessTokenSecret: creds.patSecret,
      site: { contentUrl: creds.site }
    }
  });
  const { body: resp, status } = await httpReq({
    hostname: new URL(TABLEAU_API).hostname,
    path: '/api/3.22/auth/signin',
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Accept': 'application/json', 'Content-Length': Buffer.byteLength(body) }
  }, body);
  if (!resp?.credentials?.token) throw new Error(`Tableau auth failed (${status}): ${JSON.stringify(resp)}`);
  return { token: resp.credentials.token, siteId: resp.credentials.site.id };
}

async function tableauFindWorkbook(token, siteId, name) {
  const { body } = await httpReq({
    hostname: new URL(TABLEAU_API).hostname,
    path: `/api/3.22/sites/${siteId}/workbooks?filter=name:eq:${encodeURIComponent(name)}`,
    method: 'GET',
    headers: { 'X-Tableau-Auth': token, 'Accept': 'application/json' }
  });
  const list = body?.workbooks?.workbook || [];
  if (!list.length) throw new Error(`Workbook '${name}' not found in Tableau Cloud`);
  return list[0]; // { id, name, project: { id, name } }
}

async function tableauDownload(token, siteId, workbookId) {
  const { buffer, status } = await httpReq({
    hostname: new URL(TABLEAU_API).hostname,
    path: `/api/3.22/sites/${siteId}/workbooks/${workbookId}/content`,
    method: 'GET',
    headers: { 'X-Tableau-Auth': token },
    _binary: true
  });
  if (status !== 200) throw new Error(`Tableau download failed: HTTP ${status}`);
  return buffer;
}

async function tableauPublish(token, siteId, projectId, workbookName, twbxBuffer) {
  const boundary = `TabServoBoundary${Date.now()}`;
  const meta = JSON.stringify({ workbook: { name: workbookName, project: { id: projectId } } });
  const NL = '\r\n';
  const body = Buffer.concat([
    Buffer.from(`--${boundary}${NL}Content-Disposition: name="request_payload"${NL}Content-Type: application/json${NL}${NL}${meta}${NL}`),
    Buffer.from(`--${boundary}${NL}Content-Disposition: name="tableau_workbook"; filename="${workbookName}.twbx"${NL}Content-Type: application/octet-stream${NL}${NL}`),
    twbxBuffer,
    Buffer.from(`${NL}--${boundary}--${NL}`)
  ]);
  const { body: resp, status } = await httpReq({
    hostname: new URL(TABLEAU_API).hostname,
    path: `/api/3.22/sites/${siteId}/workbooks?overwrite=true`,
    method: 'POST',
    headers: {
      'X-Tableau-Auth': token,
      'Content-Type': `multipart/mixed; boundary=${boundary}`,
      'Content-Length': body.length
    }
  }, body);
  if (status >= 400) throw new Error(`Tableau publish failed (${status}): ${JSON.stringify(resp)}`);
  return resp;
}

// ---- Workbook in-memory processing ----
function unpackWorkbook(buffer) {
  const zip = new AdmZip(buffer);
  const entry = zip.getEntries().find(e => e.entryName.endsWith('.twb'));
  if (!entry) throw new Error('No .twb file found inside the .twbx archive');
  return { twbXml: zip.readAsText(entry), zip, twbEntryName: entry.entryName };
}

function repackWorkbook(xml, zip, twbEntryName) {
  zip.updateFile(twbEntryName, Buffer.from(xml, 'utf8'));
  return zip.toBuffer();
}

// ---- Progress reporting ----
function reportProgress(issueNumber, step, pct, type = 'info', extra = {}) {
  if (pct >= 100 && type !== 'ok') logFixResult(String(issueNumber), false);
  const body = JSON.stringify({ step, pct, type, ...extra });
  const parsed = new URL(`/api/progress/${issueNumber}`, SERVER_URL);
  const mod  = parsed.protocol === 'https:' ? https : http;
  const port = parsed.port ? parseInt(parsed.port) : (parsed.protocol === 'https:' ? 443 : 80);
  const r = mod.request({
    hostname: parsed.hostname, port,
    path: `/api/progress/${issueNumber}`,
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
  }, res => res.resume());
  r.on('error', () => {});
  r.write(body);
  r.end();
  console.log(`  [${pct}%] ${step}`);
}

function logFixResult(issueId, succeeded) {
  const body = JSON.stringify({ fixSucceeded: succeeded });
  const parsed = new URL(`/api/log/${encodeURIComponent(issueId)}`, SERVER_URL);
  const mod = parsed.protocol === 'https:' ? https : http;
  const port = parsed.port ? parseInt(parsed.port) : (parsed.protocol === 'https:' ? 443 : 80);
  const r = mod.request({
    hostname: parsed.hostname, port,
    path: parsed.pathname,
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
  }, res => res.resume());
  r.on('error', () => {});
  r.write(body);
  r.end();
}

// ---- XML helpers ----
function validateXml(xml) {
  if (!xml.includes('<workbook') || !xml.includes('</workbook>'))
    return { valid: false, error: 'Missing <workbook> root element' };
  return { valid: true };
}

function extractRelevantXml(xml) {
  const cap = (str, max) => str.length > max ? str.slice(0, max) + '\n<!-- ...truncated -->' : str;
  const sections = [];

  const runMatches = [...xml.matchAll(/<run[^>]*>[^<\n]+<\/run>/g)];
  if (runMatches.length)
    sections.push(`<!-- TEXT ELEMENTS -->\n${cap([...new Set(runMatches.map(m => m[0].trim()))].join('\n'), 6000)}`);

  const calcMatches = [...xml.matchAll(/<column([^>]*)>[\s\S]*?<calculation([^>]*)\/?>[\s\S]*?<\/column>/g)];
  if (calcMatches.length)
    sections.push(`<!-- CALCULATED FIELDS -->\n${cap(calcMatches.map(m => m[0].trim()).join('\n'), 8000)}`);

  const paramMatches = [...xml.matchAll(/<column[^>]*param-domain-type='[^']*'[^>]*>[\s\S]*?<\/column>/g)];
  if (paramMatches.length)
    sections.push(`<!-- PARAMETERS -->\n${cap(paramMatches.map(m => m[0].trim()).join('\n'), 2000)}`);

  const connMatches = [...xml.matchAll(/<connection\b[^>]*>/g)];
  if (connMatches.length)
    sections.push(`<!-- CONNECTIONS -->\n${[...new Set(connMatches.map(m => m[0]))].join('\n').slice(0, 1000)}`);

  const filterMatches = [...xml.matchAll(/<filter[^>]*>[\s\S]*?<\/filter>/g)];
  if (filterMatches.length)
    sections.push(`<!-- FILTERS -->\n${cap(filterMatches.slice(0, 20).map(m => m[0].trim()).join('\n'), 2000)}`);

  // Full worksheet blocks — agent needs complete examples as templates for creating new sheets
  const wsMatches = [...xml.matchAll(/<worksheet name='[^']*'>[\s\S]*?<\/worksheet>/g)];
  if (wsMatches.length) {
    // Include up to 2 full worksheets as templates + list remaining names
    const fullSheets = wsMatches.slice(0, 2).map(m => m[0]);
    const remainingNames = wsMatches.slice(2).map(m => m[0].match(/<worksheet name='([^']+)'/)?.[1]).filter(Boolean);
    let wsSection = `<!-- WORKSHEETS (${wsMatches.length} total) -->\n${cap(fullSheets.join('\n'), 12000)}`;
    if (remainingNames.length) wsSection += `\n<!-- Additional worksheets: ${remainingNames.join(', ')} -->`;
    wsSection += `\n<!-- INSERT NEW WORKSHEETS BEFORE: </worksheets> -->`;
    sections.push(wsSection);
  }

  // Full dashboard sections — layout tree needed to add new sheets to dashboards
  const dashMatches = [...xml.matchAll(/<dashboard\b[\s\S]*?<\/dashboard>/g)];
  if (dashMatches.length)
    sections.push(`<!-- DASHBOARDS (layout) — use insert_before on a dashboard's <simple-id> to add new zones -->\n${cap(dashMatches.map(m => m[0]).join('\n'), 20000)}`);

  return sections.join('\n\n') || '<!-- No key sections extracted -->';
}

function extractWorkbookName(issueBody) {
  // Markdown format: **Workbook:** name
  const md = issueBody.match(/\*\*Workbook:\*\*\s*(.+?)(?:\n|$)/);
  if (md) return md[1].trim();
  // ADF table text: "Workbook\nname" (from adfToText)
  const adf = issueBody.match(/Workbook\n(.+?)(?:\n|$)/);
  if (adf) return adf[1].trim();
  return null;
}

function hasFixAppliedLabel(issue) {
  return issue.labels?.some(l => l.name === 'fix-applied');
}

// ---- Shared BUDA system prompt ----
const BUDA_SYSTEM = `You are TabServo, an expert Tableau workbook engineer and automated repair agent. You edit TWB/TWBX XML files with surgical precision based on the official Tableau 2026.1 schema.

## WORKBOOK TOP-LEVEL STRUCTURE
\`\`\`xml
<workbook version="..." source-build="..." source-platform="win|mac">
  <datasources>   <!-- connections, columns, calculated fields, parameters, filters -->
  <worksheets>    <!-- one <worksheet> per sheet tab -->
  <dashboards>    <!-- one <dashboard> per dashboard tab -->
</workbook>
\`\`\`

## DATASOURCE — COLUMNS & CALCULATED FIELDS
\`\`\`xml
<column name='[FieldName]'
        datatype='string|integer|real|boolean|date|datetime'
        role='dimension|measure'
        type='ordinal|quantitative|nominal'
        caption='Human Label'
        hidden='true|false'>
  <!-- For calculated fields only: -->
  <calculation class='tableau' formula='SUM([Sales])' />
</column>
\`\`\`
- \`calculation class\` valid values: \`tableau\` | \`passthrough\` | \`bin\` | \`categorical-bin\`
- Field names in formulas and on shelves ALWAYS use [Square Brackets]
- NEVER touch \`<connection>\` elements — vendor-specific, altering them breaks the datasource
- NEVER rename sheet/tab names — the \`name\` attribute on \`<worksheet>\` and \`<dashboard>\` elements must stay unchanged, renaming breaks Tableau Cloud refresh

## WORKSHEET STRUCTURE (inline format used in .twb files)
\`\`\`xml
<worksheet name='Sheet Name'>
  <table>
    <view>
      <datasource-dependencies datasource='datasourceName'>
        <column-instance column='[Field]' derivation='None' name='[Field]' type='quantitative|ordinal|nominal' />
      </datasource-dependencies>
      <aggregation value='true' />  <!-- REQUIRED — omitting this causes parse error -->
    </view>
    <style/>
    <panes>
      <pane>
        <mark class='Bar|Line|Circle|Square|Area|Pie|Text|Shape|GanttBar|Polygon|Heatmap|Automatic' />
      </pane>
    </panes>
    <rows>[FieldA]</rows>
    <cols>[FieldB]</cols>
  </table>
  <simple-id uuid='{XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX}' />  <!-- REQUIRED, unique per worksheet -->
</worksheet>
\`\`\`
Valid mark class values (schema PrimitiveType-ST): \`Automatic\`, \`Bar\`, \`Line\`, \`Area\`, \`Circle\`, \`Square\`, \`Shape\`, \`Text\`, \`Pie\`, \`GanttBar\`, \`Polygon\`, \`PolyLine\`, \`Heatmap\`, \`Rectangle\`, \`Icon\`, \`VizExtension\`

Rows/cols shelf syntax:
- Single field: \`[Field Name]\`
- Multiple fields on same shelf: \`[Field1]:[Field2]\` (colon-separated)
- Optional attrs: \`include-empty='true'\`, \`total='true'\`

## DASHBOARD STRUCTURE
\`\`\`xml
<dashboard name='Dashboard Name'>
  <size minwidth='800' minheight='600' maxwidth='1200' maxheight='800' sizing-mode='automatic|fixed|range' />
  <zones>
    <!-- Root layout zone — always present -->
    <zone id='1' x='0' y='0' w='1200' h='800' type-v2='layout-basic'>
      <zone id='2' x='0' y='0' w='600' h='800' name='Sheet Name' type-v2='worksheet' />
      <zone id='3' x='600' y='0' w='600' h='800' name='Other Sheet' type-v2='worksheet' />
    </zone>
  </zones>
  <simple-id uuid='{XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX}' />  <!-- REQUIRED -->
</dashboard>
\`\`\`
Zone rules — violating these causes HTTP 400 on publish:
- \`x\`, \`y\`, \`w\`, \`h\`, \`id\` are ALL required integers on every zone
- Zone \`id\` must be unique across the ENTIRE dashboard — scan existing ids before inserting
- \`type-v2\` valid values: \`worksheet\`, \`text\`, \`title\`, \`blank\`, \`web\`, \`paramctrl\`, \`filter\`, \`highlighter\`, \`bitmap\`, \`layout-basic\`, \`layout-flow\`, \`add-in\`, \`dashboard-object\`, \`empty\`, \`map\`
- Worksheet zone: \`name\` must EXACTLY match the \`<worksheet name='...'>\` attribute

## ADDING A NEW WORKSHEET + DASHBOARD ZONE (do ALL of these)
Use \`insert_after\` with an anchor that EXISTS VERBATIM in the XML.

**Step 1 — Insert the new worksheet:**
- Use \`"op": "insert_after"\` with \`"find"\` set to the COMPLETE closing \`</worksheet>\` tag of the LAST existing worksheet (copy the exact closing tag from the XML)
- The \`"insert"\` must be a COMPLETE \`<worksheet>...</worksheet>\` block modeled on an existing worksheet in the XML
- Copy the structure of an existing worksheet — include \`<table>\`, \`<view>\`, \`<datasource-dependencies>\`, \`<panes>\`, \`<rows>\`, \`<cols>\`, \`<simple-id>\`
- Generate a NEW unique UUID for \`<simple-id>\`

**Step 2 — Add the zone to the dashboard:**
- Use \`"op": "insert_before"\` with \`"find"\` set to the dashboard's \`<simple-id\` tag (copy it EXACTLY from the dashboard XML including the uuid)
- The \`"insert"\` is a COMPLETE \`<zone>\` element with unique \`id\`, \`name\` matching the new worksheet name, \`type-v2='worksheet'\`, and \`x\`/\`y\`/\`w\`/\`h\` values
- Use a unique zone \`id\` (scan ALL existing zone ids in the dashboard, pick the next available number)

**Step 3 — Resize an existing sibling zone** to make room for the new zone (use \`replace\` to change its \`h\` or \`w\` value)

CRITICAL: Every \`find\` string must appear VERBATIM in the raw XML. Use the exact XML provided — never fabricate or summarize tags.

## FILTERS
\`\`\`xml
<!-- Categorical -->
<filter class='categorical' column='[Field]'>
  <groupfilter function='member' member='value'/>
</filter>
<!-- Quantitative -->
<filter class='quantitative' column='[Field]' min='0' max='100'>
  <min>0</min><max>100</max>
</filter>
<!-- Relative date -->
<filter class='relative-date' column='[Date Field]'>
  <period-type-v2 value='year'/><period-anchor value='today'/><period-range-n value='1'/>
</filter>
\`\`\`

## FORMULA SYNTAX
- Aggregations: \`SUM([F])\`, \`AVG([F])\`, \`MIN([F])\`, \`MAX([F])\`, \`COUNT([F])\`, \`COUNTD([F])\`, \`ATTR([F])\`
- LOD: \`{ FIXED [dim] : AGG([F]) }\`, \`{ INCLUDE [dim] : AGG([F]) }\`, \`{ EXCLUDE [dim] : AGG([F]) }\`
- Table calcs: \`RUNNING_SUM()\`, \`WINDOW_SUM()\`, \`LOOKUP()\`, \`INDEX()\`, \`RANK()\`
- Logical: \`IF [F] THEN ... ELSEIF ... ELSE ... END\`, \`IIF()\`, \`IFNULL([F], val)\`, \`ZN([F])\`
- String: \`STR([F])\`, \`LEFT([F],n)\`, \`CONTAINS([F],'str')\`, \`REPLACE()\`, \`TRIM()\`
- Date: \`DATEPART('year|quarter|month|week|day',[F])\`, \`DATEADD()\`, \`DATEDIFF()\`, \`DATETRUNC()\`

## FIX OPERATIONS
1. **replace** — \`{ "op": "replace", "find": "...", "replace": "..." }\`
2. **insert_after** — \`{ "op": "insert_after", "find": "...", "insert": "..." }\` — inserts AFTER the found string
3. **insert_before** — \`{ "op": "insert_before", "find": "...", "insert": "..." }\` — inserts BEFORE the found string
4. **delete** — \`{ "op": "delete", "find": "..." }\`

## RULES
- Every \`find\` string MUST appear verbatim in the workbook XML — copy it exactly
- Make the smallest possible change; never restructure elements unnecessarily
- NEVER modify \`<connection>\` attributes
- NEVER rename sheet/tab names (\`name\` attribute on \`<worksheet>\` or \`<dashboard>\` elements) — renaming breaks Tableau Cloud refresh and downstream references
- Preserve the exact quoting style (' vs ") of surrounding XML
- When fixing a calculated field, only change the \`formula='...'\` attribute value
- New worksheets MUST have a unique \`<simple-id uuid='...'>\` — generate a valid UUID
- New dashboard zones MUST have unique \`id\` values — scan existing ids first`;

// ---- Core: analyze + fix ----
async function analyzeAndFix(issue, siteKey) {
  console.log(`\n[Agent] Issue #${issue.number}: ${issue.title}`);
  const n = issue.number;

  const workbookName = extractWorkbookName(issue.body);
  if (!workbookName) {
    console.log('  → No workbook name in issue, skipping');
    return;
  }

  reportProgress(n, 'Issue received — connecting to Tableau Cloud…', 10);

  // Auth + find workbook
  let token, siteId, workbook;
  try {
    ({ token, siteId } = await tableauAuth(siteKey));
    console.log('  → Authenticated with Tableau Cloud');
    workbook = await tableauFindWorkbook(token, siteId, workbookName);
    console.log(`  → Found: ${workbook.name} (${workbook.id})`);
  } catch (err) {
    reportProgress(n, `Error: ${err.message}`, 100, 'error');
    await gh('POST', `/repos/${REPO}/issues/${n}/comments`, { body: `⚠️ **Agent error:** ${err.message}` });
    return;
  }

  // Download workbook
  let originalBuffer;
  reportProgress(n, 'Downloading workbook from Tableau Cloud…', 22);
  try {
    originalBuffer = await tableauDownload(token, siteId, workbook.id);
    console.log(`  → Downloaded (${Math.round(originalBuffer.length / 1024)}KB)`);
  } catch (err) {
    reportProgress(n, `Error: ${err.message}`, 100, 'error');
    await gh('POST', `/repos/${REPO}/issues/${n}/comments`, { body: `⚠️ **Could not download workbook:** ${err.message}` });
    return;
  }

  // Unpack XML
  let wb;
  reportProgress(n, 'Reading workbook…', 30);
  try {
    wb = unpackWorkbook(originalBuffer);
    console.log(`  → Unpacked XML (${Math.round(wb.twbXml.length / 1024)}KB)`);
  } catch (err) {
    reportProgress(n, `Error: ${err.message}`, 100, 'error');
    await gh('POST', `/repos/${REPO}/issues/${n}/comments`, { body: `⚠️ **Could not read workbook:** ${err.message}` });
    return;
  }

  const relevantXml = extractRelevantXml(wb.twbXml);

  // BUDA analysis
  reportProgress(n, 'TabServo is analyzing the issue…', 45);
  console.log('  → Calling Biztory AI...');
  let result;
  try {
    const msg = await claudeCreate({
      model: 'claude-opus-4-6',
      max_tokens: 8096,
      system: BUDA_SYSTEM,

      messages: [{
        role: 'user',
        content: `A user submitted the following Tableau support issue:

**Title:** ${issue.title}
**Description:**
${issue.body.split('---')[0].trim()}

Key XML sections extracted from the workbook:

\`\`\`xml
${relevantXml}
\`\`\`

Diagnose the root cause and return the fix. Respond ONLY with a valid JSON object (no markdown outside it):
{
  "analysis": "One-sentence root cause",
  "fixes": [
    { "op": "replace|insert_after|insert_before|delete", "find": "verbatim XML to locate", "replace": "...", "insert": "..." }
  ],
  "comment": "Human-readable summary of what was changed and what the user should do next"
}

Omit \`replace\` for delete ops, omit \`insert\` for replace ops. Use \`insert_before\` when adding zones to dashboards (anchor on the dashboard's \`<simple-id\` tag). When the user asks for a new chart/sheet, you MUST also add it to the relevant dashboard. Return empty fixes array if no safe fix is possible.`
      }]
    });

    result = safeParseJson(msg.content[0].text.trim());
  } catch (err) {
    console.log(`  → Biztory AI error: ${err.message}`);
    reportProgress(n, `Error: ${err.message}`, 100, 'error');
    await gh('POST', `/repos/${REPO}/issues/${n}/comments`, { body: `⚠️ **Agent error during analysis:** ${err.message}` });
    return;
  }

  console.log(`  → Analysis: ${result.analysis}`);

  if (!result.fixes || result.fixes.length === 0) {
    reportProgress(n, 'No automatic fix found — manual review needed', 100, 'warn');
    await gh('POST', `/repos/${REPO}/issues/${n}/comments`, {
      body: `🔍 **No automatic fix applied**\n\n${result.analysis}\n\n${result.comment || ''}`
    });
    return;
  }

  // Apply fixes
  reportProgress(n, 'Applying fix to workbook…', 65);
  let xml = wb.twbXml;
  let applied = 0;
  const log = [];

  for (const fix of result.fixes) {
    const op     = fix.op || 'replace';
    const anchor = fix.find ? fix.find.slice(0, 80) + (fix.find.length > 80 ? '…' : '') : '';

    if (!fix.find || !xml.includes(fix.find)) {
      log.push(`⚠️ Not found (${op}): \`${anchor}\``);
      console.log(`  → Could not locate fix string for op: ${op}`);
      continue;
    }

    if      (op === 'replace')      xml = xml.split(fix.find).join(fix.replace);
    else if (op === 'insert_after')  xml = xml.split(fix.find).join(fix.find + fix.insert);
    else if (op === 'insert_before') xml = xml.split(fix.find).join(fix.insert + fix.find);
    else if (op === 'delete')       xml = xml.split(fix.find).join('');
    else { log.push(`⚠️ Unknown op \`${op}\``); continue; }

    applied++;
    log.push(`✅ ${op}: \`${anchor}\``);
    console.log(`  → Applied fix ${applied} (${op})`);
  }

  if (applied === 0) {
    reportProgress(n, 'Fix could not be located in workbook XML', 100, 'warn');
    await gh('POST', `/repos/${REPO}/issues/${n}/comments`, {
      body: `⚠️ **Fix identified but could not be applied**\n\n**Analysis:** ${result.analysis}\n\n${log.join('\n')}`
    });
    return;
  }

  // Validate
  reportProgress(n, 'Validating XML…', 75);
  const validation = validateXml(xml);
  if (!validation.valid) {
    reportProgress(n, 'Fix produced invalid XML — not applied', 100, 'error');
    await gh('POST', `/repos/${REPO}/issues/${n}/comments`, {
      body: `⚠️ **Fix produced invalid XML — not applied**\n\n${validation.error}`
    });
    return;
  }

  // Repack in memory
  reportProgress(n, 'Repacking workbook…', 82);
  let fixedBuffer;
  try {
    fixedBuffer = repackWorkbook(xml, wb.zip, wb.twbEntryName);
  } catch (err) {
    reportProgress(n, `Error repacking: ${err.message}`, 100, 'error');
    return;
  }

  // Store backup for potential restore
  backups.set(n, { workbookId: workbook.id, projectId: workbook.project.id, name: workbook.name, originalBuffer });

  // Publish to Tableau Cloud
  reportProgress(n, 'Publishing fixed workbook to Tableau Cloud…', 90);
  try {
    await tableauPublish(token, siteId, workbook.project.id, workbook.name, fixedBuffer);
    console.log('  → Published to Tableau Cloud');
  } catch (err) {
    reportProgress(n, `Error publishing: ${err.message}`, 100, 'error');
    await gh('POST', `/repos/${REPO}/issues/${n}/comments`, { body: `⚠️ **Fix generated but publishing failed:** ${err.message}` });
    return;
  }

  await gh('POST', `/repos/${REPO}/issues/${n}/comments`, {
    body: `✅ **Fix applied automatically by TabServo**\n\n${result.comment}\n\n**Changes (${applied}/${result.fixes.length}):**\n${log.join('\n')}\n\nThe workbook has been republished to Tableau Cloud. Reload your dashboard to see the changes.`
  });
  await gh('POST', `/repos/${REPO}/issues/${n}/labels`, { labels: ['fix-applied'] });

  reportProgress(n, 'Fix published — reload your dashboard', 100, 'ok', { hasBackup: true, issueNumber: n });
  logFixResult(String(n), true);
  console.log(`  → Done. Issue #${n} marked fix-applied.`);
}

// ---- Jira: analyze + fix ----
const seenJira = new Set();

async function analyzeAndFixJira(issueKey, siteKey) {
  if (seenJira.has(issueKey)) return;
  seenJira.add(issueKey);
  console.log(`\n[Agent] Jira ${issueKey}`);

  // Fetch issue
  reportProgress(issueKey, 'Fetching Jira issue…', 5);
  let issue;
  try {
    const { body, status } = await jira('GET', `/rest/api/3/issue/${issueKey}`);
    if (status >= 400) throw new Error(`HTTP ${status}`);
    issue = body;
  } catch (err) {
    reportProgress(issueKey, `Failed to fetch issue: ${err.message}`, 100, 'error');
    return;
  }

  const title    = issue.fields.summary;
  const descText = adfToText(issue.fields.description);
  const reporter = issue.fields.reporter?.displayName?.split(' ')[0] || 'there';

  // Step 1 — interpretation comment
  reportProgress(issueKey, 'Interpreting issue…', 8);
  let interpretation;
  try {
    const msg = await claudeCreate({
      model: 'claude-opus-4-6',
      max_tokens: 300,
      messages: [{ role: 'user', content:
        `You are TabServo, a Tableau support AI by Biztory. A user named ${reporter} submitted this issue:\n\nTitle: ${title}\nDescription: ${descText.slice(0, 800)}\n\nWrite a professional Jira comment (3-4 sentences) that:\n1. Opens with "Dear ${reporter},"\n2. Thanks them for reaching out\n3. Restates how you understand the problem\n4. Confirms you are now analyzing the workbook and will follow up shortly\n\nKeep the tone warm but professional. Do not use emojis.`
      }]
    });
    interpretation = msg.content[0].text.trim();
  } catch {
    interpretation = `Dear ${reporter},\n\nThank you for reaching out. We have received your issue "${title}" and are now analyzing your workbook. We will follow up shortly with our findings.`;
  }

  await jiraComment(issueKey, interpretation);
  reportProgress(issueKey, 'Response posted — connecting to Tableau Cloud…', 12);

  // Extract workbook name
  const workbookName = extractWorkbookName(descText);
  if (!workbookName) {
    reportProgress(issueKey, 'No workbook name found in issue', 100, 'warn');
    await jiraComment(issueKey, [
      adfParagraph(adfText(`Dear ${reporter},`)),
      adfParagraph(adfText('We were unable to identify a workbook name in your request. Could you please resubmit your issue and include the exact name of the Tableau workbook you need help with?')),
      adfParagraph(adfText('Thank you for your patience.')),
      adfRule(),
      adfParagraph(adfText('TabServo — Biztory Tableau AI Support', true))
    ]);
    return;
  }

  // Auth + find workbook
  let token, siteId, workbook;
  try {
    ({ token, siteId } = await tableauAuth(siteKey));
    workbook = await tableauFindWorkbook(token, siteId, workbookName);
    console.log(`  → Found: ${workbook.name} (${workbook.id})`);
  } catch (err) {
    reportProgress(issueKey, `Error: ${err.message}`, 100, 'error');
    await jiraComment(issueKey, [
      adfParagraph(adfText(`Dear ${reporter},`)),
      adfParagraph(adfText('We encountered an issue while connecting to Tableau Cloud to locate your workbook. Our team has been notified and will investigate further.')),
      adfParagraph(adfText('Error details: ', true), adfText(err.message)),
      adfRule(),
      adfParagraph(adfText('TabServo — Biztory Tableau AI Support', true))
    ]);
    return;
  }

  // Download
  let originalBuffer;
  reportProgress(issueKey, 'Downloading workbook from Tableau Cloud…', 22);
  try {
    originalBuffer = await tableauDownload(token, siteId, workbook.id);
    console.log(`  → Downloaded (${Math.round(originalBuffer.length / 1024)}KB)`);
  } catch (err) {
    reportProgress(issueKey, `Error: ${err.message}`, 100, 'error');
    await jiraComment(issueKey, [
      adfParagraph(adfText(`Dear ${reporter},`)),
      adfParagraph(adfText('We were unable to download your workbook from Tableau Cloud. This may be a temporary issue. Our team has been notified.')),
      adfParagraph(adfText('Error details: ', true), adfText(err.message)),
      adfRule(),
      adfParagraph(adfText('TabServo — Biztory Tableau AI Support', true))
    ]);
    return;
  }

  // Unpack
  let wb;
  reportProgress(issueKey, 'Reading workbook…', 30);
  try {
    wb = unpackWorkbook(originalBuffer);
    console.log(`  → Unpacked XML (${Math.round(wb.twbXml.length / 1024)}KB)`);
  } catch (err) {
    reportProgress(issueKey, `Error: ${err.message}`, 100, 'error');
    await jiraComment(issueKey, [
      adfParagraph(adfText(`Dear ${reporter},`)),
      adfParagraph(adfText('We were unable to read the contents of your workbook. The file may be in an unexpected format. Our team has been notified and will follow up.')),
      adfParagraph(adfText('Error details: ', true), adfText(err.message)),
      adfRule(),
      adfParagraph(adfText('TabServo — Biztory Tableau AI Support', true))
    ]);
    return;
  }

  const relevantXml = extractRelevantXml(wb.twbXml);

  // BUDA analysis
  reportProgress(issueKey, 'TabServo is analyzing the workbook…', 45);
  let result;
  try {
    const msg = await claudeCreate({
      model: 'claude-opus-4-6',
      max_tokens: 8096,
      system: BUDA_SYSTEM,
      messages: [{ role: 'user', content:
        `A user submitted the following Tableau support issue:\n\n**Title:** ${title}\n**Description:**\n${descText.split('---')[0].trim()}\n\nKey XML sections extracted from the workbook:\n\n\`\`\`xml\n${relevantXml}\n\`\`\`\n\nDiagnose the root cause and return the fix. Respond ONLY with a valid JSON object (no markdown outside it):\n{\n  "analysis": "One-sentence root cause",\n  "fixes": [\n    { "op": "replace|insert_after|insert_before|delete", "find": "verbatim XML to locate", "replace": "...", "insert": "..." }\n  ],\n  "comment": "Human-readable summary of what was changed and what the user should do next"\n}\n\nOmit \`replace\` for delete ops, omit \`insert\` for replace ops. Use \`insert_before\` when adding zones to dashboards (anchor on the dashboard's \`<simple-id\` tag). When the user asks for a new chart/sheet, you MUST also add it to the relevant dashboard. Return empty fixes array if no safe fix is possible.`
      }]
    });
    result = safeParseJson(msg.content[0].text.trim());
  } catch (err) {
    reportProgress(issueKey, `Error: ${err.message}`, 100, 'error');
    await jiraComment(issueKey, [
      adfParagraph(adfText(`Dear ${reporter},`)),
      adfParagraph(adfText('We encountered an issue while analyzing your workbook. Our team has been notified and will investigate manually.')),
      adfParagraph(adfText('Error details: ', true), adfText(err.message)),
      adfRule(),
      adfParagraph(adfText('TabServo — Biztory Tableau AI Support', true))
    ]);
    return;
  }

  console.log(`  → Analysis: ${result.analysis}`);

  if (!result.fixes || result.fixes.length === 0) {
    reportProgress(issueKey, 'No automatic fix found — manual review needed', 100, 'warn');
    await jiraComment(issueKey, [
      adfParagraph(adfText(`Dear ${reporter},`)),
      adfParagraph(adfText('After analyzing your workbook, we were unable to apply an automatic fix for this issue. Your ticket will remain open and a human support agent will review it.')),
      adfParagraph(adfText('Our analysis: ', true), adfText(result.analysis)),
      ...(result.comment ? [adfParagraph(adfText(result.comment))] : []),
      adfRule(),
      adfParagraph(adfText('TabServo — Biztory Tableau AI Support', true))
    ]);
    return;
  }

  // Apply fixes
  reportProgress(issueKey, 'Applying fix to workbook…', 65);
  let xml = wb.twbXml;
  let applied = 0;
  const log = [];

  for (const fix of result.fixes) {
    const op     = fix.op || 'replace';
    const anchor = fix.find ? fix.find.slice(0, 80) + (fix.find.length > 80 ? '…' : '') : '';
    if (!fix.find || !xml.includes(fix.find)) { log.push(`Not found (${op}): ${anchor}`); continue; }
    if      (op === 'replace')      xml = xml.split(fix.find).join(fix.replace);
    else if (op === 'insert_after')  xml = xml.split(fix.find).join(fix.find + fix.insert);
    else if (op === 'insert_before') xml = xml.split(fix.find).join(fix.insert + fix.find);
    else if (op === 'delete')       xml = xml.split(fix.find).join('');
    else { log.push(`Unknown op: ${op}`); continue; }
    applied++;
    log.push(`Applied ${op}: ${anchor}`);
  }

  if (applied === 0) {
    reportProgress(issueKey, 'Fix could not be located in workbook XML', 100, 'warn');
    await jiraComment(issueKey, [
      adfParagraph(adfText(`Dear ${reporter},`)),
      adfParagraph(adfText('We identified a potential fix but were unable to locate the exact XML elements in your workbook to apply it. Your ticket will remain open for manual review.')),
      adfParagraph(adfText('Our analysis: ', true), adfText(result.analysis)),
      adfBulletList(log),
      adfRule(),
      adfParagraph(adfText('TabServo — Biztory Tableau AI Support', true))
    ]);
    return;
  }

  // Validate
  reportProgress(issueKey, 'Validating XML…', 75);
  const validation = validateXml(xml);
  if (!validation.valid) {
    reportProgress(issueKey, 'Fix produced invalid XML — not applied', 100, 'error');
    await jiraComment(issueKey, [
      adfParagraph(adfText(`Dear ${reporter},`)),
      adfParagraph(adfText('We generated a fix for your issue, but it produced invalid workbook XML and was not applied to protect your data. Your ticket will remain open for manual review.')),
      adfParagraph(adfText('Validation details: ', true), adfText(validation.error)),
      adfRule(),
      adfParagraph(adfText('TabServo — Biztory Tableau AI Support', true))
    ]);
    return;
  }

  // Repack
  reportProgress(issueKey, 'Repacking workbook…', 82);
  let fixedBuffer;
  try {
    fixedBuffer = repackWorkbook(xml, wb.zip, wb.twbEntryName);
  } catch (err) {
    reportProgress(issueKey, `Error repacking: ${err.message}`, 100, 'error');
    return;
  }

  // Publish
  reportProgress(issueKey, 'Publishing fixed workbook to Tableau Cloud…', 90);
  try {
    await tableauPublish(token, siteId, workbook.project.id, workbook.name, fixedBuffer);
    console.log('  → Published to Tableau Cloud');
  } catch (err) {
    reportProgress(issueKey, `Error publishing: ${err.message}`, 100, 'error');
    await jiraComment(issueKey, [
      adfParagraph(adfText(`Dear ${reporter},`)),
      adfParagraph(adfText('We successfully generated a fix for your workbook, but encountered an error while publishing it back to Tableau Cloud. Our team has been notified and will resolve this manually.')),
      adfParagraph(adfText('Error details: ', true), adfText(err.message)),
      adfRule(),
      adfParagraph(adfText('TabServo — Biztory Tableau AI Support', true))
    ]);
    return;
  }

  await jiraComment(issueKey, [
    adfParagraph(adfText(`Dear ${reporter},`)),
    adfParagraph(adfText('Great news — we have automatically resolved your issue and republished the updated workbook to Tableau Cloud.')),
    adfParagraph(adfText('What was changed: ', true), adfText(result.comment || result.analysis)),
    adfParagraph(adfText(`Changes applied (${applied}/${result.fixes.length}):`, true)),
    adfBulletList(log),
    adfParagraph(adfText('Next steps: ', true), adfText('Please reload your Tableau dashboard to review the changes. You can then accept the fix or restore the previous version directly from the TabServo extension.')),
    adfRule(),
    adfParagraph(adfText('TabServo — Biztory Tableau AI Support', true))
  ]);
  reportProgress(issueKey, 'Fix published — reload your dashboard', 100, 'ok', { hasBackup: true, issueNumber: issueKey });
  logFixResult(issueKey, true);
  console.log(`  → Done. ${issueKey} fixed.`);
}

// ---- Restore handler ----
async function handleRestore(issueNumber, siteKey) {
  const backup = backups.get(issueNumber);
  if (!backup) { console.log(`[Agent] No in-memory backup for issue #${issueNumber}`); return; }
  console.log(`[Agent] Restoring issue #${issueNumber}...`);
  try {
    const { token, siteId } = await tableauAuth(siteKey);
    await tableauPublish(token, siteId, backup.projectId, backup.name, backup.originalBuffer);
    backups.delete(issueNumber);
    console.log(`  → Restored ${backup.name}`);
  } catch (err) {
    console.log(`  → Restore error: ${err.message}`);
  }
}

// ---- Poll server for triggers and restores ----
async function pollServer() {
  const parsed = new URL(SERVER_URL);
  const mod    = parsed.protocol === 'https:' ? https : http;
  const port   = parsed.port ? parseInt(parsed.port) : (parsed.protocol === 'https:' ? 443 : 80);

  async function poll(endpoint) {
    return new Promise(resolve => {
      const r = mod.request({
        hostname: parsed.hostname, port, path: endpoint, method: 'GET'
      }, res => {
        let out = '';
        res.on('data', c => out += c);
        res.on('end', () => {
          try { resolve({ status: res.statusCode, body: JSON.parse(out) }); }
          catch { resolve({ status: res.statusCode, body: null }); }
        });
      });
      r.on('error', () => resolve({ status: 0, body: null }));
      r.end();
    });
  }

  try {
    const trigger = await poll('/api/next-trigger');
    if (trigger.status === 200 && trigger.body?.issueNumber) {
      const { issueNumber: num, tableauSite } = trigger.body;
      gh('GET', `/repos/${REPO}/issues/${num}`).then(({ body: issue }) => {
        if (issue.number && !hasFixAppliedLabel(issue) && !seen.has(issue.id)) {
          seen.add(issue.id);
          analyzeAndFix(issue, tableauSite);
        }
      }).catch(err => console.error(`[Agent] trigger error: ${err.message}`));
    }

    const jiraTrigger = await poll('/api/next-jira-trigger');
    if (jiraTrigger.status === 200 && jiraTrigger.body?.issueKey) {
      const { issueKey, tableauSite } = jiraTrigger.body;
      analyzeAndFixJira(issueKey, tableauSite).catch(err => console.error(`[Agent] Jira error: ${err.message}`));
    }

    const restore = await poll('/api/next-restore');
    if (restore.status === 200 && restore.body?.issueNumber) {
      handleRestore(restore.body.issueNumber, restore.body.tableauSite);
    }
  } catch {} // server not ready yet
}

setInterval(pollServer, 5000);
pollServer();

// ---- Start ----
console.log('\n🤖 TabServo — Tableau AI Support Agent (Cloud Mode)');
console.log(`   Repo:      ${REPO}`);
console.log(`   Tableau:   ${TABLEAU_URL} · sites: ${Object.keys(TABLEAU_SITES).join(', ')} (default: ${DEFAULT_SITE})`);
console.log(`   Server:    ${SERVER_URL}`);
console.log(`   AI:        ${ANTHROPIC_KEY.slice(0, 10)}…\n`);
