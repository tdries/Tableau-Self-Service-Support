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
const TABLEAU_URL        = (e('Tableau_URL') || 'https://10ax.online.tableau.com').replace(/\/$/, '');
const TABLEAU_SITE       = e('Site') || '';
const TABLEAU_PAT_NAME   = e('TABLEAU_PAT_NAME') || 'test2';
const TABLEAU_PAT_SECRET = e('TABLEAU_PAT_SECRET') || e('test2') || '';
const SERVER_URL         = e('RAILWAY_URL') || 'http://localhost:8766';
const REPO               = 'tdries/Tableau-Self-Service-Support';

if (!GITHUB_TOKEN)       { console.error('[ERROR] GitHub token missing'); process.exit(1); }
if (!ANTHROPIC_KEY)      { console.error('[ERROR] ANTHROPIC_API_KEY missing'); process.exit(1); }
if (!TABLEAU_PAT_SECRET) { console.error('[ERROR] Tableau PAT secret missing (set TABLEAU_PAT_SECRET or test2 in .env)'); process.exit(1); }

const anthropic = new Anthropic({ apiKey: ANTHROPIC_KEY });
const seen    = new Set();
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
      'User-Agent': 'Biztory-BUDA/1.0',
      ...(data ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) } : {})
    }
  }, data);
}

// ---- Tableau REST API ----
const TABLEAU_API = `${TABLEAU_URL}/api/3.22`;

async function tableauAuth() {
  const body = JSON.stringify({
    credentials: {
      personalAccessTokenName:   TABLEAU_PAT_NAME,
      personalAccessTokenSecret: TABLEAU_PAT_SECRET,
      site: { contentUrl: TABLEAU_SITE }
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
  const boundary = `BUDABoundary${Date.now()}`;
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

  // Worksheet names — so BUDA knows what sheets already exist
  const wsNames = [...xml.matchAll(/<worksheet name='([^']+)'/g)].map(m => m[1]);
  if (wsNames.length)
    sections.push(`<!-- EXISTING WORKSHEETS -->\n${wsNames.map(n => `<worksheet name='${n}'/>`).join('\n')}`);

  // Full dashboard sections — layout tree needed to add new sheets to dashboards
  const dashMatches = [...xml.matchAll(/<dashboard\b[\s\S]*?<\/dashboard>/g)];
  if (dashMatches.length)
    sections.push(`<!-- DASHBOARDS (layout) -->\n${cap(dashMatches.map(m => m[0]).join('\n'), 10000)}`);

  return sections.join('\n\n') || '<!-- No key sections extracted -->';
}

function extractWorkbookName(issueBody) {
  const match = issueBody.match(/\*\*Workbook:\*\*\s*(.+?)(?:\n|$)/);
  return match ? match[1].trim() : null;
}

function hasFixAppliedLabel(issue) {
  return issue.labels?.some(l => l.name === 'fix-applied');
}

// ---- Core: analyze + fix ----
async function analyzeAndFix(issue) {
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
    ({ token, siteId } = await tableauAuth());
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
  reportProgress(n, 'The Biztory AI Developer Junio (BUDA) is looking at the issue…', 45);
  console.log('  → Calling Biztory AI...');
  let result;
  try {
    const msg = await anthropic.messages.create({
      model: 'claude-opus-4-6',
      max_tokens: 8096,
      system: `You are BUDA, an expert Tableau workbook engineer and automated repair agent. You edit TWB/TWBX XML files with surgical precision based on the official Tableau 2026.1 schema.

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
1. Insert \`<worksheet name='New Sheet'>\` block (with \`<simple-id uuid='...'>\`) inside \`<worksheets>\`
2. Insert a \`<zone type-v2='worksheet' name='New Sheet' id='N' x='' y='' w='' h=''>\` inside dashboard \`<zones>\`
3. Adjust sibling zone sizes so total w/h still fills the dashboard

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
2. **insert_after** — \`{ "op": "insert_after", "find": "...", "insert": "..." }\`
3. **delete** — \`{ "op": "delete", "find": "..." }\`

## RULES
- Every \`find\` string MUST appear verbatim in the workbook XML — copy it exactly
- Make the smallest possible change; never restructure elements unnecessarily
- NEVER modify \`<connection>\` attributes
- Preserve the exact quoting style (' vs ") of surrounding XML
- When fixing a calculated field, only change the \`formula='...'\` attribute value
- New worksheets MUST have a unique \`<simple-id uuid='...'>\` — generate a valid UUID
- New dashboard zones MUST have unique \`id\` values — scan existing ids first`,

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
    { "op": "replace|insert_after|delete", "find": "verbatim XML to locate", "replace": "...", "insert": "..." }
  ],
  "comment": "Human-readable summary of what was changed and what the user should do next"
}

Omit \`replace\` for delete ops, omit \`insert\` for replace ops. Return empty fixes array if no safe fix is possible.`
      }]
    });

    const text = msg.content[0].text.trim();
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('No JSON object found in response');
    result = JSON.parse(jsonMatch[0]);
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
    else if (op === 'insert_after') xml = xml.split(fix.find).join(fix.find + fix.insert);
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
    body: `✅ **Fix applied automatically by BUDA**\n\n${result.comment}\n\n**Changes (${applied}/${result.fixes.length}):**\n${log.join('\n')}\n\nThe workbook has been republished to Tableau Cloud. Reload your dashboard to see the changes.`
  });
  await gh('POST', `/repos/${REPO}/issues/${n}/labels`, { labels: ['fix-applied'] });

  reportProgress(n, 'Fix published — reload your dashboard', 100, 'ok', { hasBackup: true, issueNumber: n });
  console.log(`  → Done. Issue #${n} marked fix-applied.`);
}

// ---- Restore handler ----
async function handleRestore(issueNumber) {
  const backup = backups.get(issueNumber);
  if (!backup) { console.log(`[Agent] No in-memory backup for issue #${issueNumber}`); return; }
  console.log(`[Agent] Restoring issue #${issueNumber}...`);
  try {
    const { token, siteId } = await tableauAuth();
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
      const num = trigger.body.issueNumber;
      gh('GET', `/repos/${REPO}/issues/${num}`).then(({ body: issue }) => {
        if (issue.number && !hasFixAppliedLabel(issue) && !seen.has(issue.id)) {
          seen.add(issue.id);
          analyzeAndFix(issue);
        }
      }).catch(err => console.error(`[Agent] trigger error: ${err.message}`));
    }

    const restore = await poll('/api/next-restore');
    if (restore.status === 200 && restore.body?.issueNumber) {
      handleRestore(restore.body.issueNumber);
    }
  } catch {} // server not ready yet
}

setInterval(pollServer, 5000);
pollServer();

// ---- Start ----
console.log('\n🤖 BUDA — Biztory AI Developer Junio (Cloud Mode)');
console.log(`   Repo:      ${REPO}`);
console.log(`   Tableau:   ${TABLEAU_URL} · site: ${TABLEAU_SITE}`);
console.log(`   PAT:       ${TABLEAU_PAT_NAME}`);
console.log(`   Server:    ${SERVER_URL}`);
console.log(`   AI:        ${ANTHROPIC_KEY.slice(0, 10)}…\n`);
