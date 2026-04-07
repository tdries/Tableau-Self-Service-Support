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

// ---- Smart XML matching ----
// When exact find fails, try to match by tag name + key attributes (id, name, column, etc.)
function smartFind(xml, findStr) {
  // Exact match — always try first
  if (xml.includes(findStr)) return findStr;

  // Extract tag name and key attributes from the find string
  const tagMatch = findStr.match(/^<(\w+)\s/);
  if (!tagMatch) return null;
  const tag = tagMatch[1];

  // Extract identifying attributes (id, name, column are the most reliable)
  const attrs = {};
  for (const [, key, val] of findStr.matchAll(/\b(id|name|column|caption|uuid)='([^']+)'/g)) {
    attrs[key] = val;
  }
  if (Object.keys(attrs).length === 0) return null;

  // Find all elements of this tag in the XML that match these attributes
  // Use a regex that captures the full element (including children for non-self-closing)
  const selfClosingRegex = new RegExp(`<${tag}\\b[^>]*${Object.entries(attrs).map(([k, v]) =>
    `${k}='${v.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}'`).join('[^>]*')}[^>]*/>`);
  const selfMatch = xml.match(selfClosingRegex);
  if (selfMatch) return selfMatch[0];

  // For elements with children, match the opening tag and find its closing tag
  const openRegex = new RegExp(`<${tag}\\b[^>]*${Object.entries(attrs).map(([k, v]) =>
    `${k}='${v.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}'`).join('[^>]*')}[^>]*>`);
  const openMatch = xml.match(openRegex);
  if (!openMatch) return null;

  // Find matching closing tag by counting nesting depth
  const startIdx = xml.indexOf(openMatch[0]);
  let depth = 1;
  let pos = startIdx + openMatch[0].length;
  const openTag = `<${tag}`;
  const closeTag = `</${tag}>`;
  while (depth > 0 && pos < xml.length) {
    const nextOpen  = xml.indexOf(openTag, pos);
    const nextClose = xml.indexOf(closeTag, pos);
    if (nextClose === -1) break;
    if (nextOpen !== -1 && nextOpen < nextClose) {
      depth++;
      pos = nextOpen + openTag.length;
    } else {
      depth--;
      pos = nextClose + closeTag.length;
    }
  }
  if (depth === 0) return xml.slice(startIdx, pos);
  return null;
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

## CREATING A NEW WORKSHEET
A COMPLETE, valid worksheet requires ALL of the following elements. Copy an existing worksheet from the XML as a template and modify it.

\`\`\`xml
<worksheet name='New Sheet Name'>
  <table>
    <view>
      <datasources>
        <datasource caption='Human Name' name='datasource.internal.id' />
      </datasources>
      <datasource-dependencies datasource='datasource.internal.id'>
        <!-- Declare EVERY column used in rows/cols/encodings -->
        <column datatype='real' name='[Sales]' role='measure' type='quantitative' />
        <column datatype='date' name='[Order Date]' role='dimension' type='ordinal' />
        <!-- Column-instances: one per field on shelves. Naming: [derivation:ColumnName:typeKey] -->
        <!-- typeKey: nk=nominal, ok=ordinal, qk=quantitative -->
        <!-- derivation: none (raw), sum/avg/min/max (agg), Year/Month/Day (date), User (custom) -->
        <column-instance column='[Sales]' derivation='Sum' name='[sum:Sales:qk]' pivot='key' type='quantitative' />
        <column-instance column='[Order Date]' derivation='Year' name='[yr:Order Date:ok]' pivot='key' type='ordinal' />
      </datasource-dependencies>
      <aggregation value='true' />
    </view>
    <style />
    <panes>
      <pane selection-relaxation-option='selection-relaxation-allow'>
        <view>
          <breakdown value='auto' />
        </view>
        <mark class='Line' />
        <!-- Optional encodings: <encodings><color column='[...]' /><size column='[...]' /></encodings> -->
      </pane>
    </panes>
    <!-- Rows = vertical axis, Cols = horizontal axis -->
    <!-- Reference column-instances using FULL path: [datasource.internal.id].[instance-name] -->
    <rows>[datasource.internal.id].[sum:Sales:qk]</rows>
    <cols>[datasource.internal.id].[yr:Order Date:ok]</cols>
  </table>
  <simple-id uuid='{XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX}' />
</worksheet>
\`\`\`

Valid mark classes: \`Automatic\`, \`Bar\`, \`Line\`, \`Area\`, \`Circle\`, \`Square\`, \`Shape\`, \`Text\`, \`Pie\`, \`GanttBar\`, \`Polygon\`, \`PolyLine\`, \`Heatmap\`

CRITICAL rules for new worksheets:
- Copy the EXACT \`datasource name='...'\` from an existing worksheet in the XML — do NOT invent datasource names
- Column \`name\` attributes MUST match fields that exist in the datasource (visible in other worksheets)
- Column-instance \`name\` follows strict pattern: \`[derivation:ColumnName:typeKey]\`
- \`<simple-id>\` UUID must be unique — generate a new one
- \`<rows>\` and \`<cols>\` reference column-instances with full datasource path

## DASHBOARD STRUCTURE AND ZONE HIERARCHY
\`\`\`xml
<dashboard name='Dashboard Name'>
  <size minheight='620' minwidth='1000' />
  <zones>
    <!-- Level 1: Root layout zone — ALWAYS id-based, w=100000 h=100000 (normalized units) -->
    <zone id='2' type-v2='layout-basic' w='100000' h='100000' x='0' y='0'>
      <!-- Level 2: Flow container — splits horizontally or vertically -->
      <zone id='39' param='horz' type-v2='layout-flow' w='100000' h='100000' x='0' y='0'>
        <!-- Level 3+: Worksheet zones and other content -->
        <zone id='1' name='Sheet Name' w='50000' h='100000' x='0' y='0' />
        <zone id='8' name='Other Sheet' w='50000' h='100000' x='50000' y='0' />
      </zone>
    </zone>
  </zones>
  <simple-id uuid='{...}' />
</dashboard>
\`\`\`

Zone rules (violating these causes publish failure):
- \`x\`, \`y\`, \`w\`, \`h\`, \`id\` are ALL required on every zone (XSD: use="required")
- Coordinates use normalized units: 0-100000 scale within parent zone
- Zone \`id\` must be unique across the ENTIRE workbook — scan ALL dashboard ids
- Worksheet zones: \`name\` must EXACTLY match the \`<worksheet name='...'>\` attribute. No \`type-v2\` needed.
- Container zones: \`type-v2='layout-basic'\` or \`type-v2='layout-flow'\` with \`param='horz'|'vert'\`
- Other zone types: \`title\`, \`text\`, \`filter\`, \`paramctrl\`, \`color\`, \`empty\`, \`web\`, \`bitmap\`, \`map\`, \`highlighter\`

## ADDING A NEW WORKSHEET + DASHBOARD ZONE

**Step 1 — Create the worksheet:**
- Use \`"op": "insert_after"\` with \`"find"\` = the last \`</worksheet>\` in the XML (or use the closing tag of a specific existing worksheet)
- The \`"insert"\` must be a COMPLETE worksheet block modeled on an existing one
- Copy \`datasource name\` and column definitions from existing worksheets — never invent them

**Step 2 — Add to dashboard (use the dedicated operation):**
\`\`\`json
{ "op": "add_dashboard_zone", "dashboard": "Overview", "zone": "<zone h='30000' id='99' name='New Sheet' w='74000' x='25000' y='70000' />" }
\`\`\`
- This automatically inserts the zone as a sibling of existing worksheet zones in the named dashboard
- The zone \`name\` MUST match the new worksheet's name exactly
- Pick a unique \`id\` (scan all existing zone ids across all dashboards)
- Use position/size values that fit within the existing layout (examine sibling zones for reference)

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
5. **add_dashboard_zone** — \`{ "op": "add_dashboard_zone", "dashboard": "Dashboard Name", "zone": "<zone .../>..." }\` — automatically inserts the zone XML into the named dashboard's \`<zones>\` section. Use this INSTEAD of insert_before/insert_after when adding sheets to dashboards. The zone will be inserted as a child of the outermost layout zone.

## RULES
- Keep \`find\` strings as SHORT as possible — match the minimum unique fragment needed. For attribute changes (e.g. mark class, formula), just match the single element tag, not surrounding context
- If the same tag appears in multiple worksheets and you need to target one, include the parent \`<worksheet name='...'>\` opening tag as a separate replace to narrow scope — or accept that the change applies to all matching elements
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
    const op = fix.op || 'replace';

    // Special op: add_dashboard_zone — programmatic zone insertion
    if (op === 'add_dashboard_zone') {
      const dashName = fix.dashboard;
      const zoneXml  = fix.zone;
      if (!dashName || !zoneXml) { log.push(`⚠️ add_dashboard_zone: missing dashboard or zone`); continue; }
      // Extract the dashboard section
      const dashRegex = new RegExp(`<dashboard[^>]*name='${dashName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}'[\\s\\S]*?</dashboard>`);
      const dashMatch = xml.match(dashRegex);
      if (!dashMatch) { log.push(`⚠️ Dashboard '${dashName}' not found`); continue; }
      const dashXml = dashMatch[0];
      // Find the last worksheet zone's closing </zone> inside the dashboard
      // Worksheet zones have a name= attribute and no type-v2= (or type-v2='worksheet')
      const wsZoneCloses = [...dashXml.matchAll(/<zone[^>]+name='[^']+(?:(?!type-v2)|[^>]*type-v2='worksheet')[^>]*>[\s\S]*?<\/zone>/g)];
      let updatedDash;
      if (wsZoneCloses.length) {
        // Insert after the last worksheet zone
        const lastWsZone = wsZoneCloses[wsZoneCloses.length - 1][0];
        updatedDash = dashXml.replace(lastWsZone, lastWsZone + '\n              ' + zoneXml);
      } else {
        // Fallback: insert before the first </zone> closing inside <zones>
        const zonesMatch = dashXml.match(/<zones>[\s\S]*?<\/zones>/);
        if (!zonesMatch) { log.push(`⚠️ No <zones> found in dashboard '${dashName}'`); continue; }
        const zonesXml = zonesMatch[0];
        // Find the last </zone> before </zones>
        const lastCloseIdx = zonesXml.lastIndexOf('</zone>', zonesXml.lastIndexOf('</zones>'));
        if (lastCloseIdx === -1) { log.push(`⚠️ No zones to anchor in dashboard '${dashName}'`); continue; }
        const updatedZones = zonesXml.slice(0, lastCloseIdx) + zoneXml + '\n            ' + zonesXml.slice(lastCloseIdx);
        updatedDash = dashXml.replace(zonesXml, updatedZones);
      }
      xml = xml.replace(dashXml, updatedDash);
      applied++;
      log.push(`✅ Added zone to dashboard '${dashName}'`);
      console.log(`  → Added zone to dashboard '${dashName}'`);
      continue;
    }

    if (!fix.find) { log.push(`⚠️ Missing find string for ${op}`); continue; }
    const actual = smartFind(xml, fix.find);
    const anchor = fix.find.slice(0, 80) + (fix.find.length > 80 ? '…' : '');

    if (!actual) {
      log.push(`⚠️ Not found (${op}): \`${anchor}\``);
      console.log(`  → Could not locate fix string for op: ${op}`);
      continue;
    }

    if (actual !== fix.find) console.log(`  → Smart-matched by attributes (exact match failed)`);

    if      (op === 'replace')       xml = xml.split(actual).join(fix.replace || '');
    else if (op === 'insert_after')  xml = xml.split(actual).join(actual + fix.insert);
    else if (op === 'insert_before') xml = xml.split(actual).join(fix.insert + actual);
    else if (op === 'delete')        xml = xml.split(actual).join('');
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
    // Re-authenticate to get a fresh token — the original may have gone stale during analysis
    ({ token, siteId } = await tableauAuth(siteKey));
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
    const op = fix.op || 'replace';

    if (op === 'add_dashboard_zone') {
      const dashName = fix.dashboard;
      const zoneXml  = fix.zone;
      if (!dashName || !zoneXml) { log.push(`add_dashboard_zone: missing dashboard or zone`); continue; }
      const dashRegex = new RegExp(`<dashboard[^>]*name='${dashName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}'[\\s\\S]*?</dashboard>`);
      const dashMatch = xml.match(dashRegex);
      if (!dashMatch) { log.push(`Dashboard '${dashName}' not found`); continue; }
      const dashXml = dashMatch[0];
      const wsZoneCloses = [...dashXml.matchAll(/<zone[^>]+name='[^']+(?:(?!type-v2)|[^>]*type-v2='worksheet')[^>]*>[\s\S]*?<\/zone>/g)];
      let updatedDash;
      if (wsZoneCloses.length) {
        const lastWsZone = wsZoneCloses[wsZoneCloses.length - 1][0];
        updatedDash = dashXml.replace(lastWsZone, lastWsZone + '\n              ' + zoneXml);
      } else {
        const zonesMatch = dashXml.match(/<zones>[\s\S]*?<\/zones>/);
        if (!zonesMatch) { log.push(`No <zones> found in dashboard '${dashName}'`); continue; }
        const zonesXml = zonesMatch[0];
        const lastCloseIdx = zonesXml.lastIndexOf('</zone>', zonesXml.lastIndexOf('</zones>'));
        if (lastCloseIdx === -1) { log.push(`No zones to anchor in dashboard '${dashName}'`); continue; }
        const updatedZones = zonesXml.slice(0, lastCloseIdx) + zoneXml + '\n            ' + zonesXml.slice(lastCloseIdx);
        updatedDash = dashXml.replace(zonesXml, updatedZones);
      }
      xml = xml.replace(dashXml, updatedDash);
      applied++;
      log.push(`Added zone to dashboard '${dashName}'`);
      continue;
    }

    if (!fix.find) { log.push(`Missing find string for ${op}`); continue; }
    const actual = smartFind(xml, fix.find);
    const anchor = fix.find.slice(0, 80) + (fix.find.length > 80 ? '…' : '');

    if (!actual) { log.push(`Not found (${op}): ${anchor}`); continue; }
    if (actual !== fix.find) console.log(`  → Smart-matched by attributes (exact match failed)`);

    if      (op === 'replace')       xml = xml.split(actual).join(fix.replace || '');
    else if (op === 'insert_after')  xml = xml.split(actual).join(actual + fix.insert);
    else if (op === 'insert_before') xml = xml.split(actual).join(fix.insert + actual);
    else if (op === 'delete')        xml = xml.split(actual).join('');
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
    // Re-authenticate to get a fresh token — the original may have gone stale during analysis
    ({ token, siteId } = await tableauAuth(siteKey));
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
  const restoreKey = `restore-${issueNumber}`;
  const backup = backups.get(issueNumber);
  if (!backup) {
    reportProgress(restoreKey, 'No backup found for this issue', 100, 'error');
    console.log(`[Agent] No in-memory backup for issue #${issueNumber}`);
    return;
  }
  console.log(`[Agent] Restoring issue #${issueNumber}...`);
  reportProgress(restoreKey, 'Connecting to Tableau Cloud…', 15, 'info');
  try {
    const { token, siteId } = await tableauAuth(siteKey);
    reportProgress(restoreKey, 'Publishing original workbook…', 50, 'info');
    await tableauPublish(token, siteId, backup.projectId, backup.name, backup.originalBuffer);
    backups.delete(issueNumber);
    reportProgress(restoreKey, 'Workbook restored — you can now refresh your dashboard', 100, 'ok');
    console.log(`  → Restored ${backup.name}`);
  } catch (err) {
    reportProgress(restoreKey, `Restore failed: ${err.message}`, 100, 'error');
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
