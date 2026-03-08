const express = require('express');
const cors = require('cors');
const multer = require('multer');
const XLSX = require('xlsx');
const { setupDatabase, pool } = require('./setup-db');

const app = express();
app.use(cors());
app.use(express.json({ limit: '50mb' }));

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });

const PORT = process.env.PORT || 8080;

// ================================================================
// TOOL DEFINITIONS — sent to the AI model via MCP
// ================================================================

const TOOLS = [
  {
    name: "query_revenue",
    description: "Query revenue data from the Profit & Loss statement. Returns revenue by entity, account, and time period. Use this for questions about sales, revenue, income, or top-line performance.",
    inputSchema: {
      type: "object",
      properties: {
        entity: { type: "string", description: "Business entity: 'XYZ' or 'ABC' or 'all' for both", default: "all" },
        year: { type: "integer", description: "Year to query (2022, 2023, or 2024)" },
        month: { type: "integer", description: "Month number 1-12. Omit for full year." },
        account_name: { type: "string", description: "Specific account like 'SEO Revenue - Recurring' or 'PPC Revenue'. Omit for all revenue accounts." }
      },
      required: ["entity", "year"]
    }
  },
  {
    name: "query_expenses",
    description: "Query expense data from the Profit & Loss statement. Returns expenses by entity, category, account, and time period. Use this for questions about costs, spending, overhead, or expense analysis.",
    inputSchema: {
      type: "object",
      properties: {
        entity: { type: "string", description: "Business entity: 'XYZ' or 'ABC' or 'all' for both", default: "all" },
        year: { type: "integer", description: "Year to query (2022, 2023, or 2024)" },
        month: { type: "integer", description: "Month number 1-12. Omit for full year." },
        pl_category: { type: "string", description: "P&L category like 'Direct Costs', 'Operating Expenses', 'Payroll'. Omit for all." },
        account_name: { type: "string", description: "Specific expense account. Omit for all expenses." }
      },
      required: ["entity", "year"]
    }
  },
  {
    name: "query_balance_sheet",
    description: "Query balance sheet data. Returns assets, liabilities, and equity by entity and time period.",
    inputSchema: {
      type: "object",
      properties: {
        entity: { type: "string", description: "Business entity: 'XYZ' or 'ABC' or 'all' for both", default: "all" },
        year: { type: "integer", description: "Year to query" },
        month: { type: "integer", description: "Month number 1-12. Omit for full year." },
        category: { type: "string", description: "Category: 'Assets', 'Liabilities', 'Equity'. Omit for all." },
        account_name: { type: "string", description: "Specific account. Omit for all." }
      },
      required: ["entity", "year"]
    }
  },
  {
    name: "query_cash_flow",
    description: "Query cash flow data. Returns operating, investing, and financing activities by entity and time period.",
    inputSchema: {
      type: "object",
      properties: {
        entity: { type: "string", description: "Business entity: 'XYZ' or 'ABC' or 'all' for both", default: "all" },
        year: { type: "integer", description: "Year to query" },
        month: { type: "integer", description: "Month number 1-12. Omit for full year." },
        category: { type: "string", description: "Category: 'Operating', 'Investing', 'Financing'. Omit for all." }
      },
      required: ["entity", "year"]
    }
  },
  {
    name: "query_ar_aging",
    description: "Query accounts receivable aging data. Shows outstanding receivables by aging bucket.",
    inputSchema: {
      type: "object",
      properties: {
        entity: { type: "string", description: "Business entity: 'XYZ' or 'ABC' or 'all' for both", default: "all" },
        year: { type: "integer", description: "Year to query" },
        month: { type: "integer", description: "Month number 1-12" }
      },
      required: ["entity", "year"]
    }
  },
  {
    name: "query_budget_vs_actual",
    description: "Compare budget/forecast to actual results for variance analysis.",
    inputSchema: {
      type: "object",
      properties: {
        entity: { type: "string", description: "Business entity: 'XYZ' or 'ABC' or 'all' for both", default: "all" },
        year: { type: "integer", description: "Year to query" },
        month: { type: "integer", description: "Month number 1-12. Omit for full year." },
        account_name: { type: "string", description: "Specific account. Omit for all." }
      },
      required: ["entity", "year"]
    }
  },
  {
    name: "custom_sql_query",
    description: "Run a custom read-only SQL query. Tables: profit_loss, balance_sheet, cash_flow, ar_aging, budget. Only SELECT allowed.",
    inputSchema: {
      type: "object",
      properties: {
        sql: { type: "string", description: "SQL SELECT query to run." }
      },
      required: ["sql"]
    }
  },
  {
    name: "get_database_info",
    description: "Get info about available data — entities, time periods, accounts, row counts. Use this first to understand what data exists.",
    inputSchema: {
      type: "object",
      properties: {},
      required: []
    }
  }
];

// ================================================================
// TOOL HANDLERS
// ================================================================

async function handleToolCall(toolName, args) {
  try {
    switch (toolName) {
      case "query_revenue": {
        let sql = `SELECT entity, account_name, gm_category, period_year, period_month, amount FROM profit_loss WHERE pl_category IN ('Revenue','Income','Gross Margin') AND amount != 0`;
        const params = []; let p = 1;
        if (args.entity && args.entity !== 'all') { sql += ` AND entity = $${p++}`; params.push(args.entity); }
        if (args.year) { sql += ` AND period_year = $${p++}`; params.push(args.year); }
        if (args.month) { sql += ` AND period_month = $${p++}`; params.push(args.month); }
        if (args.account_name) { sql += ` AND account_name ILIKE $${p++}`; params.push(`%${args.account_name}%`); }
        sql += ` ORDER BY entity, period_year, period_month, account_name`;
        const result = await pool.query(sql, params);
        if (result.rows.length === 0) return { content: [{ type: "text", text: "No revenue data found." }] };
        const total = result.rows.reduce((s, r) => s + parseFloat(r.amount), 0);
        return { content: [{ type: "text", text: `Revenue Results (${result.rows.length} records, Total: $${total.toLocaleString('en-US', {minimumFractionDigits:2})})\n\n${JSON.stringify(result.rows, null, 2)}` }] };
      }
      case "query_expenses": {
        let sql = `SELECT entity, pl_category, account_name, period_year, period_month, amount FROM profit_loss WHERE pl_category NOT IN ('Revenue','Income','Gross Margin') AND amount != 0`;
        const params = []; let p = 1;
        if (args.entity && args.entity !== 'all') { sql += ` AND entity = $${p++}`; params.push(args.entity); }
        if (args.year) { sql += ` AND period_year = $${p++}`; params.push(args.year); }
        if (args.month) { sql += ` AND period_month = $${p++}`; params.push(args.month); }
        if (args.pl_category) { sql += ` AND pl_category ILIKE $${p++}`; params.push(`%${args.pl_category}%`); }
        if (args.account_name) { sql += ` AND account_name ILIKE $${p++}`; params.push(`%${args.account_name}%`); }
        sql += ` ORDER BY entity, period_year, period_month, pl_category, account_name`;
        const result = await pool.query(sql, params);
        if (result.rows.length === 0) return { content: [{ type: "text", text: "No expense data found." }] };
        const total = result.rows.reduce((s, r) => s + parseFloat(r.amount), 0);
        return { content: [{ type: "text", text: `Expense Results (${result.rows.length} records, Total: $${total.toLocaleString('en-US', {minimumFractionDigits:2})})\n\n${JSON.stringify(result.rows, null, 2)}` }] };
      }
      case "query_balance_sheet": {
        let sql = `SELECT entity, category, sub_category, account_name, period_year, period_month, amount FROM balance_sheet WHERE amount != 0`;
        const params = []; let p = 1;
        if (args.entity && args.entity !== 'all') { sql += ` AND entity = $${p++}`; params.push(args.entity); }
        if (args.year) { sql += ` AND period_year = $${p++}`; params.push(args.year); }
        if (args.month) { sql += ` AND period_month = $${p++}`; params.push(args.month); }
        if (args.category) { sql += ` AND category ILIKE $${p++}`; params.push(`%${args.category}%`); }
        if (args.account_name) { sql += ` AND account_name ILIKE $${p++}`; params.push(`%${args.account_name}%`); }
        sql += ` ORDER BY entity, period_year, period_month, category, account_name`;
        const result = await pool.query(sql, params);
        if (result.rows.length === 0) return { content: [{ type: "text", text: "No balance sheet data found." }] };
        return { content: [{ type: "text", text: `Balance Sheet Results (${result.rows.length} records)\n\n${JSON.stringify(result.rows, null, 2)}` }] };
      }
      case "query_cash_flow": {
        let sql = `SELECT entity, category, account_name, period_year, period_month, amount FROM cash_flow WHERE amount != 0`;
        const params = []; let p = 1;
        if (args.entity && args.entity !== 'all') { sql += ` AND entity = $${p++}`; params.push(args.entity); }
        if (args.year) { sql += ` AND period_year = $${p++}`; params.push(args.year); }
        if (args.month) { sql += ` AND period_month = $${p++}`; params.push(args.month); }
        if (args.category) { sql += ` AND category ILIKE $${p++}`; params.push(`%${args.category}%`); }
        sql += ` ORDER BY entity, period_year, period_month, category, account_name`;
        const result = await pool.query(sql, params);
        if (result.rows.length === 0) return { content: [{ type: "text", text: "No cash flow data found." }] };
        return { content: [{ type: "text", text: `Cash Flow Results (${result.rows.length} records)\n\n${JSON.stringify(result.rows, null, 2)}` }] };
      }
      case "query_ar_aging": {
        let sql = `SELECT entity, customer_name, aging_bucket, amount, period_year, period_month FROM ar_aging WHERE amount != 0`;
        const params = []; let p = 1;
        if (args.entity && args.entity !== 'all') { sql += ` AND entity = $${p++}`; params.push(args.entity); }
        if (args.year) { sql += ` AND period_year = $${p++}`; params.push(args.year); }
        if (args.month) { sql += ` AND period_month = $${p++}`; params.push(args.month); }
        sql += ` ORDER BY entity, period_year, period_month, aging_bucket`;
        const result = await pool.query(sql, params);
        if (result.rows.length === 0) return { content: [{ type: "text", text: "No AR aging data found." }] };
        const total = result.rows.reduce((s, r) => s + parseFloat(r.amount), 0);
        return { content: [{ type: "text", text: `AR Aging Results (${result.rows.length} records, Total: $${total.toLocaleString('en-US', {minimumFractionDigits:2})})\n\n${JSON.stringify(result.rows, null, 2)}` }] };
      }
      case "query_budget_vs_actual": {
        let sql = `SELECT b.entity, b.account_name, b.period_year, b.period_month, b.amount as budget_amount, COALESCE(p.amount,0) as actual_amount, COALESCE(p.amount,0)-b.amount as variance FROM budget b LEFT JOIN profit_loss p ON b.entity=p.entity AND b.account_name=p.account_name AND b.period_year=p.period_year AND b.period_month=p.period_month AND p.data_type='actual' WHERE b.amount != 0`;
        const params = []; let p = 1;
        if (args.entity && args.entity !== 'all') { sql += ` AND b.entity = $${p++}`; params.push(args.entity); }
        if (args.year) { sql += ` AND b.period_year = $${p++}`; params.push(args.year); }
        if (args.month) { sql += ` AND b.period_month = $${p++}`; params.push(args.month); }
        if (args.account_name) { sql += ` AND b.account_name ILIKE $${p++}`; params.push(`%${args.account_name}%`); }
        sql += ` ORDER BY b.entity, b.period_year, b.period_month, b.account_name`;
        const result = await pool.query(sql, params);
        if (result.rows.length === 0) return { content: [{ type: "text", text: "No budget vs actual data found." }] };
        return { content: [{ type: "text", text: `Budget vs Actual (${result.rows.length} records)\n\n${JSON.stringify(result.rows, null, 2)}` }] };
      }
      case "custom_sql_query": {
        const sqlLower = args.sql.trim().toLowerCase();
        if (!sqlLower.startsWith('select')) return { content: [{ type: "text", text: "Only SELECT queries allowed." }], isError: true };
        const forbidden = ['insert','update','delete','drop','alter','truncate','create','grant','revoke'];
        for (const w of forbidden) { if (sqlLower.includes(w)) return { content: [{ type: "text", text: `Forbidden keyword: ${w}` }], isError: true }; }
        const result = await pool.query(args.sql);
        return { content: [{ type: "text", text: `${result.rows.length} rows returned.\n\n${JSON.stringify(result.rows, null, 2)}` }] };
      }
      case "get_database_info": {
        const tables = ['profit_loss','balance_sheet','cash_flow','ar_aging','budget'];
        const info = {};
        for (const t of tables) {
          const cnt = await pool.query(`SELECT COUNT(*) as count FROM ${t}`);
          info[t] = { row_count: parseInt(cnt.rows[0].count) };
          if (parseInt(cnt.rows[0].count) > 0) {
            const ent = await pool.query(`SELECT DISTINCT entity FROM ${t} ORDER BY entity`);
            const yrs = await pool.query(`SELECT DISTINCT period_year FROM ${t} ORDER BY period_year`);
            info[t].entities = ent.rows.map(r => r.entity);
            info[t].years = yrs.rows.map(r => r.period_year);
            if (t === 'profit_loss') {
              const cats = await pool.query(`SELECT DISTINCT pl_category FROM profit_loss WHERE pl_category IS NOT NULL ORDER BY pl_category`);
              info[t].pl_categories = cats.rows.map(r => r.pl_category);
              const accts = await pool.query(`SELECT DISTINCT account_name FROM profit_loss ORDER BY account_name LIMIT 30`);
              info[t].sample_accounts = accts.rows.map(r => r.account_name);
            }
          }
        }
        return { content: [{ type: "text", text: `Database Info:\n\n${JSON.stringify(info, null, 2)}` }] };
      }
      default: return { content: [{ type: "text", text: `Unknown tool: ${toolName}` }], isError: true };
    }
  } catch (err) {
    return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
  }
}

// ================================================================
// MCP SSE ENDPOINT
// ================================================================

app.get('/sse', (req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'Access-Control-Allow-Origin': '*' });
  const sessionId = `session_${Date.now()}_${Math.random().toString(36).substr(2,9)}`;
  res.write(`event: endpoint\ndata: /message?sessionId=${sessionId}\n\n`);
  const keepAlive = setInterval(() => res.write(`: keepalive\n\n`), 30000);
  req.on('close', () => clearInterval(keepAlive));
});

app.post('/message', async (req, res) => {
  const { method, params, id } = req.body;
  try {
    if (method === 'initialize') return res.json({ jsonrpc:"2.0", id, result: { protocolVersion:"2024-11-05", capabilities:{tools:{listChanged:false}}, serverInfo:{name:"convor-finance-mcp",version:"1.0.0"} } });
    if (method === 'tools/list') return res.json({ jsonrpc:"2.0", id, result:{tools:TOOLS} });
    if (method === 'tools/call') { const result = await handleToolCall(params.name, params.arguments||{}); return res.json({ jsonrpc:"2.0", id, result }); }
    return res.json({ jsonrpc:"2.0", id, error:{code:-32601, message:`Unknown method: ${method}`} });
  } catch (err) { return res.json({ jsonrpc:"2.0", id, error:{code:-32603, message:err.message} }); }
});

// ================================================================
// HEALTH & INFO
// ================================================================

app.get('/health', (req, res) => res.json({ status:'ok', service:'convor-finance-mcp' }));

app.get('/api/tables', async (req, res) => {
  try {
    const tables = ['profit_loss','balance_sheet','cash_flow','ar_aging','budget'];
    const counts = {};
    for (const t of tables) { const r = await pool.query(`SELECT COUNT(*) as count FROM ${t}`); counts[t] = parseInt(r.rows[0].count); }
    res.json({ status:'ok', tables:counts });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ================================================================
// UPLOAD PAGE
// ================================================================

app.get('/upload', (req, res) => {
  res.send(`<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>Convor Finance Upload</title>
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:2rem}.container{max-width:640px;width:100%}h1{font-size:1.8rem;margin-bottom:.5rem;color:#38bdf8}p.sub{color:#94a3b8;margin-bottom:2rem;font-size:.95rem}.drop-zone{border:2px dashed #334155;border-radius:12px;padding:3rem 2rem;text-align:center;cursor:pointer;transition:all .2s;background:#1e293b}.drop-zone:hover,.drop-zone.dragover{border-color:#38bdf8;background:#1e3a5f}.drop-zone .icon{font-size:3rem;margin-bottom:1rem}.drop-zone .label{font-size:1.1rem;color:#cbd5e1}.drop-zone .hint{font-size:.85rem;color:#64748b;margin-top:.5rem}input[type=file]{display:none}#status{margin-top:1.5rem;padding:1rem;border-radius:8px;display:none;font-size:.95rem;line-height:1.6;white-space:pre-wrap}#status.success{display:block;background:#064e3b;color:#6ee7b7;border:1px solid #065f46}#status.error{display:block;background:#450a0a;color:#fca5a5;border:1px solid #7f1d1d}#status.loading{display:block;background:#1e293b;color:#93c5fd;border:1px solid #1e40af}.tables{margin-top:2rem;background:#1e293b;border-radius:8px;padding:1.5rem}.tables h3{color:#38bdf8;margin-bottom:1rem;font-size:1rem}.table-row{display:flex;justify-content:space-between;padding:.4rem 0;border-bottom:1px solid #334155;font-size:.9rem}.table-row:last-child{border:none}.table-name{color:#cbd5e1}.table-count{color:#38bdf8;font-weight:600}</style></head>
<body><div class="container"><h1>Convor Finance Data Upload</h1><p class="sub">Upload your monthly financial Excel file to load it into the database.</p>
<div class="drop-zone" id="dropZone"><div class="icon">📊</div><div class="label">Drag & drop your Excel file here</div><div class="hint">or click to browse — .xlsx files only</div></div>
<input type="file" id="fileInput" accept=".xlsx,.xls"><div id="status"></div>
<div class="tables" id="tablesInfo"><h3>Current Database Status</h3><div id="tableRows">Loading...</div></div></div>
<script>
const dz=document.getElementById('dropZone'),fi=document.getElementById('fileInput'),st=document.getElementById('status');
dz.addEventListener('click',()=>fi.click());
dz.addEventListener('dragover',e=>{e.preventDefault();dz.classList.add('dragover')});
dz.addEventListener('dragleave',()=>dz.classList.remove('dragover'));
dz.addEventListener('drop',e=>{e.preventDefault();dz.classList.remove('dragover');if(e.dataTransfer.files.length)uploadFile(e.dataTransfer.files[0])});
fi.addEventListener('change',()=>{if(fi.files.length)uploadFile(fi.files[0])});
async function uploadFile(f){if(!f.name.match(/\\.xlsx?$/i)){show('error','Please upload an .xlsx file.');return}
show('loading','Uploading and processing '+f.name+'... This may take a minute.');
const fd=new FormData();fd.append('file',f);
try{const r=await fetch('/api/upload-excel',{method:'POST',body:fd});const d=await r.json();
if(d.success){show('success','Upload complete!\\n\\n'+Object.entries(d.results).map(([k,v])=>k+': '+v+' rows inserted').join('\\n')+'\\n\\nSheets processed: '+d.sheets_processed);loadTables()}
else show('error','Error: '+(d.error||'Unknown'))}catch(e){show('error','Failed: '+e.message)}}
function show(t,m){st.className=t;st.textContent=m}
async function loadTables(){try{const r=await fetch('/api/tables');const d=await r.json();
document.getElementById('tableRows').innerHTML=Object.entries(d.tables).map(([n,c])=>'<div class="table-row"><span class="table-name">'+n+'</span><span class="table-count">'+c.toLocaleString()+' rows</span></div>').join('')}
catch(e){document.getElementById('tableRows').innerHTML='<div style="color:#f87171">Error loading</div>'}}
loadTables();
</script></body></html>`);
});

// ================================================================
// EXCEL UPLOAD API
// ================================================================

app.post('/api/upload-excel', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ success:false, error:'No file uploaded' });
    const workbook = XLSX.read(req.file.buffer, { type:'buffer', cellDates:true });
    const sheetNames = workbook.SheetNames;
    console.log('Sheets found:', sheetNames);

    await pool.query('TRUNCATE TABLE profit_loss, balance_sheet, cash_flow, ar_aging, budget RESTART IDENTITY');
    const results = {};

    for (const name of sheetNames) {
      const ws = workbook.Sheets[name];
      const data = XLSX.utils.sheet_to_json(ws, { header:1, defval:null, raw:false });
      if (!data || data.length < 3) continue;

      const nl = name.toLowerCase();

      // Detect entity from sheet name
      let entity = 'Consolidated';
      if (nl.includes('xyz')) entity = 'XYZ';
      else if (nl.includes('abc')) entity = 'ABC';

      // Detect type from sheet name
      if (nl.startsWith('pl-') || nl.includes('p&l') || nl.includes('profit and loss') || nl.includes('income statement')) {
        const count = await parsePL(data, entity, name);
        if (count > 0) results[name] = count;
      } else if (nl.startsWith('bs-') || nl.includes('balance sheet')) {
        const count = await parseBS(data, entity, name);
        if (count > 0) results[name] = count;
      } else if (nl.startsWith('cf-') && nl.includes('actual')) {
        const count = await parseCF(data, entity, name);
        if (count > 0) results[name] = count;
      } else if (nl.includes('ar aging') || nl.includes('ar_aging') || nl.includes('aging slide')) {
        const count = await parseAR(data, entity, name);
        if (count > 0) results[name] = count;
      } else if (nl.includes('budget') || nl.includes('forecast')) {
        const count = await parseBudget(data, entity, name);
        if (count > 0) results[name] = count;
      } else {
        console.log(`Skipping sheet: "${name}" (type not detected)`);
      }
    }

    console.log('Import results:', results);
    res.json({ success:true, results, sheets_processed:Object.keys(results).length });
  } catch (err) {
    console.error('Upload error:', err);
    res.status(500).json({ success:false, error:err.message });
  }
});

// ================================================================
// PARSERS — matched to the exact Excel file structure
// ================================================================

// Helper: find date columns in a row (dates are datetime strings or contain month abbreviations)
function findDateColumns(row) {
  const cols = [];
  if (!row) return cols;
  for (let i = 0; i < row.length; i++) {
    const cell = row[i];
    if (!cell) continue;
    const s = String(cell);
    // Match dates like "2022-01-01" or "1/1/2022" or Date objects serialized
    const dateMatch = s.match(/(\d{4})-(\d{2})-(\d{2})/) || s.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    if (dateMatch) {
      let year, month;
      if (s.match(/(\d{4})-(\d{2})-(\d{2})/)) {
        year = parseInt(RegExp.$1); month = parseInt(RegExp.$2);
      } else {
        month = parseInt(dateMatch[1]); year = parseInt(dateMatch[3]);
      }
      if (year >= 2020 && year <= 2030 && month >= 1 && month <= 12) {
        cols.push({ col: i, year, month });
      }
    }
  }
  return cols;
}

// Helper: find the row with date columns
function findDateRow(data, maxRows) {
  for (let i = 0; i < Math.min(data.length, maxRows || 10); i++) {
    const cols = findDateColumns(data[i]);
    if (cols.length >= 3) return { rowIdx: i, dateCols: cols };
  }
  return null;
}

// P&L Parser: Col A = GM mapping, Col B = P&L mapping (Recurring/Non-recurring), Col C = account name, data starts Col D+
async function parsePL(data, entity, sheetName) {
  console.log(`Parsing P&L: "${sheetName}" entity=${entity}`);
  
  // Find the row with dates (should be row 3 = index 2)
  const dateInfo = findDateRow(data, 10);
  if (!dateInfo) { console.log(`  No date row found in ${sheetName}`); return 0; }
  
  const { rowIdx, dateCols } = dateInfo;
  console.log(`  Date row: ${rowIdx}, ${dateCols.length} month columns found`);

  let count = 0;
  let currentPLCategory = 'Revenue';

  for (let i = rowIdx + 1; i < data.length; i++) {
    const row = data[i];
    if (!row) continue;

    // In this file structure:
    // Col 0 (A) = GM category mapping (e.g., "Recurring", "Non-recurring", or GM category name)
    // Col 1 (B) = P&L mapping or recurring/non-recurring 
    // Col 2 (C) = account name (starts with spaces and account number)
    // For XYZ: col0 is empty for headers, col1 has Recurring/Non-recurring, col2 has account
    // For ABC: col0 has GM category, col1 has Recurring/Non-recurring, col2 has account

    // Find the account name — it's the last text column before numbers start
    let accountName = null;
    let gmCategory = null;
    let recurType = null;
    
    // Check columns 0, 1, 2 for text
    const c0 = row[0] ? String(row[0]).trim() : '';
    const c1 = row[1] ? String(row[1]).trim() : '';
    const c2 = row[2] ? String(row[2]).trim() : '';

    // Detect category header rows (e.g., "Revenue", "Direct Costs", "Operating Expenses")
    const headerText = (c2 || c1 || c0).toLowerCase();
    if (!c2 && !c1 && c0) {
      // Only col A has text — might be a category header
      if (headerText.includes('revenue') || headerText.includes('income')) currentPLCategory = 'Revenue';
      else if (headerText.includes('direct cost') || headerText.includes('cost of') || headerText.includes('cogs')) currentPLCategory = 'Direct Costs';
      else if (headerText.includes('gross')) currentPLCategory = 'Gross Margin';
      else if (headerText.includes('operating')) currentPLCategory = 'Operating Expenses';
      else if (headerText.includes('payroll') || headerText.includes('salary')) currentPLCategory = 'Payroll';
      else if (headerText.includes('overhead') || headerText.includes('g&a')) currentPLCategory = 'Overhead';
      else if (headerText.includes('other expense') || headerText.includes('other income')) currentPLCategory = 'Other';
      continue;
    }
    if (c2 && !hasAnyNumber(row, dateCols)) {
      // Header row with account name but no numbers
      const h = c2.toLowerCase();
      if (h.includes('revenue') || h.includes('income')) currentPLCategory = 'Revenue';
      else if (h.includes('direct cost') || h.includes('cost of')) currentPLCategory = 'Direct Costs';
      else if (h.includes('gross')) currentPLCategory = 'Gross Margin';
      else if (h.includes('operating')) currentPLCategory = 'Operating Expenses';
      else if (h.includes('payroll')) currentPLCategory = 'Payroll';
      else if (h.includes('overhead') || h.includes('g&a')) currentPLCategory = 'Overhead';
      continue;
    }

    // Data row
    if (c2) {
      accountName = c2.replace(/^\s+/, '');
      gmCategory = c0 || null;
      recurType = c1 || null;
    } else if (c1) {
      accountName = c1.replace(/^\s+/, '');
      recurType = c0 || null;
    } else if (c0) {
      accountName = c0.replace(/^\s+/, '');
    }

    if (!accountName) continue;

    // Skip total/subtotal rows
    const acctLower = accountName.toLowerCase();
    if (acctLower.startsWith('total') || acctLower.startsWith('subtotal') || acctLower.includes('net income') || acctLower.includes('ebitda') || acctLower.includes('gross profit') || acctLower.includes('gross margin') || acctLower === 'mapping to gm' || acctLower === 'mapping to p&l' || acctLower === 'profit and loss') continue;

    // Insert data for each month column
    for (const dc of dateCols) {
      const val = parseNumber(row[dc.col]);
      if (val === null || val === 0) continue;

      await pool.query(
        `INSERT INTO profit_loss (entity, gm_category, pl_category, account_name, period_year, period_month, amount, data_type) VALUES ($1,$2,$3,$4,$5,$6,$7,'actual')`,
        [entity, gmCategory, currentPLCategory, accountName, dc.year, dc.month, val]
      );
      count++;
    }
  }
  console.log(`  ${sheetName}: ${count} rows inserted`);
  return count;
}

// Balance Sheet Parser: Col A = Mapping, Col B = account name, data starts Col C+
async function parseBS(data, entity, sheetName) {
  console.log(`Parsing BS: "${sheetName}" entity=${entity}`);
  const dateInfo = findDateRow(data, 10);
  if (!dateInfo) { console.log(`  No date row found in ${sheetName}`); return 0; }
  const { rowIdx, dateCols } = dateInfo;
  console.log(`  Date row: ${rowIdx}, ${dateCols.length} month columns found`);

  let count = 0;
  let currentCategory = 'Assets';
  let currentSubCategory = '';

  for (let i = rowIdx + 1; i < data.length; i++) {
    const row = data[i];
    if (!row) continue;
    
    const c0 = row[0] ? String(row[0]).trim() : '';
    const c1 = row[1] ? String(row[1]).trim() : '';

    // Account name is in col B (index 1)
    const label = c1 || c0;
    if (!label) continue;
    const labelLower = label.toLowerCase().replace(/^\s+/,'');

    // Detect category
    if (!hasAnyNumber(row, dateCols)) {
      if (labelLower.includes('asset')) currentCategory = 'Assets';
      else if (labelLower.includes('liabilit')) currentCategory = 'Liabilities';
      else if (labelLower.includes('equity') || labelLower.includes('shareholder')) currentCategory = 'Equity';
      else if (labelLower.includes('current asset')) currentSubCategory = 'Current Assets';
      else if (labelLower.includes('fixed asset') || labelLower.includes('non-current') || labelLower.includes('long-term')) currentSubCategory = 'Non-Current';
      else if (labelLower.includes('bank account')) currentSubCategory = 'Bank Accounts';
      else if (labelLower.includes('accounts receivable')) currentSubCategory = 'Accounts Receivable';
      else if (labelLower.includes('current liabilit')) currentSubCategory = 'Current Liabilities';
      else if (labelLower.includes('long-term') || labelLower.includes('non-current liabilit')) currentSubCategory = 'Long-Term Liabilities';
      else if (label.trim().length > 2) currentSubCategory = label.trim();
      continue;
    }

    // Skip totals
    if (labelLower.startsWith('total') || labelLower.includes('balance sheet')) continue;

    const accountName = label.replace(/^\s+/, '');

    for (const dc of dateCols) {
      const val = parseNumber(row[dc.col]);
      if (val === null || val === 0) continue;

      await pool.query(
        `INSERT INTO balance_sheet (entity, category, sub_category, account_name, period_year, period_month, amount) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
        [entity, currentCategory, currentSubCategory, accountName, dc.year, dc.month, val]
      );
      count++;
    }
  }
  console.log(`  ${sheetName}: ${count} rows inserted`);
  return count;
}

// Cash Flow Parser: Col A = account name, data starts Col B+
async function parseCF(data, entity, sheetName) {
  console.log(`Parsing CF: "${sheetName}" entity=${entity}`);
  const dateInfo = findDateRow(data, 10);
  if (!dateInfo) { console.log(`  No date row found in ${sheetName}`); return 0; }
  const { rowIdx, dateCols } = dateInfo;
  console.log(`  Date row: ${rowIdx}, ${dateCols.length} month columns found`);

  let count = 0;
  let currentCategory = 'Operating';

  for (let i = rowIdx + 1; i < data.length; i++) {
    const row = data[i];
    if (!row) continue;

    const label = row[0] ? String(row[0]).trim() : '';
    if (!label) continue;
    const labelLower = label.toLowerCase();

    if (!hasAnyNumber(row, dateCols)) {
      if (labelLower.includes('operating')) currentCategory = 'Operating';
      else if (labelLower.includes('investing')) currentCategory = 'Investing';
      else if (labelLower.includes('financing')) currentCategory = 'Financing';
      continue;
    }

    if (labelLower.startsWith('total') || labelLower.startsWith('net change') || labelLower.includes('cash flow statement') || labelLower.includes('beginning') || labelLower.includes('ending')) continue;

    for (const dc of dateCols) {
      const val = parseNumber(row[dc.col]);
      if (val === null || val === 0) continue;

      await pool.query(
        `INSERT INTO cash_flow (entity, category, account_name, period_year, period_month, amount) VALUES ($1,$2,$3,$4,$5,$6)`,
        [entity, currentCategory, label, dc.year, dc.month, val]
      );
      count++;
    }
  }
  console.log(`  ${sheetName}: ${count} rows inserted`);
  return count;
}

// AR Aging Parser: dates in col A, aging buckets Current/1-30/31-60/61-90/91+ in header row
async function parseAR(data, entity, sheetName) {
  console.log(`Parsing AR: "${sheetName}"`);

  // Find header row with aging bucket labels
  let headerIdx = -1;
  let bucketCols = [];
  for (let i = 0; i < Math.min(data.length, 10); i++) {
    const row = data[i];
    if (!row) continue;
    const rowStr = row.map(c => String(c||'').toLowerCase()).join('|');
    if (rowStr.includes('current') && (rowStr.includes('1 - 30') || rowStr.includes('1-30') || rowStr.includes('gross'))) {
      headerIdx = i;
      for (let c = 0; c < row.length; c++) {
        const cell = String(row[c]||'').trim().toLowerCase();
        if (cell === 'current') bucketCols.push({ col:c, bucket:'Current' });
        else if (cell.includes('1 - 30') || cell.includes('1-30')) bucketCols.push({ col:c, bucket:'1-30 Days' });
        else if (cell.includes('31 - 60') || cell.includes('31-60')) bucketCols.push({ col:c, bucket:'31-60 Days' });
        else if (cell.includes('61 - 90') || cell.includes('61-90')) bucketCols.push({ col:c, bucket:'61-90 Days' });
        else if (cell.includes('91') || cell.includes('over 90') || cell.includes('90+')) bucketCols.push({ col:c, bucket:'91+ Days' });
      }
      break;
    }
  }

  // Also check for the sub-header row (row 3 in the file has "Current", "1 - 30", etc.)
  if (bucketCols.length === 0) {
    for (let i = 0; i < Math.min(data.length, 10); i++) {
      const row = data[i];
      if (!row) continue;
      let found = 0;
      for (let c = 0; c < row.length; c++) {
        const cell = String(row[c]||'').trim().toLowerCase();
        if (cell === 'current') { bucketCols.push({col:c, bucket:'Current'}); found++; }
        else if (cell === '1 - 30' || cell === '1-30') { bucketCols.push({col:c, bucket:'1-30 Days'}); found++; }
        else if (cell === '31 - 60' || cell === '31-60') { bucketCols.push({col:c, bucket:'31-60 Days'}); found++; }
        else if (cell === '61 - 90' || cell === '61-90') { bucketCols.push({col:c, bucket:'61-90 Days'}); found++; }
        else if (cell === '91+' || cell === '91 +') { bucketCols.push({col:c, bucket:'91+ Days'}); found++; }
      }
      if (found >= 3) { headerIdx = i; break; }
    }
  }

  if (headerIdx < 0 || bucketCols.length === 0) {
    console.log(`  No aging buckets found in ${sheetName}`);
    return 0;
  }
  console.log(`  Header row: ${headerIdx}, buckets:`, bucketCols.map(b => b.bucket));

  let count = 0;
  // Data rows: Col A has the date, then bucket columns have amounts
  for (let i = headerIdx + 1; i < data.length; i++) {
    const row = data[i];
    if (!row || !row[0]) continue;

    // Parse date from col A
    const dateStr = String(row[0]);
    const dateMatch = dateStr.match(/(\d{4})-(\d{2})-(\d{2})/) || dateStr.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    let year, month;
    if (dateMatch) {
      if (dateStr.match(/(\d{4})-(\d{2})-(\d{2})/)) { year = parseInt(RegExp.$1); month = parseInt(RegExp.$2); }
      else { month = parseInt(dateMatch[1]); year = parseInt(dateMatch[3]); }
    }
    if (!year || !month) continue;

    for (const bc of bucketCols) {
      const val = parseNumber(row[bc.col]);
      if (val === null || val === 0) continue;

      await pool.query(
        `INSERT INTO ar_aging (entity, customer_name, aging_bucket, amount, period_year, period_month) VALUES ($1,$2,$3,$4,$5,$6)`,
        ['Consolidated', `Period ${year}-${String(month).padStart(2,'0')}`, bc.bucket, val, year, month]
      );
      count++;
    }
  }
  console.log(`  ${sheetName}: ${count} rows inserted`);
  return count;
}

// Budget Parser: dates in row 5, account names in col A, data starts col B
async function parseBudget(data, entity, sheetName) {
  console.log(`Parsing Budget: "${sheetName}"`);
  const dateInfo = findDateRow(data, 10);
  if (!dateInfo) { console.log(`  No date row found in ${sheetName}`); return 0; }
  const { rowIdx, dateCols } = dateInfo;
  console.log(`  Date row: ${rowIdx}, ${dateCols.length} month columns found`);

  let count = 0;
  for (let i = rowIdx + 1; i < data.length; i++) {
    const row = data[i];
    if (!row) continue;

    const label = row[0] ? String(row[0]).trim() : '';
    if (!label) continue;
    const labelLower = label.toLowerCase();
    if (labelLower.startsWith('total') || labelLower.includes('margin %') || labelLower === 'profit and loss') continue;
    if (!hasAnyNumber(row, dateCols)) continue;

    for (const dc of dateCols) {
      const val = parseNumber(row[dc.col]);
      if (val === null || val === 0) continue;

      await pool.query(
        `INSERT INTO budget (entity, account_name, period_year, period_month, amount) VALUES ($1,$2,$3,$4,$5)`,
        ['Consolidated', label, dc.year, dc.month, val]
      );
      count++;
    }
  }
  console.log(`  ${sheetName}: ${count} rows inserted`);
  return count;
}

// Helpers
function parseNumber(val) {
  if (val === null || val === undefined || val === '') return null;
  const n = parseFloat(String(val).replace(/[,$]/g, ''));
  return isNaN(n) ? null : n;
}

function hasAnyNumber(row, dateCols) {
  for (const dc of dateCols) {
    const val = parseNumber(row[dc.col]);
    if (val !== null && val !== 0) return true;
  }
  return false;
}

// ================================================================
// INGEST API (for Make.com)
// ================================================================

app.post('/api/ingest', async (req, res) => {
  try {
    const { table, data } = req.body;
    if (!table || !data || !Array.isArray(data)) return res.status(400).json({ error:'Need { table, data:[] }' });
    const valid = ['profit_loss','balance_sheet','cash_flow','ar_aging','budget'];
    if (!valid.includes(table)) return res.status(400).json({ error:`Invalid table. Use: ${valid.join(', ')}` });
    let ins = 0;
    for (const row of data) {
      const cols = Object.keys(row); const vals = Object.values(row);
      await pool.query(`INSERT INTO ${table} (${cols.join(',')}) VALUES (${vals.map((_,i)=>`$${i+1}`).join(',')})`, vals);
      ins++;
    }
    res.json({ success:true, table, inserted:ins });
  } catch (err) { res.status(500).json({ error:err.message }); }
});

// ================================================================
// START
// ================================================================

async function start() {
  await setupDatabase();
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Convor Finance MCP Server running on port ${PORT}`);
    console.log(`  Upload page: /upload`);
    console.log(`  SSE endpoint: /sse`);
    console.log(`  Health: /health`);
  });
}
start().catch(err => { console.error('Failed to start:', err); process.exit(1); });
