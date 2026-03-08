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
    description: "Query balance sheet data. Returns assets, liabilities, and equity by entity and time period. Use this for questions about financial position, assets, debt, equity, or working capital.",
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
    description: "Query cash flow data. Returns operating, investing, and financing activities by entity and time period. Use this for questions about cash position, cash burn, or liquidity.",
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
    description: "Query accounts receivable aging data. Shows outstanding receivables by aging bucket (current, 1-30 days, 31-60 days, 61-90 days, 91+ days). Use this for questions about collections, outstanding invoices, or receivables.",
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
    description: "Compare budget/forecast to actual results. Use this for variance analysis, budget tracking, or forecast accuracy questions.",
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
    description: "Run a custom read-only SQL query against the financial database. Use this for complex analysis that the other tools can't handle. Tables: profit_loss, balance_sheet, cash_flow, ar_aging, budget. Only SELECT queries allowed.",
    inputSchema: {
      type: "object",
      properties: {
        sql: { type: "string", description: "SQL SELECT query to run. Must be read-only." }
      },
      required: ["sql"]
    }
  },
  {
    name: "get_database_info",
    description: "Get information about what data is available in the database — entities, time periods, account names, and row counts. Use this first to understand what data exists before querying.",
    inputSchema: {
      type: "object",
      properties: {},
      required: []
    }
  }
];

// ================================================================
// TOOL HANDLERS — execute the actual SQL queries
// ================================================================

async function handleToolCall(toolName, args) {
  try {
    switch (toolName) {

      case "query_revenue": {
        let sql = `SELECT entity, account_name, gm_category, period_year, period_month, amount 
                    FROM profit_loss WHERE pl_category IN ('Revenue', 'Gross Margin') 
                    AND amount != 0`;
        const params = [];
        let paramIdx = 1;
        if (args.entity && args.entity !== 'all') { sql += ` AND entity = $${paramIdx++}`; params.push(args.entity); }
        if (args.year) { sql += ` AND period_year = $${paramIdx++}`; params.push(args.year); }
        if (args.month) { sql += ` AND period_month = $${paramIdx++}`; params.push(args.month); }
        if (args.account_name) { sql += ` AND account_name ILIKE $${paramIdx++}`; params.push(`%${args.account_name}%`); }
        sql += ` ORDER BY entity, period_year, period_month, gm_category, account_name`;
        const result = await pool.query(sql, params);
        if (result.rows.length === 0) return { content: [{ type: "text", text: "No revenue data found for the specified criteria." }] };
        const total = result.rows.reduce((sum, r) => sum + parseFloat(r.amount), 0);
        return { content: [{ type: "text", text: `Revenue Query Results (${result.rows.length} records, Total: $${total.toLocaleString('en-US', {minimumFractionDigits: 2})})\n\n${JSON.stringify(result.rows, null, 2)}` }] };
      }

      case "query_expenses": {
        let sql = `SELECT entity, pl_category, account_name, period_year, period_month, amount 
                    FROM profit_loss WHERE pl_category NOT IN ('Revenue', 'Gross Margin') 
                    AND amount != 0`;
        const params = [];
        let paramIdx = 1;
        if (args.entity && args.entity !== 'all') { sql += ` AND entity = $${paramIdx++}`; params.push(args.entity); }
        if (args.year) { sql += ` AND period_year = $${paramIdx++}`; params.push(args.year); }
        if (args.month) { sql += ` AND period_month = $${paramIdx++}`; params.push(args.month); }
        if (args.pl_category) { sql += ` AND pl_category ILIKE $${paramIdx++}`; params.push(`%${args.pl_category}%`); }
        if (args.account_name) { sql += ` AND account_name ILIKE $${paramIdx++}`; params.push(`%${args.account_name}%`); }
        sql += ` ORDER BY entity, period_year, period_month, pl_category, account_name`;
        const result = await pool.query(sql, params);
        if (result.rows.length === 0) return { content: [{ type: "text", text: "No expense data found for the specified criteria." }] };
        const total = result.rows.reduce((sum, r) => sum + parseFloat(r.amount), 0);
        return { content: [{ type: "text", text: `Expense Query Results (${result.rows.length} records, Total: $${total.toLocaleString('en-US', {minimumFractionDigits: 2})})\n\n${JSON.stringify(result.rows, null, 2)}` }] };
      }

      case "query_balance_sheet": {
        let sql = `SELECT entity, category, sub_category, account_name, period_year, period_month, amount 
                    FROM balance_sheet WHERE amount != 0`;
        const params = [];
        let paramIdx = 1;
        if (args.entity && args.entity !== 'all') { sql += ` AND entity = $${paramIdx++}`; params.push(args.entity); }
        if (args.year) { sql += ` AND period_year = $${paramIdx++}`; params.push(args.year); }
        if (args.month) { sql += ` AND period_month = $${paramIdx++}`; params.push(args.month); }
        if (args.category) { sql += ` AND category ILIKE $${paramIdx++}`; params.push(`%${args.category}%`); }
        if (args.account_name) { sql += ` AND account_name ILIKE $${paramIdx++}`; params.push(`%${args.account_name}%`); }
        sql += ` ORDER BY entity, period_year, period_month, category, sub_category, account_name`;
        const result = await pool.query(sql, params);
        if (result.rows.length === 0) return { content: [{ type: "text", text: "No balance sheet data found for the specified criteria." }] };
        return { content: [{ type: "text", text: `Balance Sheet Results (${result.rows.length} records)\n\n${JSON.stringify(result.rows, null, 2)}` }] };
      }

      case "query_cash_flow": {
        let sql = `SELECT entity, category, account_name, period_year, period_month, amount 
                    FROM cash_flow WHERE amount != 0`;
        const params = [];
        let paramIdx = 1;
        if (args.entity && args.entity !== 'all') { sql += ` AND entity = $${paramIdx++}`; params.push(args.entity); }
        if (args.year) { sql += ` AND period_year = $${paramIdx++}`; params.push(args.year); }
        if (args.month) { sql += ` AND period_month = $${paramIdx++}`; params.push(args.month); }
        if (args.category) { sql += ` AND category ILIKE $${paramIdx++}`; params.push(`%${args.category}%`); }
        sql += ` ORDER BY entity, period_year, period_month, category, account_name`;
        const result = await pool.query(sql, params);
        if (result.rows.length === 0) return { content: [{ type: "text", text: "No cash flow data found for the specified criteria." }] };
        const total = result.rows.reduce((sum, r) => sum + parseFloat(r.amount), 0);
        return { content: [{ type: "text", text: `Cash Flow Results (${result.rows.length} records, Net: $${total.toLocaleString('en-US', {minimumFractionDigits: 2})})\n\n${JSON.stringify(result.rows, null, 2)}` }] };
      }

      case "query_ar_aging": {
        let sql = `SELECT entity, customer_name, aging_bucket, amount, period_year, period_month 
                    FROM ar_aging WHERE amount != 0`;
        const params = [];
        let paramIdx = 1;
        if (args.entity && args.entity !== 'all') { sql += ` AND entity = $${paramIdx++}`; params.push(args.entity); }
        if (args.year) { sql += ` AND period_year = $${paramIdx++}`; params.push(args.year); }
        if (args.month) { sql += ` AND period_month = $${paramIdx++}`; params.push(args.month); }
        sql += ` ORDER BY entity, period_year, period_month, aging_bucket, customer_name`;
        const result = await pool.query(sql, params);
        if (result.rows.length === 0) return { content: [{ type: "text", text: "No AR aging data found for the specified criteria." }] };
        const total = result.rows.reduce((sum, r) => sum + parseFloat(r.amount), 0);
        return { content: [{ type: "text", text: `AR Aging Results (${result.rows.length} records, Total Outstanding: $${total.toLocaleString('en-US', {minimumFractionDigits: 2})})\n\n${JSON.stringify(result.rows, null, 2)}` }] };
      }

      case "query_budget_vs_actual": {
        let sql = `SELECT b.entity, b.account_name, b.period_year, b.period_month,
                    b.amount as budget_amount,
                    COALESCE(p.amount, 0) as actual_amount,
                    COALESCE(p.amount, 0) - b.amount as variance,
                    CASE WHEN b.amount != 0 THEN ROUND(((COALESCE(p.amount, 0) - b.amount) / ABS(b.amount)) * 100, 2) ELSE 0 END as variance_pct
                    FROM budget b
                    LEFT JOIN profit_loss p ON b.entity = p.entity AND b.account_name = p.account_name 
                      AND b.period_year = p.period_year AND b.period_month = p.period_month AND p.data_type = 'actual'
                    WHERE b.amount != 0`;
        const params = [];
        let paramIdx = 1;
        if (args.entity && args.entity !== 'all') { sql += ` AND b.entity = $${paramIdx++}`; params.push(args.entity); }
        if (args.year) { sql += ` AND b.period_year = $${paramIdx++}`; params.push(args.year); }
        if (args.month) { sql += ` AND b.period_month = $${paramIdx++}`; params.push(args.month); }
        if (args.account_name) { sql += ` AND b.account_name ILIKE $${paramIdx++}`; params.push(`%${args.account_name}%`); }
        sql += ` ORDER BY b.entity, b.period_year, b.period_month, b.account_name`;
        const result = await pool.query(sql, params);
        if (result.rows.length === 0) return { content: [{ type: "text", text: "No budget vs actual data found for the specified criteria." }] };
        return { content: [{ type: "text", text: `Budget vs Actual Results (${result.rows.length} records)\n\n${JSON.stringify(result.rows, null, 2)}` }] };
      }

      case "custom_sql_query": {
        const sqlLower = args.sql.trim().toLowerCase();
        if (!sqlLower.startsWith('select')) {
          return { content: [{ type: "text", text: "Error: Only SELECT queries are allowed. No INSERT, UPDATE, DELETE, DROP, or ALTER." }], isError: true };
        }
        const forbidden = ['insert', 'update', 'delete', 'drop', 'alter', 'truncate', 'create', 'grant', 'revoke'];
        for (const word of forbidden) {
          if (sqlLower.includes(word)) {
            return { content: [{ type: "text", text: `Error: Query contains forbidden keyword '${word}'. Only read-only SELECT queries allowed.` }], isError: true };
          }
        }
        const result = await pool.query(args.sql);
        return { content: [{ type: "text", text: `Query returned ${result.rows.length} rows.\n\n${JSON.stringify(result.rows, null, 2)}` }] };
      }

      case "get_database_info": {
        const tables = ['profit_loss', 'balance_sheet', 'cash_flow', 'ar_aging', 'budget'];
        const info = {};
        for (const table of tables) {
          const countRes = await pool.query(`SELECT COUNT(*) as count FROM ${table}`);
          const count = parseInt(countRes.rows[0].count);
          info[table] = { row_count: count };
          if (count > 0) {
            if (table === 'profit_loss' || table === 'budget') {
              const entities = await pool.query(`SELECT DISTINCT entity FROM ${table} ORDER BY entity`);
              const years = await pool.query(`SELECT DISTINCT period_year FROM ${table} ORDER BY period_year`);
              const accounts = await pool.query(`SELECT DISTINCT account_name FROM ${table} ORDER BY account_name LIMIT 30`);
              info[table].entities = entities.rows.map(r => r.entity);
              info[table].years = years.rows.map(r => r.period_year);
              info[table].sample_accounts = accounts.rows.map(r => r.account_name);
            }
            if (table === 'profit_loss') {
              const categories = await pool.query(`SELECT DISTINCT pl_category FROM profit_loss WHERE pl_category IS NOT NULL ORDER BY pl_category`);
              info[table].pl_categories = categories.rows.map(r => r.pl_category);
            }
          }
        }
        return { content: [{ type: "text", text: `Database Info:\n\n${JSON.stringify(info, null, 2)}` }] };
      }

      default:
        return { content: [{ type: "text", text: `Unknown tool: ${toolName}` }], isError: true };
    }
  } catch (err) {
    return { content: [{ type: "text", text: `Error executing ${toolName}: ${err.message}` }], isError: true };
  }
}

// ================================================================
// SSE/MCP ENDPOINT — LibreChat connects here
// ================================================================

app.get('/sse', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*'
  });

  const sessionId = `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

  // Send endpoint info
  res.write(`event: endpoint\ndata: /message?sessionId=${sessionId}\n\n`);

  // Keep alive
  const keepAlive = setInterval(() => {
    res.write(`: keepalive\n\n`);
  }, 30000);

  req.on('close', () => {
    clearInterval(keepAlive);
  });
});

app.post('/message', async (req, res) => {
  const { method, params, id } = req.body;

  try {
    if (method === 'initialize') {
      return res.json({
        jsonrpc: "2.0",
        id,
        result: {
          protocolVersion: "2024-11-05",
          capabilities: { tools: { listChanged: false } },
          serverInfo: { name: "convor-finance-mcp", version: "1.0.0" }
        }
      });
    }

    if (method === 'tools/list') {
      return res.json({
        jsonrpc: "2.0",
        id,
        result: { tools: TOOLS }
      });
    }

    if (method === 'tools/call') {
      const { name, arguments: args } = params;
      const result = await handleToolCall(name, args || {});
      return res.json({
        jsonrpc: "2.0",
        id,
        result
      });
    }

    return res.json({
      jsonrpc: "2.0",
      id,
      error: { code: -32601, message: `Method not found: ${method}` }
    });
  } catch (err) {
    console.error('MCP message error:', err);
    return res.json({
      jsonrpc: "2.0",
      id,
      error: { code: -32603, message: err.message }
    });
  }
});

// ================================================================
// HEALTH & INFO ENDPOINTS
// ================================================================

app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'convor-finance-mcp', timestamp: new Date().toISOString() });
});

app.get('/api/tables', async (req, res) => {
  try {
    const tables = ['profit_loss', 'balance_sheet', 'cash_flow', 'ar_aging', 'budget'];
    const counts = {};
    for (const t of tables) {
      const r = await pool.query(`SELECT COUNT(*) as count FROM ${t}`);
      counts[t] = parseInt(r.rows[0].count);
    }
    res.json({ status: 'ok', tables: counts });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ================================================================
// EXCEL UPLOAD PAGE — drag and drop your financial Excel file
// ================================================================

app.get('/upload', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Convor Finance — Upload Financial Data</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 2rem; }
    .container { max-width: 640px; width: 100%; }
    h1 { font-size: 1.8rem; margin-bottom: 0.5rem; color: #38bdf8; }
    p.sub { color: #94a3b8; margin-bottom: 2rem; font-size: 0.95rem; }
    .drop-zone { border: 2px dashed #334155; border-radius: 12px; padding: 3rem 2rem; text-align: center; cursor: pointer; transition: all 0.2s; background: #1e293b; }
    .drop-zone:hover, .drop-zone.dragover { border-color: #38bdf8; background: #1e3a5f; }
    .drop-zone .icon { font-size: 3rem; margin-bottom: 1rem; }
    .drop-zone .label { font-size: 1.1rem; color: #cbd5e1; }
    .drop-zone .hint { font-size: 0.85rem; color: #64748b; margin-top: 0.5rem; }
    input[type=file] { display: none; }
    #status { margin-top: 1.5rem; padding: 1rem; border-radius: 8px; display: none; font-size: 0.95rem; line-height: 1.6; }
    #status.success { display: block; background: #064e3b; color: #6ee7b7; border: 1px solid #065f46; }
    #status.error { display: block; background: #450a0a; color: #fca5a5; border: 1px solid #7f1d1d; }
    #status.loading { display: block; background: #1e293b; color: #93c5fd; border: 1px solid #1e40af; }
    .tables { margin-top: 2rem; background: #1e293b; border-radius: 8px; padding: 1.5rem; }
    .tables h3 { color: #38bdf8; margin-bottom: 1rem; font-size: 1rem; }
    .table-row { display: flex; justify-content: space-between; padding: 0.4rem 0; border-bottom: 1px solid #334155; font-size: 0.9rem; }
    .table-row:last-child { border: none; }
    .table-name { color: #cbd5e1; } .table-count { color: #38bdf8; font-weight: 600; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Convor Finance Data Upload</h1>
    <p class="sub">Upload your monthly financial Excel file to load it into the database.</p>

    <div class="drop-zone" id="dropZone">
      <div class="icon">📊</div>
      <div class="label">Drag & drop your Excel file here</div>
      <div class="hint">or click to browse — .xlsx files only</div>
    </div>
    <input type="file" id="fileInput" accept=".xlsx,.xls">

    <div id="status"></div>

    <div class="tables" id="tablesInfo">
      <h3>Current Database Status</h3>
      <div id="tableRows">Loading...</div>
    </div>
  </div>

  <script>
    const dropZone = document.getElementById('dropZone');
    const fileInput = document.getElementById('fileInput');
    const status = document.getElementById('status');

    dropZone.addEventListener('click', () => fileInput.click());
    dropZone.addEventListener('dragover', (e) => { e.preventDefault(); dropZone.classList.add('dragover'); });
    dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));
    dropZone.addEventListener('drop', (e) => { e.preventDefault(); dropZone.classList.remove('dragover'); if (e.dataTransfer.files.length) uploadFile(e.dataTransfer.files[0]); });
    fileInput.addEventListener('change', () => { if (fileInput.files.length) uploadFile(fileInput.files[0]); });

    async function uploadFile(file) {
      if (!file.name.match(/\\.xlsx?$/i)) { showStatus('error', 'Please upload an .xlsx file.'); return; }
      showStatus('loading', 'Uploading and processing ' + file.name + '... This may take a minute.');
      const formData = new FormData();
      formData.append('file', file);
      try {
        const resp = await fetch('/api/upload-excel', { method: 'POST', body: formData });
        const data = await resp.json();
        if (data.success) {
          showStatus('success', 'Upload complete!\\n\\n' + Object.entries(data.results).map(([k,v]) => k + ': ' + v + ' rows inserted').join('\\n'));
          loadTables();
        } else {
          showStatus('error', 'Error: ' + (data.error || 'Unknown error'));
        }
      } catch (err) {
        showStatus('error', 'Upload failed: ' + err.message);
      }
    }

    function showStatus(type, msg) { status.className = type; status.textContent = msg; }

    async function loadTables() {
      try {
        const resp = await fetch('/api/tables');
        const data = await resp.json();
        document.getElementById('tableRows').innerHTML = Object.entries(data.tables).map(([name, count]) =>
          '<div class="table-row"><span class="table-name">' + name + '</span><span class="table-count">' + count.toLocaleString() + ' rows</span></div>'
        ).join('');
      } catch (err) {
        document.getElementById('tableRows').innerHTML = '<div style="color:#f87171">Error loading table info</div>';
      }
    }
    loadTables();
  </script>
</body>
</html>`);
});

// ================================================================
// EXCEL UPLOAD API — processes the uploaded file into database
// ================================================================

app.post('/api/upload-excel', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ success: false, error: 'No file uploaded' });

    const workbook = XLSX.read(req.file.buffer, { type: 'buffer' });
    const sheetNames = workbook.SheetNames;
    console.log('Processing Excel file. Sheets:', sheetNames);

    const results = {};

    // Clear existing data before import
    await pool.query('TRUNCATE TABLE profit_loss, balance_sheet, cash_flow, ar_aging, budget RESTART IDENTITY');

    // Process each sheet
    for (const sheetName of sheetNames) {
      const ws = workbook.Sheets[sheetName];
      const data = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

      if (!data || data.length < 3) continue;

      const sheetLower = sheetName.toLowerCase();

      // Detect entity from sheet name
      let entity = 'Unknown';
      if (sheetLower.includes('xyz')) entity = 'XYZ';
      else if (sheetLower.includes('abc')) entity = 'ABC';
      else if (sheetLower.includes('consol')) entity = 'Consolidated';

      // Detect sheet type
      if (sheetLower.includes('p&l') || sheetLower.includes('p & l') || sheetLower.includes('profit') || sheetLower.includes('income') || sheetLower.includes('pl ')) {
        const count = await parseProfitLoss(data, entity);
        results[sheetName] = count;
      } else if (sheetLower.includes('balance') || sheetLower.includes('bs ') || sheetLower.includes('bs_')) {
        const count = await parseBalanceSheet(data, entity);
        results[sheetName] = count;
      } else if (sheetLower.includes('cash flow') || sheetLower.includes('cashflow') || sheetLower.includes('cf ') || sheetLower.includes('cf_')) {
        const count = await parseCashFlow(data, entity);
        results[sheetName] = count;
      } else if (sheetLower.includes('ar') || sheetLower.includes('aging') || sheetLower.includes('receivable')) {
        const count = await parseARAging(data, entity);
        results[sheetName] = count;
      } else if (sheetLower.includes('budget') || sheetLower.includes('forecast')) {
        const count = await parseBudget(data, entity);
        results[sheetName] = count;
      } else {
        // Try to detect from content — attempt P&L parse as fallback
        console.log(`Sheet "${sheetName}" — type not detected from name, attempting generic parse...`);
        const count = await parseGenericSheet(data, entity, sheetName);
        if (count > 0) results[sheetName] = count;
      }
    }

    console.log('Import results:', results);
    res.json({ success: true, results, sheets_processed: Object.keys(results).length });

  } catch (err) {
    console.error('Upload error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ================================================================
// EXCEL PARSERS — convert spreadsheet rows into database records
// ================================================================

// Find the header row that contains month columns (Jan, Feb, etc.)
function findMonthHeaderRow(data) {
  const monthNames = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
  for (let i = 0; i < Math.min(data.length, 15); i++) {
    const row = data[i];
    if (!row) continue;
    const rowStr = row.map(c => String(c || '').toLowerCase()).join(' ');
    const monthsFound = monthNames.filter(m => rowStr.includes(m));
    if (monthsFound.length >= 3) return i;
  }
  return -1;
}

// Extract month/year pairs from a header row
function extractMonthColumns(headerRow) {
  const months = [];
  const monthMap = { jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6, jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12 };
  
  for (let col = 0; col < headerRow.length; col++) {
    const cell = String(headerRow[col] || '').trim().toLowerCase();
    for (const [abbr, num] of Object.entries(monthMap)) {
      if (cell.includes(abbr)) {
        // Try to extract year
        let year = null;
        const yearMatch = cell.match(/(20\d{2})/);
        if (yearMatch) year = parseInt(yearMatch[1]);
        // Check the row above for year if not in this cell
        months.push({ col, month: num, year });
        break;
      }
    }
  }
  return months;
}

// Try to find years from surrounding rows
function resolveYears(data, headerRowIdx, months) {
  // Check row above header for year info
  if (headerRowIdx > 0) {
    const aboveRow = data[headerRowIdx - 1];
    if (aboveRow) {
      for (const m of months) {
        if (!m.year) {
          const cell = String(aboveRow[m.col] || '').trim();
          const yearMatch = cell.match(/(20\d{2})/);
          if (yearMatch) m.year = parseInt(yearMatch[1]);
        }
      }
    }
    // Also check 2 rows above
    if (headerRowIdx > 1) {
      const above2 = data[headerRowIdx - 2];
      if (above2) {
        for (const m of months) {
          if (!m.year) {
            const cell = String(above2[m.col] || '').trim();
            const yearMatch = cell.match(/(20\d{2})/);
            if (yearMatch) m.year = parseInt(yearMatch[1]);
          }
        }
      }
    }
  }

  // Fill in years by inferring from neighbors
  // If Jan follows Dec, increment year
  let lastYear = null;
  let lastMonth = null;
  for (const m of months) {
    if (m.year) { lastYear = m.year; lastMonth = m.month; continue; }
    if (lastYear && lastMonth) {
      if (m.month < lastMonth) m.year = lastYear + 1;
      else m.year = lastYear;
    }
    lastMonth = m.month;
  }

  // Backfill any remaining nulls
  for (let i = months.length - 1; i >= 0; i--) {
    if (!months[i].year && i < months.length - 1 && months[i + 1].year) {
      if (months[i].month > months[i + 1].month) months[i].year = months[i + 1].year - 1;
      else months[i].year = months[i + 1].year;
    }
  }

  // Default to 2023 if still null
  for (const m of months) {
    if (!m.year) m.year = 2023;
  }
}

async function parseProfitLoss(data, entity) {
  const headerIdx = findMonthHeaderRow(data);
  if (headerIdx < 0) return 0;

  const months = extractMonthColumns(data[headerIdx]);
  resolveYears(data, headerIdx, months);

  let count = 0;
  let currentCategory = '';
  let currentGM = '';

  for (let i = headerIdx + 1; i < data.length; i++) {
    const row = data[i];
    if (!row) continue;

    const label = String(row[0] || row[1] || '').trim();
    if (!label) continue;

    // Detect category headers (rows with text but no numbers)
    const hasNumbers = months.some(m => {
      const val = row[m.col];
      return val !== null && val !== undefined && val !== '' && !isNaN(parseFloat(val));
    });

    if (!hasNumbers) {
      const labelLower = label.toLowerCase();
      if (labelLower.includes('revenue') || labelLower.includes('income') || labelLower.includes('sales')) currentCategory = 'Revenue';
      else if (labelLower.includes('direct cost') || labelLower.includes('cogs') || labelLower.includes('cost of')) currentCategory = 'Direct Costs';
      else if (labelLower.includes('gross margin') || labelLower.includes('gross profit')) currentCategory = 'Gross Margin';
      else if (labelLower.includes('operating') || labelLower.includes('opex')) currentCategory = 'Operating Expenses';
      else if (labelLower.includes('payroll') || labelLower.includes('salary') || labelLower.includes('compensation')) currentCategory = 'Payroll';
      else if (labelLower.includes('overhead') || labelLower.includes('g&a') || labelLower.includes('general')) currentCategory = 'Overhead';
      else if (labelLower.includes('total')) continue;
      else if (label.length > 2) currentCategory = label;

      // Check for GM category (SEO, PPC, etc.)
      if (labelLower.includes('seo')) currentGM = 'SEO';
      else if (labelLower.includes('ppc')) currentGM = 'PPC';
      else if (labelLower.includes('web') || labelLower.includes('design')) currentGM = 'Web Design';
      else if (labelLower.includes('consulting') || labelLower.includes('other')) currentGM = 'Other';

      continue;
    }

    // Skip total/subtotal rows
    const labelLower = label.toLowerCase();
    if (labelLower.startsWith('total') || labelLower.startsWith('subtotal') || labelLower.includes('net income') || labelLower.includes('ebitda')) continue;

    // Insert data for each month
    for (const m of months) {
      const val = parseFloat(row[m.col]);
      if (isNaN(val) || val === 0) continue;

      await pool.query(
        `INSERT INTO profit_loss (entity, gm_category, pl_category, account_name, period_year, period_month, amount, data_type)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'actual')`,
        [entity, currentGM || null, currentCategory || 'Other', label, m.year, m.month, val]
      );
      count++;
    }
  }
  return count;
}

async function parseBalanceSheet(data, entity) {
  const headerIdx = findMonthHeaderRow(data);
  if (headerIdx < 0) return 0;

  const months = extractMonthColumns(data[headerIdx]);
  resolveYears(data, headerIdx, months);

  let count = 0;
  let currentCategory = '';
  let currentSubCategory = '';

  for (let i = headerIdx + 1; i < data.length; i++) {
    const row = data[i];
    if (!row) continue;

    const label = String(row[0] || row[1] || '').trim();
    if (!label) continue;

    const hasNumbers = months.some(m => {
      const val = row[m.col];
      return val !== null && val !== undefined && val !== '' && !isNaN(parseFloat(val));
    });

    if (!hasNumbers) {
      const labelLower = label.toLowerCase();
      if (labelLower.includes('asset')) currentCategory = 'Assets';
      else if (labelLower.includes('liabilit')) currentCategory = 'Liabilities';
      else if (labelLower.includes('equity') || labelLower.includes('shareholder')) currentCategory = 'Equity';
      else if (labelLower.includes('current')) currentSubCategory = 'Current';
      else if (labelLower.includes('non-current') || labelLower.includes('long-term') || labelLower.includes('fixed')) currentSubCategory = 'Non-Current';
      else if (label.length > 2) currentSubCategory = label;
      continue;
    }

    const labelLower = label.toLowerCase();
    if (labelLower.startsWith('total')) continue;

    for (const m of months) {
      const val = parseFloat(row[m.col]);
      if (isNaN(val) || val === 0) continue;

      await pool.query(
        `INSERT INTO balance_sheet (entity, category, sub_category, account_name, period_year, period_month, amount)
         VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        [entity, currentCategory || 'Other', currentSubCategory || '', label, m.year, m.month, val]
      );
      count++;
    }
  }
  return count;
}

async function parseCashFlow(data, entity) {
  const headerIdx = findMonthHeaderRow(data);
  if (headerIdx < 0) return 0;

  const months = extractMonthColumns(data[headerIdx]);
  resolveYears(data, headerIdx, months);

  let count = 0;
  let currentCategory = '';

  for (let i = headerIdx + 1; i < data.length; i++) {
    const row = data[i];
    if (!row) continue;

    const label = String(row[0] || row[1] || '').trim();
    if (!label) continue;

    const hasNumbers = months.some(m => {
      const val = row[m.col];
      return val !== null && val !== undefined && val !== '' && !isNaN(parseFloat(val));
    });

    if (!hasNumbers) {
      const labelLower = label.toLowerCase();
      if (labelLower.includes('operating')) currentCategory = 'Operating';
      else if (labelLower.includes('investing')) currentCategory = 'Investing';
      else if (labelLower.includes('financing')) currentCategory = 'Financing';
      continue;
    }

    const labelLower = label.toLowerCase();
    if (labelLower.startsWith('total') || labelLower.startsWith('net ')) continue;

    for (const m of months) {
      const val = parseFloat(row[m.col]);
      if (isNaN(val) || val === 0) continue;

      await pool.query(
        `INSERT INTO cash_flow (entity, category, account_name, period_year, period_month, amount)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [entity, currentCategory || 'Other', label, m.year, m.month, val]
      );
      count++;
    }
  }
  return count;
}

async function parseARAging(data, entity) {
  const headerIdx = findMonthHeaderRow(data);
  // AR aging might not have month headers — look for aging bucket headers instead
  let count = 0;

  // Try to find aging bucket columns
  for (let i = 0; i < Math.min(data.length, 10); i++) {
    const row = data[i];
    if (!row) continue;
    const rowStr = row.map(c => String(c || '').toLowerCase()).join(' ');
    if (rowStr.includes('current') || rowStr.includes('1-30') || rowStr.includes('30') || rowStr.includes('aging')) {
      // Found header row for aging
      const buckets = [];
      for (let col = 0; col < row.length; col++) {
        const cell = String(row[col] || '').trim().toLowerCase();
        if (cell.includes('current') && !cell.includes('non')) buckets.push({ col, bucket: 'Current' });
        else if (cell.includes('1-30') || cell.includes('1 - 30')) buckets.push({ col, bucket: '1-30 Days' });
        else if (cell.includes('31-60') || cell.includes('31 - 60')) buckets.push({ col, bucket: '31-60 Days' });
        else if (cell.includes('61-90') || cell.includes('61 - 90')) buckets.push({ col, bucket: '61-90 Days' });
        else if (cell.includes('91') || cell.includes('90+') || cell.includes('over 90')) buckets.push({ col, bucket: '91+ Days' });
        else if (cell.includes('total')) buckets.push({ col, bucket: 'Total' });
      }

      if (buckets.length > 0) {
        for (let j = i + 1; j < data.length; j++) {
          const dataRow = data[j];
          if (!dataRow) continue;
          const customer = String(dataRow[0] || dataRow[1] || '').trim();
          if (!customer || customer.toLowerCase().startsWith('total')) continue;

          for (const b of buckets) {
            if (b.bucket === 'Total') continue;
            const val = parseFloat(dataRow[b.col]);
            if (isNaN(val) || val === 0) continue;
            await pool.query(
              `INSERT INTO ar_aging (entity, customer_name, aging_bucket, amount, period_year, period_month)
               VALUES ($1, $2, $3, $4, $5, $6)`,
              [entity, customer, b.bucket, val, 2024, 3]
            );
            count++;
          }
        }
      }
      break;
    }
  }

  // Fallback: if we have month headers, parse like other sheets
  if (count === 0 && headerIdx >= 0) {
    console.log('AR Aging: falling back to month-based parse');
  }

  return count;
}

async function parseBudget(data, entity) {
  const headerIdx = findMonthHeaderRow(data);
  if (headerIdx < 0) return 0;

  const months = extractMonthColumns(data[headerIdx]);
  resolveYears(data, headerIdx, months);

  let count = 0;

  for (let i = headerIdx + 1; i < data.length; i++) {
    const row = data[i];
    if (!row) continue;

    const label = String(row[0] || row[1] || '').trim();
    if (!label || label.toLowerCase().startsWith('total')) continue;

    const hasNumbers = months.some(m => {
      const val = row[m.col];
      return val !== null && val !== undefined && val !== '' && !isNaN(parseFloat(val));
    });
    if (!hasNumbers) continue;

    for (const m of months) {
      const val = parseFloat(row[m.col]);
      if (isNaN(val) || val === 0) continue;

      await pool.query(
        `INSERT INTO budget (entity, account_name, period_year, period_month, amount)
         VALUES ($1, $2, $3, $4, $5)`,
        [entity, label, m.year, m.month, val]
      );
      count++;
    }
  }
  return count;
}

async function parseGenericSheet(data, entity, sheetName) {
  // Attempt to parse as P&L by default
  const count = await parseProfitLoss(data, entity);
  if (count > 0) console.log(`Sheet "${sheetName}" parsed as P&L: ${count} rows`);
  return count;
}

// ================================================================
// MANUAL INGEST API — for Make.com or other integrations
// ================================================================

app.post('/api/ingest', async (req, res) => {
  try {
    const { table, data } = req.body;
    if (!table || !data || !Array.isArray(data)) {
      return res.status(400).json({ error: 'Required: { table: "profit_loss", data: [{...}, ...] }' });
    }

    const validTables = ['profit_loss', 'balance_sheet', 'cash_flow', 'ar_aging', 'budget'];
    if (!validTables.includes(table)) {
      return res.status(400).json({ error: `Invalid table. Valid: ${validTables.join(', ')}` });
    }

    let inserted = 0;
    for (const row of data) {
      const columns = Object.keys(row);
      const values = Object.values(row);
      const placeholders = values.map((_, i) => `$${i + 1}`);
      await pool.query(
        `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${placeholders.join(', ')})`,
        values
      );
      inserted++;
    }

    res.json({ success: true, table, inserted });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ================================================================
// START SERVER
// ================================================================

async function start() {
  await setupDatabase();
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Convor Finance MCP Server running on port ${PORT}`);
    console.log(`  SSE endpoint: /sse`);
    console.log(`  Message endpoint: /message`);
    console.log(`  Upload page: /upload`);
    console.log(`  Health check: /health`);
    console.log(`  Ingest API: /api/ingest`);
  });
}

start().catch(err => { console.error('Failed to start:', err); process.exit(1); });
