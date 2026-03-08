const express = require('express');
const cors = require('cors');
const { setupDatabase, pool } = require('./setup-db');

const app = express();
app.use(cors());
app.use(express.json({ limit: '50mb' }));

const PORT = process.env.PORT || 8000;

// ============================================================
// SSE/MCP ENDPOINT - LibreChat connects here
// ============================================================

// Tool definitions that get sent to the AI model
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
    description: "Query expense data from the Profit & Loss statement. Returns expenses by entity, category, and time period. Use this for questions about costs, spending, payroll, or operating expenses.",
    inputSchema: {
      type: "object",
      properties: {
        entity: { type: "string", description: "Business entity: 'XYZ' or 'ABC' or 'all' for both", default: "all" },
        year: { type: "integer", description: "Year to query (2022, 2023, or 2024)" },
        month: { type: "integer", description: "Month number 1-12. Omit for full year." }
      },
      required: ["entity", "year"]
    }
  },
  {
    name: "query_balance_sheet",
    description: "Query balance sheet data including assets, liabilities, and equity. Use for questions about cash position, accounts receivable, debt, or net worth.",
    inputSchema: {
      type: "object",
      properties: {
        entity: { type: "string", description: "Business entity: 'XYZ' or 'ABC' or 'all'", default: "all" },
        year: { type: "integer", description: "Year to query" },
        month: { type: "integer", description: "Month number 1-12" },
        category: { type: "string", description: "Filter by mapping category like 'Cash', 'Accounts Receivable', etc. Omit for all." }
      },
      required: ["entity", "year", "month"]
    }
  },
  {
    name: "query_cash_flow",
    description: "Query cash flow statement data. Use for questions about operating cash flow, investing activities, financing, or overall cash generation.",
    inputSchema: {
      type: "object",
      properties: {
        entity: { type: "string", description: "Business entity: 'XYZ' or 'ABC' or 'all'", default: "all" },
        year: { type: "integer", description: "Year to query" },
        month: { type: "integer", description: "Month number 1-12. Omit for full year." }
      },
      required: ["entity", "year"]
    }
  },
  {
    name: "query_ar_aging",
    description: "Query accounts receivable aging data. Shows how much money is owed and how overdue it is. Use for questions about collections, receivables health, or days sales outstanding.",
    inputSchema: {
      type: "object",
      properties: {
        year: { type: "integer", description: "Year to query" },
        month: { type: "integer", description: "Month number 1-12. Omit for full year trend." }
      },
      required: ["year"]
    }
  },
  {
    name: "query_budget_vs_actual",
    description: "Compare 2024 budget/forecast to actual results. Use for variance analysis, budget tracking, and forecast accuracy questions.",
    inputSchema: {
      type: "object",
      properties: {
        month: { type: "integer", description: "Month number 1-12 in 2024" },
        line_item: { type: "string", description: "Specific budget line item. Omit for summary." }
      },
      required: ["month"]
    }
  },
  {
    name: "run_financial_query",
    description: "Run a custom SQL query against the financial database. Use this for complex analysis that other tools can't handle, like custom aggregations, comparisons across multiple periods, or calculating growth rates. Available tables: profit_loss, balance_sheet, cash_flow, ar_aging, budget.",
    inputSchema: {
      type: "object",
      properties: {
        sql: { type: "string", description: "SQL SELECT query to run. Only SELECT queries are allowed." }
      },
      required: ["sql"]
    }
  },
  {
    name: "get_database_info",
    description: "Get information about what data is available in the database — which entities, time periods, accounts, and how many records exist. Use this first when you need to understand what data is available before running queries.",
    inputSchema: {
      type: "object",
      properties: {},
      required: []
    }
  }
];

// ============================================================
// TOOL EXECUTION FUNCTIONS
// ============================================================

async function executeQuery(toolName, args) {
  const client = await pool.connect();
  try {
    switch (toolName) {
      case 'query_revenue': {
        let sql = `SELECT entity, account_name, gm_category, pl_category, period_year, period_month, SUM(amount) as total 
                    FROM profit_loss 
                    WHERE gm_category NOT IN ('Cost of Sales', 'Operating Expenses', 'Other Expenses', 'Depreciation', 'Interest') 
                    AND amount != 0`;
        const params = [];
        let paramIdx = 1;
        
        if (args.entity && args.entity !== 'all') {
          sql += ` AND entity = $${paramIdx++}`;
          params.push(args.entity);
        }
        sql += ` AND period_year = $${paramIdx++}`;
        params.push(args.year);
        
        if (args.month) {
          sql += ` AND period_month = $${paramIdx++}`;
          params.push(args.month);
        }
        if (args.account_name) {
          sql += ` AND account_name ILIKE $${paramIdx++}`;
          params.push(`%${args.account_name}%`);
        }
        sql += ' GROUP BY entity, account_name, gm_category, pl_category, period_year, period_month ORDER BY entity, period_month, total DESC';
        
        const res = await client.query(sql, params);
        return JSON.stringify(res.rows, null, 2);
      }
      
      case 'query_expenses': {
        let sql = `SELECT entity, account_name, gm_category, period_year, period_month, SUM(amount) as total 
                    FROM profit_loss 
                    WHERE (gm_category IN ('Cost of Sales', 'Operating Expenses', 'Other Expenses', 'Depreciation', 'Interest') 
                    OR account_name ILIKE '%expense%' OR account_name ILIKE '%cost%' OR account_name ILIKE '%salary%' OR account_name ILIKE '%payroll%')
                    AND amount != 0`;
        const params = [];
        let paramIdx = 1;
        
        if (args.entity && args.entity !== 'all') {
          sql += ` AND entity = $${paramIdx++}`;
          params.push(args.entity);
        }
        sql += ` AND period_year = $${paramIdx++}`;
        params.push(args.year);
        
        if (args.month) {
          sql += ` AND period_month = $${paramIdx++}`;
          params.push(args.month);
        }
        sql += ' GROUP BY entity, account_name, gm_category, period_year, period_month ORDER BY entity, period_month, total DESC';
        
        const res = await client.query(sql, params);
        return JSON.stringify(res.rows, null, 2);
      }
      
      case 'query_balance_sheet': {
        let sql = `SELECT entity, mapping_category, account_name, period_year, period_month, amount 
                    FROM balance_sheet WHERE amount != 0`;
        const params = [];
        let paramIdx = 1;
        
        if (args.entity && args.entity !== 'all') {
          sql += ` AND entity = $${paramIdx++}`;
          params.push(args.entity);
        }
        sql += ` AND period_year = $${paramIdx++}`;
        params.push(args.year);
        sql += ` AND period_month = $${paramIdx++}`;
        params.push(args.month);
        
        if (args.category) {
          sql += ` AND mapping_category ILIKE $${paramIdx++}`;
          params.push(`%${args.category}%`);
        }
        sql += ' ORDER BY entity, account_name';
        
        const res = await client.query(sql, params);
        return JSON.stringify(res.rows, null, 2);
      }
      
      case 'query_cash_flow': {
        let sql = `SELECT entity, section, line_item, period_year, period_month, amount 
                    FROM cash_flow WHERE amount != 0`;
        const params = [];
        let paramIdx = 1;
        
        if (args.entity && args.entity !== 'all') {
          sql += ` AND entity = $${paramIdx++}`;
          params.push(args.entity);
        }
        sql += ` AND period_year = $${paramIdx++}`;
        params.push(args.year);
        
        if (args.month) {
          sql += ` AND period_month = $${paramIdx++}`;
          params.push(args.month);
        }
        sql += ' ORDER BY entity, period_month, section';
        
        const res = await client.query(sql, params);
        return JSON.stringify(res.rows, null, 2);
      }
      
      case 'query_ar_aging': {
        let sql = `SELECT * FROM ar_aging WHERE EXTRACT(YEAR FROM period_date) = $1`;
        const params = [args.year];
        
        if (args.month) {
          sql += ` AND EXTRACT(MONTH FROM period_date) = $2`;
          params.push(args.month);
        }
        sql += ' ORDER BY period_date';
        
        const res = await client.query(sql, params);
        return JSON.stringify(res.rows, null, 2);
      }
      
      case 'query_budget_vs_actual': {
        // Get budget data
        let sql = `
          SELECT b.line_item, b.amount as budget_amount, 
                 COALESCE(SUM(p.amount), 0) as actual_amount,
                 COALESCE(SUM(p.amount), 0) - b.amount as variance
          FROM budget b
          LEFT JOIN profit_loss p ON LOWER(p.account_name) ILIKE '%' || LOWER(b.line_item) || '%'
            AND p.period_year = 2024 AND p.period_month = $1
          WHERE b.period_month = $1 AND b.period_year = 2024
          GROUP BY b.line_item, b.amount
          ORDER BY b.line_item`;
        
        const res = await client.query(sql, [args.month]);
        return JSON.stringify(res.rows, null, 2);
      }
      
      case 'run_financial_query': {
        // Safety check - only allow SELECT
        const cleanSql = args.sql.trim().toUpperCase();
        if (!cleanSql.startsWith('SELECT') && !cleanSql.startsWith('WITH')) {
          return JSON.stringify({ error: 'Only SELECT queries are allowed for safety.' });
        }
        if (cleanSql.includes('DROP') || cleanSql.includes('DELETE') || cleanSql.includes('UPDATE') || cleanSql.includes('INSERT') || cleanSql.includes('ALTER') || cleanSql.includes('TRUNCATE')) {
          return JSON.stringify({ error: 'Destructive queries are not allowed.' });
        }
        
        const res = await client.query(args.sql);
        return JSON.stringify(res.rows, null, 2);
      }
      
      case 'get_database_info': {
        const info = {};
        
        // Get row counts
        const tables = ['profit_loss', 'balance_sheet', 'cash_flow', 'ar_aging', 'budget'];
        for (const table of tables) {
          const res = await client.query(`SELECT COUNT(*) FROM ${table}`);
          info[`${table}_rows`] = parseInt(res.rows[0].count);
        }
        
        // Get entities
        const entities = await client.query('SELECT DISTINCT entity FROM profit_loss');
        info.entities = entities.rows.map(r => r.entity);
        
        // Get year range
        const years = await client.query('SELECT DISTINCT period_year FROM profit_loss ORDER BY period_year');
        info.years = years.rows.map(r => r.period_year);
        
        // Get account categories
        const categories = await client.query('SELECT DISTINCT gm_category FROM profit_loss WHERE gm_category IS NOT NULL ORDER BY gm_category');
        info.revenue_categories = categories.rows.map(r => r.gm_category);
        
        // Get P&L account names
        const accounts = await client.query('SELECT DISTINCT account_name FROM profit_loss ORDER BY account_name LIMIT 50');
        info.sample_accounts = accounts.rows.map(r => r.account_name);
        
        return JSON.stringify(info, null, 2);
      }
      
      default:
        return JSON.stringify({ error: `Unknown tool: ${toolName}` });
    }
  } finally {
    client.release();
  }
}

// ============================================================
// SSE ENDPOINT FOR MCP
// ============================================================

app.get('/sse', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  
  // Send initial connection event
  res.write(`data: ${JSON.stringify({ type: 'connection', status: 'connected' })}\n\n`);
  
  // Keep alive
  const keepAlive = setInterval(() => {
    res.write(': keepalive\n\n');
  }, 30000);
  
  req.on('close', () => {
    clearInterval(keepAlive);
  });
});

// MCP message endpoint
app.post('/message', async (req, res) => {
  const { method, params, id } = req.body;
  
  try {
    if (method === 'initialize') {
      return res.json({
        jsonrpc: '2.0',
        id,
        result: {
          protocolVersion: '2024-11-05',
          capabilities: { tools: {} },
          serverInfo: { name: 'convor-finance', version: '1.0.0' }
        }
      });
    }
    
    if (method === 'tools/list') {
      return res.json({
        jsonrpc: '2.0',
        id,
        result: { tools: TOOLS }
      });
    }
    
    if (method === 'tools/call') {
      const { name, arguments: args } = params;
      const result = await executeQuery(name, args || {});
      return res.json({
        jsonrpc: '2.0',
        id,
        result: {
          content: [{ type: 'text', text: result }]
        }
      });
    }
    
    res.json({ jsonrpc: '2.0', id, error: { code: -32601, message: 'Method not found' } });
  } catch (err) {
    console.error('MCP Error:', err);
    res.json({
      jsonrpc: '2.0',
      id,
      result: {
        content: [{ type: 'text', text: JSON.stringify({ error: err.message }) }]
      }
    });
  }
});

// ============================================================
// REST API ENDPOINTS (for Make.com ingestion & health checks)
// ============================================================

app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'convor-finance-mcp' });
});

// Endpoint to receive Excel data from Make.com
app.post('/api/ingest', async (req, res) => {
  const { table, rows } = req.body;
  
  if (!table || !rows || !Array.isArray(rows)) {
    return res.status(400).json({ error: 'Required: table (string) and rows (array)' });
  }
  
  const allowed = ['profit_loss', 'balance_sheet', 'cash_flow', 'ar_aging', 'budget'];
  if (!allowed.includes(table)) {
    return res.status(400).json({ error: `Table must be one of: ${allowed.join(', ')}` });
  }
  
  const client = await pool.connect();
  try {
    let inserted = 0;
    for (const row of rows) {
      const columns = Object.keys(row);
      const values = Object.values(row);
      const placeholders = values.map((_, i) => `$${i + 1}`).join(', ');
      
      await client.query(
        `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${placeholders})`,
        values
      );
      inserted++;
    }
    
    res.json({ success: true, inserted, table });
  } catch (err) {
    console.error('Ingest error:', err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Get table info
app.get('/api/tables', async (req, res) => {
  const client = await pool.connect();
  try {
    const tables = ['profit_loss', 'balance_sheet', 'cash_flow', 'ar_aging', 'budget'];
    const info = {};
    for (const table of tables) {
      const count = await client.query(`SELECT COUNT(*) FROM ${table}`);
      info[table] = parseInt(count.rows[0].count);
    }
    res.json(info);
  } catch (err) {
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// ============================================================
// START SERVER
// ============================================================

async function start() {
  try {
    await setupDatabase();
    console.log('Database setup complete.');
    
    app.listen(PORT, '0.0.0.0', () => {
      console.log(`Convor Finance MCP Server running on port ${PORT}`);
      console.log(`  SSE endpoint: /sse`);
      console.log(`  Message endpoint: /message`);
      console.log(`  Health check: /health`);
      console.log(`  Ingest API: /api/ingest`);
    });
  } catch (err) {
    console.error('Failed to start:', err);
    process.exit(1);
  }
}

start();
