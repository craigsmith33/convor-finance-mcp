const XLSX = require('xlsx');
const { Pool } = require('pg');
const path = require('path');
const fs = require('fs');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('railway') ? { rejectUnauthorized: false } : false
});

async function ingestExcel(filePath) {
  console.log(`Reading Excel file: ${filePath}`);
  const workbook = XLSX.readFile(filePath);
  console.log('Sheets found:', workbook.SheetNames);

  const client = await pool.connect();

  try {
    // Clear existing data
    console.log('Clearing existing data...');
    await client.query('TRUNCATE profit_loss, balance_sheet, cash_flow, ar_aging, budget RESTART IDENTITY');

    // ============================================================
    // INGEST P&L DATA (XYZ and ABC)
    // ============================================================
    for (const sheetConfig of [
      { sheet: 'PL-XYZ Actual', entity: 'XYZ' },
      { sheet: 'PL-ABC Actual', entity: 'ABC' }
    ]) {
      console.log(`\nProcessing ${sheetConfig.sheet}...`);
      const ws = workbook.Sheets[sheetConfig.sheet];
      if (!ws) { console.log(`  Sheet not found, skipping`); continue; }

      const data = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

      // Row 3 has the dates (columns starting from index 3)
      const dateRow = data[2]; // 0-indexed row 3
      const dates = [];
      for (let col = 3; col < dateRow.length; col++) {
        const val = dateRow[col];
        if (val) {
          let d;
          if (typeof val === 'number') {
            d = XLSX.SSF.parse_date_code(val);
            dates.push({ col, year: d.y, month: d.m });
          } else if (val instanceof Date) {
            dates.push({ col, year: val.getFullYear(), month: val.getMonth() + 1 });
          }
        }
      }

      let rowCount = 0;
      // Data rows start from row 5 (index 4)
      for (let row = 4; row < data.length; row++) {
        const r = data[row];
        if (!r || !r[2]) continue; // Skip empty rows
        
        const accountName = String(r[2]).trim();
        if (!accountName || accountName.startsWith('Total') || accountName === 'Revenue' || 
            accountName === 'Income' || accountName === 'Cost of Sales' || 
            accountName === 'Operating Expenses' || accountName === 'Other Income / Expense' ||
            accountName === 'Gross Profit' || accountName === 'Net Operating Income' ||
            accountName === 'Net Income' || accountName === 'EBITDA') continue;

        const gmCategory = r[0] ? String(r[0]).trim() : null;
        const plCategory = r[1] ? String(r[1]).trim() : null;

        for (const dateInfo of dates) {
          const amount = r[dateInfo.col];
          if (amount === null || amount === undefined || amount === '' || amount === 0) continue;
          
          const numAmount = parseFloat(amount);
          if (isNaN(numAmount)) continue;

          await client.query(
            `INSERT INTO profit_loss (entity, gm_category, pl_category, account_name, period_year, period_month, amount, data_type) 
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'actual')`,
            [sheetConfig.entity, gmCategory, plCategory, accountName, dateInfo.year, dateInfo.month, numAmount]
          );
          rowCount++;
        }
      }
      console.log(`  Inserted ${rowCount} P&L rows for ${sheetConfig.entity}`);
    }

    // ============================================================
    // INGEST BALANCE SHEET DATA (XYZ and ABC)
    // ============================================================
    for (const sheetConfig of [
      { sheet: 'BS-XYZ Actual', entity: 'XYZ' },
      { sheet: 'BS-ABC Actual', entity: 'ABC' }
    ]) {
      console.log(`\nProcessing ${sheetConfig.sheet}...`);
      const ws = workbook.Sheets[sheetConfig.sheet];
      if (!ws) { console.log(`  Sheet not found, skipping`); continue; }

      const data = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

      // Row 3 has dates starting from column 2
      const dateRow = data[2];
      const dates = [];
      for (let col = 2; col < dateRow.length; col++) {
        const val = dateRow[col];
        if (val) {
          let d;
          if (typeof val === 'number') {
            d = XLSX.SSF.parse_date_code(val);
            dates.push({ col, year: d.y, month: d.m });
          } else if (val instanceof Date) {
            dates.push({ col, year: val.getFullYear(), month: val.getMonth() + 1 });
          }
        }
      }

      let rowCount = 0;
      for (let row = 3; row < data.length; row++) {
        const r = data[row];
        if (!r || !r[1]) continue;
        
        const accountName = String(r[1]).trim();
        if (!accountName || accountName === 'ASSETS' || accountName === 'LIABILITIES AND EQUITY' ||
            accountName.startsWith('Total') || accountName === 'Current Assets' || 
            accountName === 'Bank Accounts' || accountName === 'Accounts Receivable' ||
            accountName === 'Other Current Assets' || accountName === 'Fixed Assets' ||
            accountName === 'Other Assets' || accountName === 'Current Liabilities' ||
            accountName === 'Long-Term Liabilities' || accountName === 'Equity') continue;

        const mappingCategory = r[0] ? String(r[0]).trim() : null;

        for (const dateInfo of dates) {
          const amount = r[dateInfo.col];
          if (amount === null || amount === undefined || amount === '' || amount === 0) continue;
          
          const numAmount = parseFloat(amount);
          if (isNaN(numAmount)) continue;

          await client.query(
            `INSERT INTO balance_sheet (entity, mapping_category, account_name, period_year, period_month, amount, data_type) 
             VALUES ($1, $2, $3, $4, $5, $6, 'actual')`,
            [sheetConfig.entity, mappingCategory, accountName, dateInfo.year, dateInfo.month, numAmount]
          );
          rowCount++;
        }
      }
      console.log(`  Inserted ${rowCount} Balance Sheet rows for ${sheetConfig.entity}`);
    }

    // ============================================================
    // INGEST CASH FLOW DATA (XYZ and ABC)
    // ============================================================
    for (const sheetConfig of [
      { sheet: 'CF-XYZ Actual', entity: 'XYZ' },
      { sheet: 'CF-ABC Actual', entity: 'ABC' }
    ]) {
      console.log(`\nProcessing ${sheetConfig.sheet}...`);
      const ws = workbook.Sheets[sheetConfig.sheet];
      if (!ws) { console.log(`  Sheet not found, skipping`); continue; }

      const data = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

      // Row 3 has dates starting from column 1
      const dateRow = data[2];
      const dates = [];
      for (let col = 1; col < dateRow.length; col++) {
        const val = dateRow[col];
        if (val) {
          let d;
          if (typeof val === 'number') {
            d = XLSX.SSF.parse_date_code(val);
            dates.push({ col, year: d.y, month: d.m });
          } else if (val instanceof Date) {
            dates.push({ col, year: val.getFullYear(), month: val.getMonth() + 1 });
          }
        }
      }

      let currentSection = 'Operating Activities';
      let rowCount = 0;
      for (let row = 3; row < data.length; row++) {
        const r = data[row];
        if (!r || !r[0]) continue;
        
        const lineItem = String(r[0]).trim();
        
        // Track sections
        if (lineItem.includes('Operating Activities')) { currentSection = 'Operating Activities'; continue; }
        if (lineItem.includes('Investing Activities')) { currentSection = 'Investing Activities'; continue; }
        if (lineItem.includes('Financing Activities')) { currentSection = 'Financing Activities'; continue; }
        if (lineItem.startsWith('Total') || lineItem.startsWith('Net Change') || 
            lineItem === 'Adjustments to net income:' || lineItem === 'Change in operating assets and liabilities:') continue;

        for (const dateInfo of dates) {
          const amount = r[dateInfo.col];
          if (amount === null || amount === undefined || amount === '' || amount === 0) continue;
          
          const numAmount = parseFloat(amount);
          if (isNaN(numAmount)) continue;

          await client.query(
            `INSERT INTO cash_flow (entity, section, line_item, period_year, period_month, amount, data_type) 
             VALUES ($1, $2, $3, $4, $5, $6, 'actual')`,
            [sheetConfig.entity, currentSection, lineItem, dateInfo.year, dateInfo.month, numAmount]
          );
          rowCount++;
        }
      }
      console.log(`  Inserted ${rowCount} Cash Flow rows for ${sheetConfig.entity}`);
    }

    // ============================================================
    // INGEST AR AGING DATA
    // ============================================================
    console.log('\nProcessing AR Aging Slide...');
    const arWs = workbook.Sheets['AR Aging Slide'];
    if (arWs) {
      const data = XLSX.utils.sheet_to_json(arWs, { header: 1, defval: null });
      let rowCount = 0;
      
      for (let row = 4; row < data.length; row++) {
        const r = data[row];
        if (!r || !r[1]) continue;
        
        let periodDate;
        const val = r[1];
        if (typeof val === 'number') {
          const d = XLSX.SSF.parse_date_code(val);
          periodDate = `${d.y}-${String(d.m).padStart(2, '0')}-01`;
        } else if (val instanceof Date) {
          periodDate = val.toISOString().split('T')[0];
        } else continue;

        await client.query(
          `INSERT INTO ar_aging (period_date, current_amount, days_1_30, days_1_30_total, days_31_60, days_61_90, days_91_plus, gross_ar, bad_debt_reserve, net_ar) 
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
          [
            periodDate,
            parseFloat(r[2]) || 0,
            parseFloat(r[3]) || 0,
            parseFloat(r[4]) || 0,
            parseFloat(r[5]) || 0,
            parseFloat(r[6]) || 0,
            parseFloat(r[7]) || 0,
            parseFloat(r[9]) || 0,
            parseFloat(r[10]) || 0,
            parseFloat(r[11]) || 0
          ]
        );
        rowCount++;
      }
      console.log(`  Inserted ${rowCount} AR Aging rows`);
    }

    // ============================================================
    // INGEST BUDGET DATA
    // ============================================================
    console.log('\nProcessing 2024 Budget...');
    const budgetWs = workbook.Sheets['2024 Budget'];
    if (budgetWs) {
      const data = XLSX.utils.sheet_to_json(budgetWs, { header: 1, defval: null });
      
      // Row 5 has dates (index 4)
      const dateRow = data[4];
      const dates = [];
      for (let col = 1; col <= 12; col++) {
        const val = dateRow[col];
        if (val) {
          let d;
          if (typeof val === 'number') {
            d = XLSX.SSF.parse_date_code(val);
            dates.push({ col, year: d.y, month: d.m });
          } else if (val instanceof Date) {
            dates.push({ col, year: val.getFullYear(), month: val.getMonth() + 1 });
          }
        }
      }

      // Quarter mapping from row 2
      const quarterRow = data[1];
      
      let rowCount = 0;
      for (let row = 5; row < data.length; row++) {
        const r = data[row];
        if (!r || !r[0]) continue;
        
        const lineItem = String(r[0]).trim();
        if (!lineItem || lineItem === 'Margin %') continue;

        for (const dateInfo of dates) {
          const amount = r[dateInfo.col];
          if (amount === null || amount === undefined || amount === '' || amount === 0) continue;
          
          const numAmount = parseFloat(amount);
          if (isNaN(numAmount)) continue;

          const quarter = quarterRow[dateInfo.col] ? String(quarterRow[dateInfo.col]).trim() : null;

          await client.query(
            `INSERT INTO budget (line_item, period_year, period_month, amount, quarter, forecast_type) 
             VALUES ($1, $2, $3, $4, $5, 'forecast')`,
            [lineItem, dateInfo.year, dateInfo.month, numAmount, quarter]
          );
          rowCount++;
        }
      }
      console.log(`  Inserted ${rowCount} Budget rows`);
    }

    console.log('\n✅ Data ingestion complete!');
    
    // Print summary
    const tables = ['profit_loss', 'balance_sheet', 'cash_flow', 'ar_aging', 'budget'];
    console.log('\nFinal row counts:');
    for (const table of tables) {
      const res = await client.query(`SELECT COUNT(*) FROM ${table}`);
      console.log(`  ${table}: ${res.rows[0].count} rows`);
    }

  } catch (err) {
    console.error('Ingestion error:', err);
    throw err;
  } finally {
    client.release();
    await pool.end();
  }
}

// Run with file path argument or default
const filePath = process.argv[2] || path.join(__dirname, 'Example_Financial_Sheet.xlsx');
if (!fs.existsSync(filePath)) {
  console.error(`File not found: ${filePath}`);
  process.exit(1);
}

ingestExcel(filePath);
