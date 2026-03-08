const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('railway') ? { rejectUnauthorized: false } : false
});

async function setupDatabase() {
  const client = await pool.connect();
  try {
    console.log('Creating database tables...');
    
    await client.query(`
      CREATE TABLE IF NOT EXISTS profit_loss (
        id SERIAL PRIMARY KEY,
        entity VARCHAR(20) NOT NULL,
        gm_category VARCHAR(100),
        pl_category VARCHAR(100),
        account_name VARCHAR(200) NOT NULL,
        period_year INTEGER NOT NULL,
        period_month INTEGER NOT NULL,
        amount DECIMAL(15,2) DEFAULT 0,
        data_type VARCHAR(20) DEFAULT 'actual',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS balance_sheet (
        id SERIAL PRIMARY KEY,
        entity VARCHAR(20) NOT NULL,
        mapping_category VARCHAR(100),
        account_name VARCHAR(200) NOT NULL,
        period_year INTEGER NOT NULL,
        period_month INTEGER NOT NULL,
        amount DECIMAL(15,2) DEFAULT 0,
        data_type VARCHAR(20) DEFAULT 'actual',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS cash_flow (
        id SERIAL PRIMARY KEY,
        entity VARCHAR(20) NOT NULL,
        section VARCHAR(100),
        line_item VARCHAR(200) NOT NULL,
        period_year INTEGER NOT NULL,
        period_month INTEGER NOT NULL,
        amount DECIMAL(15,2) DEFAULT 0,
        data_type VARCHAR(20) DEFAULT 'actual',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS ar_aging (
        id SERIAL PRIMARY KEY,
        period_date DATE NOT NULL,
        current_amount DECIMAL(15,2) DEFAULT 0,
        days_1_30 DECIMAL(15,2) DEFAULT 0,
        days_1_30_total DECIMAL(15,2) DEFAULT 0,
        days_31_60 DECIMAL(15,2) DEFAULT 0,
        days_61_90 DECIMAL(15,2) DEFAULT 0,
        days_91_plus DECIMAL(15,2) DEFAULT 0,
        gross_ar DECIMAL(15,2) DEFAULT 0,
        bad_debt_reserve DECIMAL(15,2) DEFAULT 0,
        net_ar DECIMAL(15,2) DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS budget (
        id SERIAL PRIMARY KEY,
        line_item VARCHAR(200) NOT NULL,
        period_year INTEGER NOT NULL,
        period_month INTEGER NOT NULL,
        amount DECIMAL(15,2) DEFAULT 0,
        quarter VARCHAR(10),
        forecast_type VARCHAR(20) DEFAULT 'forecast',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

    // Create indexes
    const indexes = [
      'CREATE INDEX IF NOT EXISTS idx_pl_entity_period ON profit_loss(entity, period_year, period_month)',
      'CREATE INDEX IF NOT EXISTS idx_pl_account ON profit_loss(account_name)',
      'CREATE INDEX IF NOT EXISTS idx_pl_category ON profit_loss(gm_category, pl_category)',
      'CREATE INDEX IF NOT EXISTS idx_bs_entity_period ON balance_sheet(entity, period_year, period_month)',
      'CREATE INDEX IF NOT EXISTS idx_bs_account ON balance_sheet(account_name)',
      'CREATE INDEX IF NOT EXISTS idx_cf_entity_period ON cash_flow(entity, period_year, period_month)',
      'CREATE INDEX IF NOT EXISTS idx_ar_date ON ar_aging(period_date)',
      'CREATE INDEX IF NOT EXISTS idx_budget_period ON budget(period_year, period_month)',
    ];

    for (const idx of indexes) {
      await client.query(idx);
    }

    console.log('All tables and indexes created successfully!');
    
    // Show table counts
    const tables = ['profit_loss', 'balance_sheet', 'cash_flow', 'ar_aging', 'budget'];
    for (const table of tables) {
      const res = await client.query(`SELECT COUNT(*) FROM ${table}`);
      console.log(`  ${table}: ${res.rows[0].count} rows`);
    }

  } catch (err) {
    console.error('Error setting up database:', err);
    throw err;
  } finally {
    client.release();
  }
}

module.exports = { setupDatabase, pool };
