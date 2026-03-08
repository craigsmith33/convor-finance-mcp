const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes('railway') ? { rejectUnauthorized: false } : false
});

async function setupDatabase() {
  console.log('Creating database tables...');

  // Drop and recreate all tables to ensure schema matches
  await pool.query(`
    DROP TABLE IF EXISTS profit_loss CASCADE;
    DROP TABLE IF EXISTS balance_sheet CASCADE;
    DROP TABLE IF EXISTS cash_flow CASCADE;
    DROP TABLE IF EXISTS ar_aging CASCADE;
    DROP TABLE IF EXISTS budget CASCADE;
  `);

  await pool.query(`
    CREATE TABLE profit_loss (
      id SERIAL PRIMARY KEY,
      entity VARCHAR(20) NOT NULL,
      gm_category VARCHAR(100),
      pl_category VARCHAR(100),
      account_code VARCHAR(20),
      account_name VARCHAR(200) NOT NULL,
      period_year INTEGER NOT NULL,
      period_month INTEGER NOT NULL,
      amount DECIMAL(15,2) DEFAULT 0,
      data_type VARCHAR(20) DEFAULT 'actual',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE balance_sheet (
      id SERIAL PRIMARY KEY,
      entity VARCHAR(20) NOT NULL,
      category VARCHAR(100),
      sub_category VARCHAR(100),
      account_name VARCHAR(200) NOT NULL,
      period_year INTEGER NOT NULL,
      period_month INTEGER NOT NULL,
      amount DECIMAL(15,2) DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE cash_flow (
      id SERIAL PRIMARY KEY,
      entity VARCHAR(20) NOT NULL,
      category VARCHAR(100),
      account_name VARCHAR(200) NOT NULL,
      period_year INTEGER NOT NULL,
      period_month INTEGER NOT NULL,
      amount DECIMAL(15,2) DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE ar_aging (
      id SERIAL PRIMARY KEY,
      entity VARCHAR(20) NOT NULL,
      customer_name VARCHAR(200),
      aging_bucket VARCHAR(50),
      amount DECIMAL(15,2) DEFAULT 0,
      period_year INTEGER NOT NULL,
      period_month INTEGER NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE budget (
      id SERIAL PRIMARY KEY,
      entity VARCHAR(20) NOT NULL,
      account_name VARCHAR(200) NOT NULL,
      period_year INTEGER NOT NULL,
      period_month INTEGER NOT NULL,
      amount DECIMAL(15,2) DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // Create indexes for fast queries
  await pool.query(`
    CREATE INDEX idx_pl_entity_period ON profit_loss(entity, period_year, period_month);
    CREATE INDEX idx_pl_category ON profit_loss(pl_category);
    CREATE INDEX idx_pl_account ON profit_loss(account_name);
    CREATE INDEX idx_bs_entity_period ON balance_sheet(entity, period_year, period_month);
    CREATE INDEX idx_bs_category ON balance_sheet(category);
    CREATE INDEX idx_cf_entity_period ON cash_flow(entity, period_year, period_month);
    CREATE INDEX idx_cf_category ON cash_flow(category);
    CREATE INDEX idx_ar_entity_period ON ar_aging(entity, period_year, period_month);
    CREATE INDEX idx_ar_bucket ON ar_aging(aging_bucket);
    CREATE INDEX idx_budget_entity_period ON budget(entity, period_year, period_month);
    CREATE INDEX idx_budget_account ON budget(account_name);
  `);

  // Show table status
  const tables = ['profit_loss', 'balance_sheet', 'cash_flow', 'ar_aging', 'budget'];
  for (const t of tables) {
    const r = await pool.query(`SELECT COUNT(*) as count FROM ${t}`);
    console.log(`  ${t}: ${r.rows[0].count} rows`);
  }

  console.log('All tables and indexes created successfully!');
}

module.exports = { setupDatabase, pool };
