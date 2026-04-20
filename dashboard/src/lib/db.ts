import { Pool } from "pg";

declare global {
  var __bidDashPool: Pool | undefined;
}

function required(name: string): string {
  const val = process.env[name];
  if (!val) {
    throw new Error(`Missing env var: ${name}`);
  }
  return val;
}

export function getPool(): Pool {
  if (!global.__bidDashPool) {
    const dsn = process.env.SUPABASE_DB_DSN;
    global.__bidDashPool = new Pool({
      connectionString: dsn || undefined,
      host: dsn ? undefined : required("SUPABASE_DB_HOST"),
      port: Number(process.env.SUPABASE_DB_PORT || "5432"),
      database: process.env.SUPABASE_DB_NAME || "postgres",
      user: process.env.SUPABASE_DB_USER || "postgres",
      password: dsn ? process.env.SUPABASE_DB_PASSWORD || undefined : required("SUPABASE_DB_PASSWORD"),
      ssl: { rejectUnauthorized: false },
      max: 6,
      idleTimeoutMillis: 10000,
    });
  }
  return global.__bidDashPool;
}
