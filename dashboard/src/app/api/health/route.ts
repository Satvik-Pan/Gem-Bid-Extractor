import { NextResponse } from "next/server";

import { getPool } from "@/lib/db";

export const dynamic = "force-dynamic";

export async function GET() {
  const startedAt = Date.now();
  try {
    const pool = getPool();
    const result = await pool.query("select now() as db_now");
    return NextResponse.json({
      status: "ok",
      db: "reachable",
      db_now: result.rows[0]?.db_now ?? null,
      latency_ms: Date.now() - startedAt,
    });
  } catch (error) {
    console.error("GET /api/health failed", error);
    return NextResponse.json(
      { status: "error", db: "unreachable", latency_ms: Date.now() - startedAt },
      { status: 500 }
    );
  }
}
