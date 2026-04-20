import { NextRequest, NextResponse } from "next/server";

import { getPool } from "@/lib/db";

export const dynamic = "force-dynamic";

export async function GET(req: NextRequest) {
  const tab = (req.nextUrl.searchParams.get("tab") || "extracted").toLowerCase();

  const pool = getPool();
  let query = "";
  const params: string[] = [];

  if (tab === "extracted") {
    query = `
      select bid_id, reference_no, category, status, llm_confidence, llm_reason, pipeline_source, payload,
             first_seen_at, last_seen_at, resolved_at
      from bid_worklist
      where category = 'EXTRACTED' and status = 'ACTIVE'
      order by first_seen_at desc
      limit 1000
    `;
  } else if (tab === "doubtful") {
    query = `
      select bid_id, reference_no, category, status, llm_confidence, llm_reason, pipeline_source, payload,
             first_seen_at, last_seen_at, resolved_at
      from bid_worklist
      where category = 'DOUBTFUL' and status = 'ACTIVE'
      order by first_seen_at desc
      limit 1000
    `;
  } else {
    query = `
      select bid_id, reference_no, category, status, llm_confidence, llm_reason, pipeline_source, payload,
             first_seen_at, last_seen_at, resolved_at
      from bid_worklist
      where status in ('RESOLVED', 'REJECTED')
      order by coalesce(resolved_at, last_seen_at) desc
      limit 1000
    `;
  }

  try {
    const result = await pool.query(query, params);
    return NextResponse.json({ rows: result.rows });
  } catch (error) {
    const msg = error instanceof Error ? error.message : "Query failed";
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}
