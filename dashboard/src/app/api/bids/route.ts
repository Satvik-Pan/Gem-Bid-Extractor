import { NextRequest, NextResponse } from "next/server";

import { getPool } from "@/lib/db";

export const dynamic = "force-dynamic";
const VALID_TABS = new Set(["extracted", "doubtful", "history"]);

export async function GET(req: NextRequest) {
  const tab = (req.nextUrl.searchParams.get("tab") || "extracted").toLowerCase();
  if (!VALID_TABS.has(tab)) {
    return NextResponse.json({ error: "Invalid tab value" }, { status: 400 });
  }

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
      where status in ('RESOLVED', 'REVIEW_REJECTED')
      order by coalesce(resolved_at, last_seen_at) desc
      limit 1000
    `;
  }

  try {
    const result = await pool.query(query, params);
    return NextResponse.json({ rows: result.rows });
  } catch (error) {
    console.error("GET /api/bids failed", error);
    return NextResponse.json({ error: "Query failed" }, { status: 500 });
  }
}
