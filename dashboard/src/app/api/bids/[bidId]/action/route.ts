import { NextRequest, NextResponse } from "next/server";

import { getPool } from "@/lib/db";

export const dynamic = "force-dynamic";

export async function PATCH(
  req: NextRequest,
  { params }: { params: Promise<{ bidId: string }> }
) {
  const body = (await req.json().catch(() => ({}))) as { action?: string; reason?: string };
  const action = String(body.action || "").toLowerCase();
  const reason = String(body.reason || "").trim();
  const { bidId } = await params;

  if (!bidId || !["resolve", "reject", "promote"].includes(action)) {
    return NextResponse.json({ error: "Invalid request" }, { status: 400 });
  }
  if (!reason) {
    return NextResponse.json({ error: "Reason is required" }, { status: 400 });
  }

  const pool = getPool();

  try {
    if (action === "resolve") {
      const result = await pool.query(
        `
          update bid_worklist
          set status = 'RESOLVED', resolved_at = now(), last_seen_at = now(),
              payload = jsonb_set(
                jsonb_set(
                  coalesce(payload, '{}'::jsonb),
                  '{Review Action}', to_jsonb($2::text), true
                ),
                '{Review Reason}', to_jsonb($3::text), true
              )
          where bid_id = $1 and status = 'ACTIVE'
        `,
        [bidId, "TICK", reason]
      );
      if (!result.rowCount) {
        return NextResponse.json({ error: "Bid not found or no longer actionable" }, { status: 409 });
      }
      return NextResponse.json({ ok: true });
    }

    if (action === "reject") {
      const result = await pool.query(
        `
          update bid_worklist
          set status = 'REVIEW_REJECTED', resolved_at = now(), last_seen_at = now(), category = 'REJECTED',
              payload = jsonb_set(
                jsonb_set(
                  coalesce(payload, '{}'::jsonb),
                  '{Review Action}', to_jsonb($2::text), true
                ),
                '{Review Reason}', to_jsonb($3::text), true
              )
          where bid_id = $1 and status = 'ACTIVE'
        `,
        [bidId, "CROSS", reason]
      );
      if (!result.rowCount) {
        return NextResponse.json({ error: "Bid not found or no longer actionable" }, { status: 409 });
      }
      return NextResponse.json({ ok: true });
    }

    const result = await pool.query(
      `
        update bid_worklist
        set category = 'EXTRACTED', status = 'RESOLVED', resolved_at = now(), last_seen_at = now(),
            payload = jsonb_set(
              jsonb_set(
                jsonb_set(
                  coalesce(payload, '{}'::jsonb),
                  '{Final Category}', '"EXTRACTED"', true
                ),
                '{Review Action}', to_jsonb($2::text), true
              ),
              '{Review Reason}', to_jsonb($3::text), true
            )
        where bid_id = $1 and category = 'DOUBTFUL' and status = 'ACTIVE'
      `,
      [bidId, "TICK", reason]
    );
    if (!result.rowCount) {
      return NextResponse.json({ error: "Bid not found or no longer actionable" }, { status: 409 });
    }
    return NextResponse.json({ ok: true });
  } catch (error) {
    console.error("PATCH /api/bids/[bidId]/action failed", error);
    return NextResponse.json({ error: "Action failed" }, { status: 500 });
  }
}
