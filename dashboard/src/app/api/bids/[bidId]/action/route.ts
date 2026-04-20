import { NextRequest, NextResponse } from "next/server";

import { getPool } from "@/lib/db";

export const dynamic = "force-dynamic";

export async function PATCH(
  req: NextRequest,
  { params }: { params: Promise<{ bidId: string }> }
) {
  const body = (await req.json().catch(() => ({}))) as { action?: string };
  const action = String(body.action || "").toLowerCase();
  const { bidId } = await params;

  if (!bidId || !["resolve", "reject", "promote"].includes(action)) {
    return NextResponse.json({ error: "Invalid request" }, { status: 400 });
  }

  const pool = getPool();

  try {
    if (action === "resolve") {
      await pool.query(
        `
          update bid_worklist
          set status = 'RESOLVED', resolved_at = now(), last_seen_at = now()
          where bid_id = $1 and category = 'EXTRACTED' and status = 'ACTIVE'
        `,
        [bidId]
      );
      return NextResponse.json({ ok: true });
    }

    if (action === "reject") {
      await pool.query(
        `
          update bid_worklist
          set status = 'REJECTED', resolved_at = now(), last_seen_at = now(), category = 'REJECTED'
          where bid_id = $1 and status = 'ACTIVE'
        `,
        [bidId]
      );
      return NextResponse.json({ ok: true });
    }

    await pool.query(
      `
        update bid_worklist
        set category = 'EXTRACTED', status = 'ACTIVE', resolved_at = null, last_seen_at = now(),
            payload = jsonb_set(payload, '{Final Category}', '"EXTRACTED"', true)
        where bid_id = $1 and category = 'DOUBTFUL' and status = 'ACTIVE'
      `,
      [bidId]
    );
    return NextResponse.json({ ok: true });
  } catch (error) {
    const msg = error instanceof Error ? error.message : "Action failed";
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}
