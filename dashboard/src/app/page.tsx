"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import styles from "./page.module.css";

type TabKey = "extracted" | "doubtful" | "history";

type BidRow = {
  bid_id: string;
  reference_no: string;
  category: string;
  status: string;
  llm_confidence: number | null;
  llm_reason: string | null;
  pipeline_source: string | null;
  payload: Record<string, unknown>;
  first_seen_at: string;
  last_seen_at: string;
  resolved_at: string | null;
};

export default function Home() {
  const [tab, setTab] = useState<TabKey>("extracted");
  const [rows, setRows] = useState<BidRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const title = useMemo(() => {
    if (tab === "extracted") return "Extracted Bids";
    if (tab === "doubtful") return "Doubtful Bids";
    return "History";
  }, [tab]);

  const loadRows = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const res = await fetch(`/api/bids?tab=${tab}`, { cache: "no-store" });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data.error || "Failed to load bids");
      }
      setRows(data.rows || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
      setRows([]);
    } finally {
      setLoading(false);
    }
  }, [tab]);

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    void loadRows();
  }, [loadRows]);

  useEffect(() => {
    const timer = setInterval(() => {
      void loadRows();
    }, 60000);
    return () => clearInterval(timer);
  }, [loadRows]);

  const runAction = async (bidId: string, action: "resolve" | "reject" | "promote") => {
    setError("");
    try {
      const res = await fetch(`/api/bids/${encodeURIComponent(bidId)}/action`, {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ action }),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data.error || "Action failed");
      }
      await loadRows();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown action error");
    }
  };

  return (
    <div className={styles.pageShell}>
      <header className={styles.headerCard}>
        <div>
          <p className={styles.eyebrow}>GEM Cybersecurity Worklist</p>
          <h1 className={styles.pageTitle}>Ops Dashboard</h1>
          <p className={styles.pageSub}>Live queue sourced from extractor runs and Supabase worklist.</p>
        </div>
        <div className={styles.statsBlock}>
          <span className={styles.statsLabel}>Visible</span>
          <strong className={styles.statsValue}>{rows.length}</strong>
        </div>
      </header>

      <main className={styles.mainCard}>
        <div className={styles.tabRow}>
          <button className={`${styles.tab} ${tab === "extracted" ? styles.tabActive : ""}`} onClick={() => setTab("extracted")}>Extracted</button>
          <button className={`${styles.tab} ${tab === "doubtful" ? styles.tabActive : ""}`} onClick={() => setTab("doubtful")}>Doubtful</button>
          <button className={`${styles.tab} ${tab === "history" ? styles.tabActive : ""}`} onClick={() => setTab("history")}>History</button>
        </div>

        <div className={styles.panelTitleRow}>
          <h2>{title}</h2>
          {loading ? <span className={styles.badgeMuted}>Refreshing...</span> : <span className={styles.badgeMuted}>Live</span>}
        </div>

        {error ? <div className={styles.errorBox}>{error}</div> : null}

        <div className={styles.tableWrap}>
          <table className={styles.table}>
            <thead>
              <tr>
                <th>Ref</th>
                <th>Title</th>
                <th>Dept</th>
                <th>Confidence</th>
                <th>Reason</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {!loading && rows.length === 0 ? (
                <tr>
                  <td colSpan={6} className={styles.emptyCell}>No rows in this tab.</td>
                </tr>
              ) : null}
              {rows.map((row) => {
                const payload = row.payload || {};
                const name = String(payload["Name"] || "");
                const dept = String(payload["Department"] || "");
                return (
                  <tr key={row.bid_id}>
                    <td className={styles.refCell}>{row.reference_no}</td>
                    <td>{name}</td>
                    <td>{dept}</td>
                    <td>{row.llm_confidence != null ? row.llm_confidence.toFixed(3) : "-"}</td>
                    <td>{row.llm_reason || "-"}</td>
                    <td>
                      {tab === "extracted" || tab === "doubtful" ? (
                        <div className={styles.inlineActions}>
                          <button
                            className={styles.promoteBtn}
                            onClick={() => void runAction(row.bid_id, tab === "doubtful" ? "promote" : "resolve")}
                          >
                            Tick
                          </button>
                          <button className={styles.rejectBtn} onClick={() => void runAction(row.bid_id, "reject")}>Cross</button>
                        </div>
                      ) : null}
                      {tab === "history" ? (
                        <span className={`${styles.historyLabel} ${row.status === "RESOLVED" ? styles.historyOk : styles.historyBad}`}>
                          {row.status === "RESOLVED" ? "ACCEPTED" : "REJECTED"}
                        </span>
                      ) : null}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </main>
    </div>
  );
}
