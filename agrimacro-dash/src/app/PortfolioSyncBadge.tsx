"use client";
import { useEffect, useState } from "react";

type Props = { lastUpdateISO?: string };

export default function PortfolioSyncBadge({ lastUpdateISO }: Props) {
  const [syncing, setSyncing] = useState(false);
  const [msg, setMsg] = useState<string>("");
  const [now, setNow] = useState(Date.now());

  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 60_000);
    return () => clearInterval(t);
  }, []);

  const ageHours = lastUpdateISO
    ? (now - new Date(lastUpdateISO).getTime()) / 3_600_000
    : Infinity;

  const status =
    ageHours < 2 ? { color: "#00C878", text: "AO VIVO", label: "< 2h" } :
    ageHours < 24 ? { color: "#DCB432", text: "RECENTE", label: `${ageHours.toFixed(1)}h` } :
    ageHours < 720 ? { color: "#DC3C3C", text: "DESATUALIZADO", label: `${(ageHours/24).toFixed(0)}d` } :
    { color: "#666", text: "SEM DADOS", label: "—" };

  const handleSync = async () => {
    setSyncing(true);
    setMsg("");
    try {
      const r = await fetch("/api/sync-portfolio", { method: "POST" });
      const d = await r.json();
      if (d.status === "ok") {
        setMsg("Sincronizado!");
        setTimeout(() => window.location.reload(), 1500);
      } else {
        setMsg(`Erro: ${d.error || "desconhecido"}`);
      }
    } catch (e: any) {
      setMsg(`Falha: ${e.message}`);
    } finally {
      setSyncing(false);
    }
  };

  return (
    <div style={{
      display: "flex", alignItems: "center", gap: 12,
      padding: "8px 14px", background: "#142332",
      border: `1px solid ${status.color}`, borderRadius: 6,
      fontSize: 11, fontFamily: "DejaVu Sans, sans-serif"
    }}>
      <span style={{
        display: "inline-block", width: 8, height: 8,
        borderRadius: "50%", background: status.color,
        boxShadow: `0 0 6px ${status.color}`
      }}/>
      <span style={{ color: status.color, fontWeight: 600 }}>
        PORTFOLIO {status.text}
      </span>
      <span style={{ color: "#888" }}>({status.label})</span>
      <button
        onClick={handleSync}
        disabled={syncing}
        style={{
          marginLeft: "auto", padding: "4px 12px",
          background: syncing ? "#333" : "#00C878",
          color: syncing ? "#888" : "#0E1A24",
          border: "none", borderRadius: 4,
          fontSize: 11, fontWeight: 600,
          cursor: syncing ? "wait" : "pointer"
        }}
      >
        {syncing ? "Sincronizando..." : "Sincronizar"}
      </button>
      {msg && <span style={{ color: "#DCB432" }}>{msg}</span>}
    </div>
  );
}
