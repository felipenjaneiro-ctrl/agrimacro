"use client";
import { useState, useEffect, useRef } from "react";

/* ═══════════════════════════════════════════════════════════════════════
   VideoScriptPanel — Painel lateral para exibir o roteiro de vídeo
   Estilo consistente com dashboard.tsx e ChatPanel.tsx
   ═══════════════════════════════════════════════════════════════════════ */

interface ScriptBlock {
  id: string;
  title: string;
  duration_sec: number;
  narration: string;
  visual_notes: string;
  data_refs: string[];
}

interface VideoScript {
  date: string;
  weekday: string;
  duration_target_min: number;
  blocks: ScriptBlock[];
  total_words: number;
  total_duration_sec: number;
  sources_cited: string[];
}

const C = {
  bg: "#0d1117", panel: "#161b22", panelAlt: "#1c2128", border: "rgba(148,163,184,.12)",
  text: "#e2e8f0", textDim: "#94a3b8", textMuted: "#64748b",
  green: "#22c55e", red: "#ef4444", amber: "#f59e0b", blue: "#3b82f6",
  cyan: "#06b6d4", purple: "#a78bfa",
  greenBg: "rgba(34,197,94,.12)", amberBg: "rgba(245,158,11,.12)", blueBg: "rgba(59,130,246,.12)",
  greenBorder: "rgba(34,197,94,.3)", amberBorder: "rgba(245,158,11,.3)",
};

const BLOCK_COLORS: Record<string, string> = {
  abertura: "#f59e0b", graos: "#22c55e", softs: "#a855f7", pecuaria: "#ef4444",
  energia: "#3b82f6", macro: "#06b6d4", cot: "#ec4899", spreads: "#fbbf24",
  stocks: "#10b981", fechamento: "#f59e0b", encerramento: "#f59e0b",
};

function getBlockColor(id: string): string {
  for (const key of Object.keys(BLOCK_COLORS)) {
    if (id.toLowerCase().includes(key)) return BLOCK_COLORS[key];
  }
  return C.textDim;
}

function formatDuration(sec: number): string {
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  if (m === 0) return `${s}s`;
  if (s === 0) return `${m}min`;
  return `${m}:${s.toString().padStart(2, "0")}`;
}

export default function VideoScriptPanel() {
  const [open, setOpen] = useState(false);
  const [script, setScript] = useState<VideoScript | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedVisuals, setExpandedVisuals] = useState<Record<string, boolean>>({});
  const [copied, setCopied] = useState<string | null>(null);
  const panelRef = useRef<HTMLDivElement>(null);

  // Fetch script when panel opens
  useEffect(() => {
    if (!open || script) return;
    setLoading(true);
    fetch("/data/processed/video_script.json?t=" + Date.now())
      .then(r => {
        if (!r.ok) throw new Error("video_script.json não encontrado");
        return r.json();
      })
      .then(d => { setScript(d); setError(null); })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, [open]);

  const copyToClipboard = (text: string, label: string) => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(label);
      setTimeout(() => setCopied(null), 2000);
    });
  };

  const copyAllNarrations = () => {
    if (!script) return;
    const all = script.blocks.map(b => `## ${b.title}\n\n${b.narration}`).join("\n\n---\n\n");
    const header = `# Roteiro AgriMacro — ${script.date} (${script.weekday})\nDuração: ${formatDuration(script.total_duration_sec)} | ${script.total_words} palavras\n\n---\n\n`;
    copyToClipboard(header + all, "all");
  };

  const copyJson = () => {
    if (!script) return;
    copyToClipboard(JSON.stringify(script, null, 2), "json");
  };

  const toggleVisual = (id: string) => {
    setExpandedVisuals(prev => ({ ...prev, [id]: !prev[id] }));
  };

  // Floating button
  if (!open) {
    return (
      <button onClick={() => setOpen(true)} title="Video Script" style={{
        position: "fixed", bottom: 96, right: 24, width: 56, height: 56,
        borderRadius: "50%", background: "linear-gradient(135deg, #7c3aed, #a78bfa)",
        border: "none", cursor: "pointer", boxShadow: "0 4px 20px rgba(124,58,237,.4)",
        display: "flex", alignItems: "center", justifyContent: "center",
        fontSize: 24, color: "#fff", zIndex: 9998,
        transition: "transform .15s, box-shadow .15s",
      }}
        onMouseEnter={e => { e.currentTarget.style.transform = "scale(1.08)"; e.currentTarget.style.boxShadow = "0 6px 28px rgba(124,58,237,.5)"; }}
        onMouseLeave={e => { e.currentTarget.style.transform = "scale(1)"; e.currentTarget.style.boxShadow = "0 4px 20px rgba(124,58,237,.4)"; }}
      >
        🎬
      </button>
    );
  }

  // Panel
  return (
    <div ref={panelRef} style={{
      position: "fixed", top: 0, right: 0, width: 520, height: "100vh",
      background: C.bg, borderLeft: `1px solid ${C.border}`,
      display: "flex", flexDirection: "column", zIndex: 9998,
      boxShadow: "-8px 0 40px rgba(0,0,0,.5)",
      animation: "slideIn .2s ease-out",
    }}>
      <style>{`@keyframes slideIn{from{transform:translateX(100%)}to{transform:translateX(0)}}`}</style>

      {/* Header */}
      <div style={{
        padding: "16px 20px", borderBottom: `1px solid ${C.border}`,
        background: "linear-gradient(135deg, rgba(124,58,237,.1), rgba(167,139,250,.05))",
        flexShrink: 0,
      }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <span style={{ fontSize: 22 }}>🎬</span>
            <div>
              <span style={{ fontSize: 15, fontWeight: 700, color: C.purple }}>Video Script</span>
              <span style={{ fontSize: 11, color: C.textMuted, marginLeft: 8 }}>AgriMacro</span>
            </div>
          </div>
          <button onClick={() => setOpen(false)} style={{
            background: "none", border: "none", color: C.textMuted, cursor: "pointer",
            fontSize: 20, padding: "4px 8px", borderRadius: 4,
          }}
            onMouseEnter={e => e.currentTarget.style.color = C.text}
            onMouseLeave={e => e.currentTarget.style.color = C.textMuted}
          >✕</button>
        </div>

        {script && (
          <>
            {/* Meta info */}
            <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginBottom: 12 }}>
              <span style={{ fontSize: 10, padding: "3px 10px", borderRadius: 12, background: C.amberBg, border: `1px solid ${C.amberBorder}`, color: C.amber, fontWeight: 600 }}>
                📅 {script.date} — {script.weekday}
              </span>
              <span style={{ fontSize: 10, padding: "3px 10px", borderRadius: 12, background: C.blueBg, border: "1px solid rgba(59,130,246,.3)", color: C.blue, fontWeight: 600 }}>
                ⏱ {formatDuration(script.total_duration_sec)}
              </span>
              <span style={{ fontSize: 10, padding: "3px 10px", borderRadius: 12, background: "rgba(148,163,184,.08)", border: "1px solid rgba(148,163,184,.15)", color: C.textDim, fontWeight: 600 }}>
                📝 {script.total_words} palavras
              </span>
              <span style={{ fontSize: 10, padding: "3px 10px", borderRadius: 12, background: "rgba(148,163,184,.08)", border: "1px solid rgba(148,163,184,.15)", color: C.textDim, fontWeight: 600 }}>
                🎯 {script.blocks.length} blocos
              </span>
            </div>

            {/* Action buttons */}
            <div style={{ display: "flex", gap: 8 }}>
              <button onClick={copyAllNarrations} style={{
                padding: "6px 14px", fontSize: 10, fontWeight: 600, borderRadius: 6, cursor: "pointer",
                background: copied === "all" ? C.greenBg : "rgba(167,139,250,.12)",
                color: copied === "all" ? C.green : C.purple,
                border: `1px solid ${copied === "all" ? C.greenBorder : "rgba(167,139,250,.3)"}`,
                transition: "all .2s",
              }}>
                {copied === "all" ? "✓ Copiado!" : "📋 Copiar Tudo"}
              </button>
              <button onClick={copyJson} style={{
                padding: "6px 14px", fontSize: 10, fontWeight: 600, borderRadius: 6, cursor: "pointer",
                background: copied === "json" ? C.greenBg : "rgba(148,163,184,.06)",
                color: copied === "json" ? C.green : C.textDim,
                border: `1px solid ${copied === "json" ? C.greenBorder : "rgba(148,163,184,.15)"}`,
                transition: "all .2s",
              }}>
                {copied === "json" ? "✓ Copiado!" : "{ } Copiar JSON"}
              </button>
            </div>
          </>
        )}
      </div>

      {/* Content */}
      <div style={{ flex: 1, overflowY: "auto", padding: "16px 20px" }}>
        {loading && (
          <div style={{ display: "flex", alignItems: "center", justifyContent: "center", padding: 60 }}>
            <div style={{ width: 32, height: 32, border: `3px solid ${C.border}`, borderTopColor: C.purple, borderRadius: "50%", animation: "spin 1s linear infinite" }} />
            <style>{`@keyframes spin{to{transform:rotate(360deg)}}`}</style>
          </div>
        )}

        {error && (
          <div style={{ padding: 40, textAlign: "center", background: C.panelAlt, borderRadius: 8, border: `1px dashed ${C.border}` }}>
            <div style={{ fontSize: 28, marginBottom: 8 }}>📹</div>
            <div style={{ fontSize: 14, fontWeight: 600, color: C.textDim, marginBottom: 6 }}>Script não encontrado</div>
            <div style={{ fontSize: 11, color: C.textMuted, maxWidth: 300, margin: "0 auto" }}>
              Execute o Step 17 do pipeline para gerar <code style={{ color: C.purple }}>video_script.json</code>
            </div>
          </div>
        )}

        {script && script.blocks.map((block, idx) => {
          const color = getBlockColor(block.id);
          const isVisualOpen = expandedVisuals[block.id];
          const blockCopyKey = "block-" + block.id;

          return (
            <div key={block.id} style={{
              marginBottom: 14, background: C.panelAlt, borderRadius: 10,
              border: `1px solid ${C.border}`, borderLeft: `4px solid ${color}`,
              overflow: "hidden",
            }}>
              {/* Block header */}
              <div style={{
                padding: "12px 16px", display: "flex", justifyContent: "space-between",
                alignItems: "center", borderBottom: `1px solid ${C.border}`,
              }}>
                <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                  <span style={{
                    fontSize: 10, fontWeight: 800, color: C.textMuted, fontFamily: "monospace",
                    background: "rgba(148,163,184,.08)", padding: "2px 8px", borderRadius: 4,
                  }}>{(idx + 1).toString().padStart(2, "0")}</span>
                  <span style={{ fontSize: 14, fontWeight: 700, color: C.text }}>{block.title}</span>
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <span style={{
                    fontSize: 10, padding: "2px 8px", borderRadius: 10,
                    background: color + "18", color: color, fontWeight: 700, fontFamily: "monospace",
                  }}>
                    {formatDuration(block.duration_sec)}
                  </span>
                  <button
                    onClick={() => copyToClipboard(block.narration, blockCopyKey)}
                    title="Copiar narração"
                    style={{
                      background: copied === blockCopyKey ? C.greenBg : "rgba(148,163,184,.06)",
                      border: `1px solid ${copied === blockCopyKey ? C.greenBorder : "rgba(148,163,184,.12)"}`,
                      color: copied === blockCopyKey ? C.green : C.textMuted,
                      cursor: "pointer", borderRadius: 4, padding: "3px 8px", fontSize: 10,
                      transition: "all .2s", fontWeight: 600,
                    }}
                  >
                    {copied === blockCopyKey ? "✓" : "📋"}
                  </button>
                </div>
              </div>

              {/* Narration */}
              <div style={{
                padding: "14px 16px", fontSize: 13, color: C.text,
                lineHeight: 1.7, whiteSpace: "pre-wrap",
              }}>
                {block.narration}
              </div>

              {/* Visual notes (collapsible) */}
              {block.visual_notes && (
                <div style={{ borderTop: `1px solid ${C.border}` }}>
                  <button onClick={() => toggleVisual(block.id)} style={{
                    width: "100%", padding: "8px 16px", background: "none", border: "none",
                    cursor: "pointer", display: "flex", alignItems: "center", gap: 6,
                    color: C.textMuted, fontSize: 10, textAlign: "left",
                  }}>
                    <span style={{ fontSize: 8, transition: "transform .15s", transform: isVisualOpen ? "rotate(90deg)" : "rotate(0)" }}>▶</span>
                    <span>🎨 Notas visuais</span>
                  </button>
                  {isVisualOpen && (
                    <div style={{
                      padding: "0 16px 12px", fontSize: 11, color: C.textMuted, lineHeight: 1.6,
                      fontStyle: "italic",
                    }}>
                      {block.visual_notes}
                    </div>
                  )}
                </div>
              )}

              {/* Data refs */}
              {block.data_refs && block.data_refs.length > 0 && (
                <div style={{
                  padding: "6px 16px 10px", display: "flex", gap: 6, flexWrap: "wrap",
                }}>
                  {block.data_refs.map((ref, ri) => (
                    <span key={ri} style={{
                      fontSize: 9, padding: "1px 6px", borderRadius: 3,
                      background: "rgba(148,163,184,.06)", color: C.textMuted,
                      fontFamily: "monospace",
                    }}>{ref}</span>
                  ))}
                </div>
              )}
            </div>
          );
        })}

        {/* Sources footer */}
        {script && script.sources_cited && script.sources_cited.length > 0 && (
          <div style={{
            padding: "12px 16px", borderRadius: 8, marginTop: 8,
            background: "rgba(148,163,184,.03)", border: `1px solid rgba(148,163,184,.08)`,
            fontSize: 9, color: C.textMuted,
          }}>
            <strong>Fontes:</strong> {script.sources_cited.join(" • ")}
          </div>
        )}

        {/* Timeline bar */}
        {script && (
          <div style={{ marginTop: 16, padding: "12px 16px", background: C.panelAlt, borderRadius: 8, border: `1px solid ${C.border}` }}>
            <div style={{ fontSize: 10, fontWeight: 700, color: C.textDim, marginBottom: 8, letterSpacing: 0.5 }}>TIMELINE</div>
            <div style={{ display: "flex", height: 24, borderRadius: 6, overflow: "hidden", gap: 2 }}>
              {script.blocks.map(block => {
                const pct = (block.duration_sec / script.total_duration_sec) * 100;
                const color = getBlockColor(block.id);
                return (
                  <div key={block.id} title={`${block.title} — ${formatDuration(block.duration_sec)}`} style={{
                    flex: `0 0 ${pct}%`, background: color + "30", borderTop: `3px solid ${color}`,
                    display: "flex", alignItems: "center", justifyContent: "center",
                    fontSize: 8, color: C.textMuted, fontWeight: 600, overflow: "hidden",
                    cursor: "default", transition: "background .15s",
                  }}
                    onMouseEnter={e => e.currentTarget.style.background = color + "50"}
                    onMouseLeave={e => e.currentTarget.style.background = color + "30"}
                  >
                    {pct > 8 ? block.title.slice(0, 12) : ""}
                  </div>
                );
              })}
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4, fontSize: 9, color: C.textMuted }}>
              <span>0:00</span>
              <span>{formatDuration(script.total_duration_sec)}</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
