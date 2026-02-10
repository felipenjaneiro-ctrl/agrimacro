"use client";
import { useState, useRef, useEffect } from "react";

type Msg = { role: "user" | "assistant"; content: string };

export default function ChatPanel() {
  const [open, setOpen] = useState(false);
  const [msgs, setMsgs] = useState<Msg[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const endRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [msgs]);

  const send = async () => {
    if (!input.trim() || loading) return;
    const userMsg: Msg = { role: "user", content: input.trim() };
    const newMsgs = [...msgs, userMsg];
    setMsgs(newMsgs);
    setInput("");
    setLoading(true);
    try {
      const res = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ messages: newMsgs }),
      });
      const data = await res.json();
      if (data.error) {
        setMsgs([...newMsgs, { role: "assistant", content: "Erro: " + data.error }]);
      } else {
        setMsgs([...newMsgs, { role: "assistant", content: data.response }]);
      }
    } catch (e: any) {
      setMsgs([...newMsgs, { role: "assistant", content: "Erro: " + e.message }]);
    }
    setLoading(false);
  };

  const C = { bg: "#0d1117", panel: "#161b22", border: "#30363d", text: "#c9d1d9", dim: "#8b949e", amber: "#f59e0b", blue: "#3b82f6" };

  if (!open) {
    return (
      <button onClick={() => setOpen(true)} style={{
        position: "fixed", bottom: 24, right: 24, width: 56, height: 56,
        borderRadius: "50%", background: "linear-gradient(135deg, #d97706, #f59e0b)",
        border: "none", cursor: "pointer", boxShadow: "0 4px 20px rgba(217,119,6,.4)",
        display: "flex", alignItems: "center", justifyContent: "center",
        fontSize: 24, color: "#fff", zIndex: 9999
      }}>{String.fromCharCode(0x1F4AC)}</button>
    );
  }

  return (
    <div style={{
      position: "fixed", bottom: 24, right: 24, width: 420, height: 520,
      background: C.bg, border: "1px solid " + C.border, borderRadius: 12,
      display: "flex", flexDirection: "column", zIndex: 9999,
      boxShadow: "0 8px 40px rgba(0,0,0,.5)"
    }}>
      {/* Header */}
      <div style={{
        padding: "12px 16px", borderBottom: "1px solid " + C.border,
        display: "flex", justifyContent: "space-between", alignItems: "center",
        background: "linear-gradient(135deg, rgba(217,119,6,.1), rgba(245,158,11,.05))"
      }}>
        <div>
          <span style={{ fontSize: 13, fontWeight: 700, color: C.amber }}>Claude</span>
          <span style={{ fontSize: 11, color: C.dim, marginLeft: 8 }}>AgriMacro AI</span>
        </div>
        <button onClick={() => setOpen(false)} style={{
          background: "none", border: "none", color: C.dim, cursor: "pointer", fontSize: 18
        }}>x</button>
      </div>

      {/* Messages */}
      <div style={{ flex: 1, overflowY: "auto", padding: 12, display: "flex", flexDirection: "column", gap: 8 }}>
        {msgs.length === 0 && (
          <div style={{ textAlign: "center", color: C.dim, fontSize: 11, marginTop: 40 }}>
            <div style={{ fontSize: 32, marginBottom: 8 }}>{String.fromCharCode(0x1F33E)}</div>
            <div style={{ fontWeight: 600 }}>AgriMacro AI Assistant</div>
            <div style={{ marginTop: 4 }}>Pergunte sobre mercado, posicoes, spreads...</div>
          </div>
        )}
        {msgs.map((m, i) => (
          <div key={i} style={{
            alignSelf: m.role === "user" ? "flex-end" : "flex-start",
            maxWidth: "85%", padding: "8px 12px", borderRadius: 10,
            fontSize: 12, lineHeight: 1.5, whiteSpace: "pre-wrap",
            background: m.role === "user" ? "rgba(59,130,246,.15)" : "rgba(48,54,61,.6)",
            color: m.role === "user" ? "#93c5fd" : C.text,
            border: m.role === "user" ? "1px solid rgba(59,130,246,.2)" : "1px solid " + C.border
          }}>
            {m.content}
          </div>
        ))}
        {loading && (
          <div style={{ alignSelf: "flex-start", padding: "8px 12px", borderRadius: 10, background: "rgba(48,54,61,.6)", color: C.dim, fontSize: 12, border: "1px solid " + C.border }}>
            Analisando...
          </div>
        )}
        <div ref={endRef} />
      </div>

      {/* Input */}
      <div style={{ padding: 12, borderTop: "1px solid " + C.border, display: "flex", gap: 8 }}>
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && send()}
          placeholder="Pergunte sobre o mercado..."
          style={{
            flex: 1, padding: "8px 12px", borderRadius: 8, border: "1px solid " + C.border,
            background: C.panel, color: C.text, fontSize: 12, outline: "none"
          }}
        />
        <button onClick={send} disabled={loading} style={{
          padding: "8px 16px", borderRadius: 8, border: "none", cursor: "pointer",
          background: loading ? C.border : C.amber, color: loading ? C.dim : "#000",
          fontWeight: 700, fontSize: 12
        }}>
          {String.fromCharCode(0x27A4)}
        </button>
      </div>
    </div>
  );
}