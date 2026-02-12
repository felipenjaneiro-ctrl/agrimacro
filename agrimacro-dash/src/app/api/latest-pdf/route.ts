import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";

export async function GET() {
  const dir = path.join(process.cwd(), "public", "data", "reports");
  try {
    const files = fs.readdirSync(dir)
      .filter(f => f.startsWith("agrimacro_") && f.endsWith(".pdf"))
      .sort()
      .reverse();
    if (files.length === 0) {
      return NextResponse.json({ error: "No PDF found" }, { status: 404 });
    }
    return NextResponse.redirect(new URL(`/data/reports/${files[0]}`, "http://localhost:3000"));
  } catch {
    return NextResponse.json({ error: "Reports folder not found" }, { status: 500 });
  }
}
