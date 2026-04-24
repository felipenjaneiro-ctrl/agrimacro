import { NextRequest, NextResponse } from 'next/server';
import { readFileSync, existsSync } from 'fs';
import { join } from 'path';

const SNAPSHOT_PATH = join(process.cwd(), 'public/data/processed/iv_analytics.json');
const HISTORY_DIR = join(process.cwd(), '..', 'pipeline', 'cache', 'iv_history');
const CACHE_HEADERS = { 'Cache-Control': 'public, max-age=300' };

export async function GET(req: NextRequest) {
  if (!existsSync(SNAPSHOT_PATH)) {
    return NextResponse.json({}, { status: 200, headers: CACHE_HEADERS });
  }

  let snapshot: any;
  try {
    snapshot = JSON.parse(readFileSync(SNAPSHOT_PATH, 'utf8'));
  } catch {
    return NextResponse.json({}, { status: 200, headers: CACHE_HEADERS });
  }

  const sym = req.nextUrl.searchParams.get('commodity');
  const wantHistory = req.nextUrl.searchParams.get('history') === 'true';

  if (!sym) {
    return NextResponse.json(snapshot, { status: 200, headers: CACHE_HEADERS });
  }

  const data = snapshot.commodities?.[sym];
  if (!data) {
    return NextResponse.json({ error: 'not_found', commodity: sym }, { status: 404 });
  }

  const body: any = { as_of: snapshot.as_of, commodity: sym, data };

  if (wantHistory) {
    const historyPath = join(HISTORY_DIR, `${sym}.json`);
    if (existsSync(historyPath)) {
      try {
        const full: any[] = JSON.parse(readFileSync(historyPath, 'utf8'));
        body.history = full.slice(-252);
      } catch {
        // histórico corrompido: ignora silenciosamente
      }
    }
  }

  return NextResponse.json(body, { status: 200, headers: CACHE_HEADERS });
}
