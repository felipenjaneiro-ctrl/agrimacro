#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AgriMacro v3.2 - Step 18: Video MP4 Generator
Le video_script.json (Step 17) + PDF do relatorio diario.
Gera video narrado com TTS (edge-tts) e imagens do PDF como fundo.
Output: reports/AgriMacro_YYYY-MM-DD.mp4

Dependencias:
  pip install edge-tts pdf2image Pillow
  Sistema: ffmpeg, poppler (poppler-utils no Linux, poppler no Windows via choco/scoop)
"""
import json
import os
import sys
import glob
import subprocess
import tempfile
import shutil
import asyncio
import re
from datetime import datetime
from pathlib import Path

# ── CONFIG ──
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJ_DIR = os.path.join(BASE_DIR, "..")
DATA_PROC = os.path.join(PROJ_DIR, "agrimacro-dash", "public", "data", "processed")
REPORT_DIR_DASH = os.path.join(PROJ_DIR, "agrimacro-dash", "public", "data", "reports")
REPORT_DIR = os.path.join(PROJ_DIR, "reports")
TODAY_STR = datetime.now().strftime("%Y-%m-%d")
OUTPUT_MP4 = os.path.join(REPORT_DIR, f"AgriMacro_{TODAY_STR}.mp4")

TTS_VOICE = "pt-BR-AntonioNeural"
VIDEO_W = 1920
VIDEO_H = 1080
VIDEO_FPS = 30

# ── Mapeamento palavras-chave → titulos de pagina do PDF ──
# Generico: funciona com qualquer numero de paginas.
# O mapeamento e feito por palavras-chave no titulo do bloco/narracao
# → palavras-chave nos titulos das paginas do PDF.
KEYWORD_TO_PDF_TOPICS = {
    # Palavras no bloco do roteiro → topicos que devem aparecer no PDF
    "soja": ["preco", "grao", "oleaginosa", "spread", "cot", "crush"],
    "milho": ["preco", "grao", "oleaginosa", "spread", "cot", "variacao"],
    "trigo": ["preco", "grao", "oleaginosa", "spread", "cot"],
    "graos": ["preco", "grao", "oleaginosa", "spread", "variacao"],
    "oleaginosa": ["preco", "grao", "oleaginosa", "spread"],
    "boi": ["preco", "carne", "energia", "metal", "fisico", "spread", "confinamento"],
    "gado": ["preco", "carne", "energia", "metal", "fisico", "spread"],
    "pecuaria": ["preco", "carne", "energia", "metal", "fisico"],
    "porco": ["preco", "carne", "energia", "metal", "fisico"],
    "cafe": ["preco", "grao", "oleaginosa", "variacao"],
    "acucar": ["preco", "grao", "oleaginosa", "variacao"],
    "algodao": ["preco", "grao", "oleaginosa", "variacao"],
    "cacau": ["preco", "carne", "energia", "metal", "variacao"],
    "macro": ["macro", "cambio", "brasil"],
    "cambio": ["macro", "cambio", "brasil"],
    "dolar": ["macro", "cambio", "brasil"],
    "juros": ["macro", "cambio", "brasil"],
    "energia": ["energia", "eia"],
    "petroleo": ["energia", "eia"],
    "diesel": ["energia", "eia"],
    "etanol": ["energia", "eia"],
    "clima": ["clima", "safra"],
    "safra": ["clima", "safra"],
    "chuva": ["clima", "safra"],
    "tempo": ["clima", "safra"],
    "cot": ["cot", "cftc", "posicionamento"],
    "cftc": ["cot", "cftc", "posicionamento"],
    "estoque": ["estoque", "fundamento"],
    "usda": ["estoque", "fundamento", "variacao"],
    "noticia": ["noticia", "contexto"],
    "calendario": ["calendario", "agenda", "evento"],
    "agenda": ["calendario", "agenda", "evento"],
    "abertura": ["capa", "resumo", "preco", "grao"],
    "encerramento": ["calendario", "agenda", "capa", "glossario"],
    "spread": ["spread", "termometro", "regime"],
    "fisico": ["fisico", "brasil", "internacional"],
}


def sload(name):
    p = os.path.join(DATA_PROC, name)
    if not os.path.exists(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def find_pdf():
    """Find today's PDF report"""
    candidates = [
        os.path.join(REPORT_DIR_DASH, f"agrimacro_{TODAY_STR}.pdf"),
        os.path.join(REPORT_DIR, f"agrimacro_{TODAY_STR}.pdf"),
    ]
    # Also try glob for any recent PDF
    for pattern_dir in [REPORT_DIR_DASH, REPORT_DIR]:
        candidates += sorted(glob.glob(os.path.join(pattern_dir, "agrimacro_*.pdf")), reverse=True)
    for c in candidates:
        if os.path.exists(c):
            return c
    return None


def pdf_to_images(pdf_path, output_dir, dpi=150):
    """Convert PDF pages to PNG images using pdf2image"""
    from pdf2image import convert_from_path
    print(f"  Convertendo PDF em imagens (DPI={dpi})...")
    images = convert_from_path(pdf_path, dpi=dpi, fmt="png", size=(VIDEO_W, None))
    paths = []
    for i, img in enumerate(images):
        # Resize/pad to exact 1920x1080
        from PIL import Image
        canvas = Image.new("RGB", (VIDEO_W, VIDEO_H), (15, 17, 23))  # BG color #0f1117
        # Scale image to fit
        img_w, img_h = img.size
        scale = min(VIDEO_W / img_w, VIDEO_H / img_h)
        new_w = int(img_w * scale)
        new_h = int(img_h * scale)
        img_resized = img.resize((new_w, new_h), Image.LANCZOS)
        # Center on canvas
        x_off = (VIDEO_W - new_w) // 2
        y_off = (VIDEO_H - new_h) // 2
        canvas.paste(img_resized, (x_off, y_off))
        out_path = os.path.join(output_dir, f"page_{i+1:03d}.png")
        canvas.save(out_path, "PNG")
        paths.append(out_path)
    print(f"  {len(paths)} paginas convertidas para PNG")
    return paths


def extract_page_titles_from_pdf(page_images):
    """
    Build a simple mapping: page_number (1-based) → lowercase keywords.
    Based on the known PDF structure from generate_report_pdf.py.
    This is a heuristic that works with the enhanced 15-page PDF.
    """
    # Known page structure for the enhanced PDF (15 pages)
    # But we make it flexible: map page index to likely topic keywords
    page_topic_map = {}
    known_pages = [
        # (page_num, keywords list)
        (1, ["capa", "resumo", "executivo", "destaque"]),
        (2, ["macro", "cambio", "brasil", "brl", "selic", "juros"]),
        (3, ["preco", "grao", "oleaginosa", "soja", "milho", "trigo", "cafe", "acucar", "algodao"]),
        (4, ["preco", "carne", "energia", "metal", "boi", "porco", "petroleo", "ouro", "cacau"]),
        (5, ["variacao", "tabela", "1d", "1s", "1m", "52w"]),
        (6, ["spread", "termometro", "regime", "percentil", "zscore"]),
        (7, ["spread", "regime", "detalhamento", "analise"]),
        (8, ["estoque", "fundamento", "usda", "stocks"]),
        (9, ["cot", "cftc", "posicionamento", "commercial", "managed"]),
        (10, ["energia", "eia", "petroleo", "gas", "etanol", "diesel", "refino"]),
        (11, ["fisico", "brasil", "internacional", "cepea", "esalq"]),
        (12, ["calendario", "agenda", "evento", "release"]),
        (13, ["clima", "safra", "chuva", "temperatura", "enso"]),
        (14, ["noticia", "contexto", "news"]),
        (15, ["glossario", "termos", "explicacao"]),
    ]
    total_pages = len(page_images)
    for pg_num, keywords in known_pages:
        if pg_num <= total_pages:
            page_topic_map[pg_num] = keywords
    # If PDF has fewer pages (old version with 11), adapt
    if total_pages <= 11:
        page_topic_map = {
            1: ["capa", "resumo", "executivo", "destaque"],
            2: ["macro", "cambio", "brasil", "brl", "selic"],
            3: ["preco", "grao", "oleaginosa", "soja", "milho", "variacao"],
            4: ["spread", "termometro", "regime"],
            5: ["estoque", "fundamento", "usda"],
            6: ["cot", "cftc", "posicionamento"],
            7: ["energia", "eia", "petroleo"],
            8: ["fisico", "brasil", "internacional"],
            9: ["calendario", "agenda", "evento"],
            10: ["clima", "safra"],
            11: ["noticia", "contexto"],
        }
    return page_topic_map


def map_block_to_pages(block, page_topic_map, total_pages):
    """
    Given a video script block, find the best PDF pages to show.
    Returns list of page numbers (1-based).
    """
    # Combine block title + narration for keyword matching
    text = (block.get("title", "") + " " + block.get("narration", "") + " " + block.get("id", "")).lower()
    # Remove accents for simpler matching
    text = text.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    text = text.replace("ã", "a").replace("õ", "o").replace("â", "a").replace("ê", "e").replace("ç", "c")

    # Find matching PDF topics via keywords
    matched_topics = set()
    for keyword, topics in KEYWORD_TO_PDF_TOPICS.items():
        if keyword in text:
            matched_topics.update(topics)

    if not matched_topics:
        # Default: show cover page
        return [1]

    # Score each page by how many topic keywords match
    page_scores = {}
    for pg_num, pg_keywords in page_topic_map.items():
        score = len(set(pg_keywords) & matched_topics)
        if score > 0:
            page_scores[pg_num] = score

    if not page_scores:
        return [1]

    # Return top pages sorted by score (max 4 pages per block)
    sorted_pages = sorted(page_scores.keys(), key=lambda p: page_scores[p], reverse=True)
    return sorted_pages[:4]


async def generate_tts_async(text, output_path, voice=TTS_VOICE):
    """Generate TTS audio using edge-tts"""
    import edge_tts
    communicate = edge_tts.Communicate(text, voice)
    await communicate.save(output_path)


def generate_tts(text, output_path, voice=TTS_VOICE):
    """Sync wrapper for edge-tts"""
    asyncio.run(generate_tts_async(text, output_path, voice))


def get_audio_duration(audio_path):
    """Get duration of audio file in seconds using ffprobe"""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", audio_path],
            capture_output=True, text=True, timeout=30
        )
        return float(result.stdout.strip())
    except Exception:
        return 30.0  # fallback


def create_segment_video(image_path, audio_path, output_path, duration):
    """Create a video segment: static image + audio for given duration"""
    cmd = [
        "ffmpeg", "-y",
        "-loop", "1", "-i", image_path,
        "-i", audio_path,
        "-c:v", "libx264", "-tune", "stillimage",
        "-c:a", "aac", "-b:a", "192k",
        "-vf", f"scale={VIDEO_W}:{VIDEO_H}:force_original_aspect_ratio=decrease,pad={VIDEO_W}:{VIDEO_H}:(ow-iw)/2:(oh-ih)/2:color=0x0f1117",
        "-pix_fmt", "yuv420p",
        "-r", str(VIDEO_FPS),
        "-shortest",
        "-t", str(duration + 0.5),  # small buffer
        output_path
    ]
    subprocess.run(cmd, capture_output=True, timeout=120)


def create_slideshow_segment(image_paths, audio_path, output_path, total_duration):
    """
    Create a video segment with multiple images (slideshow) + audio.
    Each image gets equal time.
    """
    if not image_paths:
        return
    n_images = len(image_paths)
    duration_per_image = max(2.0, total_duration / n_images)

    # Create concat file for images
    tmp_dir = os.path.dirname(output_path)
    concat_file = os.path.join(tmp_dir, "img_concat.txt")
    with open(concat_file, "w") as f:
        for img_path in image_paths:
            # ffmpeg concat demuxer format
            f.write(f"file '{img_path}'\n")
            f.write(f"duration {duration_per_image:.2f}\n")
        # Repeat last image (ffmpeg concat quirk)
        f.write(f"file '{image_paths[-1]}'\n")

    cmd = [
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0", "-i", concat_file,
        "-i", audio_path,
        "-c:v", "libx264", "-tune", "stillimage",
        "-c:a", "aac", "-b:a", "192k",
        "-vf", f"scale={VIDEO_W}:{VIDEO_H}:force_original_aspect_ratio=decrease,pad={VIDEO_W}:{VIDEO_H}:(ow-iw)/2:(oh-ih)/2:color=0x0f1117",
        "-pix_fmt", "yuv420p",
        "-r", str(VIDEO_FPS),
        "-shortest",
        output_path
    ]
    subprocess.run(cmd, capture_output=True, timeout=180)

    # Cleanup
    if os.path.exists(concat_file):
        os.remove(concat_file)


def generate_srt(blocks_timing, output_path):
    """
    Generate SRT subtitle file from blocks.
    blocks_timing: list of (start_sec, end_sec, narration_text)
    """
    with open(output_path, "w", encoding="utf-8") as f:
        idx = 1
        for start_sec, end_sec, narration in blocks_timing:
            # Split narration into sentences for better subtitles
            sentences = re.split(r'(?<=[.!?])\s+', narration)
            if not sentences:
                continue
            sent_duration = (end_sec - start_sec) / max(len(sentences), 1)
            for i, sent in enumerate(sentences):
                sent = sent.strip()
                if not sent:
                    continue
                s_start = start_sec + i * sent_duration
                s_end = min(start_sec + (i + 1) * sent_duration, end_sec)
                # Format timestamps
                f.write(f"{idx}\n")
                f.write(f"{format_srt_time(s_start)} --> {format_srt_time(s_end)}\n")
                # Line break at ~45 chars for readability
                if len(sent) > 45:
                    mid = len(sent) // 2
                    # Find nearest space
                    space_pos = sent.rfind(" ", 0, mid + 10)
                    if space_pos > mid - 15:
                        sent = sent[:space_pos] + "\n" + sent[space_pos + 1:]
                f.write(f"{sent}\n\n")
                idx += 1


def format_srt_time(seconds):
    """Format seconds to SRT timestamp: HH:MM:SS,mmm"""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    ms = int((seconds % 1) * 1000)
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


def burn_subtitles(input_video, srt_path, output_video):
    """Burn SRT subtitles into video using ffmpeg"""
    # Escape path for ffmpeg subtitles filter (Windows needs special handling)
    srt_escaped = srt_path.replace("\\", "/").replace(":", "\\:")
    cmd = [
        "ffmpeg", "-y",
        "-i", input_video,
        "-vf", f"subtitles='{srt_escaped}':force_style='FontSize=22,FontName=Arial,PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,Outline=2,Shadow=1,MarginV=40'",
        "-c:v", "libx264", "-crf", "23",
        "-c:a", "copy",
        "-pix_fmt", "yuv420p",
        output_video
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        # Fallback: try without subtitle escaping
        cmd2 = [
            "ffmpeg", "-y",
            "-i", input_video,
            "-vf", f"subtitles={srt_path}:force_style='FontSize=22,FontName=Arial,PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,Outline=2,Shadow=1,MarginV=40'",
            "-c:v", "libx264", "-crf", "23",
            "-c:a", "copy",
            "-pix_fmt", "yuv420p",
            output_video
        ]
        subprocess.run(cmd2, capture_output=True, text=True, timeout=600)


def concatenate_videos(segment_paths, output_path):
    """Concatenate video segments using ffmpeg concat demuxer"""
    tmp_dir = os.path.dirname(output_path)
    concat_file = os.path.join(tmp_dir, "video_concat.txt")
    with open(concat_file, "w") as f:
        for seg in segment_paths:
            f.write(f"file '{seg}'\n")

    cmd = [
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0", "-i", concat_file,
        "-c:v", "libx264", "-crf", "23",
        "-c:a", "aac", "-b:a", "192k",
        "-pix_fmt", "yuv420p",
        "-r", str(VIDEO_FPS),
        output_path
    ]
    subprocess.run(cmd, capture_output=True, timeout=600)

    if os.path.exists(concat_file):
        os.remove(concat_file)


def main():
    print("=" * 60)
    print("AgriMacro v3.2 - Step 18: Video MP4 Generator")
    print("=" * 60)

    # ── 1. Load video script ──
    script = sload("video_script.json")
    if not script or not script.get("blocks"):
        print("  [ERROR] video_script.json nao encontrado ou sem blocos.")
        print("  Execute Step 17 primeiro.")
        sys.exit(1)

    blocks = script["blocks"]
    print(f"  [OK] Script carregado: {len(blocks)} blocos, ~{script.get('total_words', 0)} palavras")

    # ── 2. Find PDF ──
    pdf_path = find_pdf()
    if not pdf_path:
        print("  [ERROR] PDF do relatorio nao encontrado.")
        print("  Execute Step 16 primeiro.")
        sys.exit(1)
    print(f"  [OK] PDF: {pdf_path}")

    # ── 3. Create temp directory ──
    tmp_dir = tempfile.mkdtemp(prefix="agrimacro_video_")
    print(f"  [OK] Temp dir: {tmp_dir}")

    try:
        # ── 4. Convert PDF to images ──
        page_images = pdf_to_images(pdf_path, tmp_dir)
        total_pages = len(page_images)
        print(f"  [OK] {total_pages} paginas PNG geradas")

        # ── 5. Build page topic map ──
        page_topic_map = extract_page_titles_from_pdf(page_images)

        # ── 6. Process each block ──
        segment_paths = []
        blocks_timing = []
        current_time = 0.0

        for bi, block in enumerate(blocks):
            block_id = block.get("id", f"block_{bi}")
            title = block.get("title", "")
            narration = block.get("narration", "")
            print(f"\n  Bloco {bi+1}/{len(blocks)}: [{block_id}] {title}")

            if not narration.strip():
                print(f"    [SKIP] Sem narracao")
                continue

            # 6a. Generate TTS audio
            audio_path = os.path.join(tmp_dir, f"audio_{bi:03d}.mp3")
            print(f"    TTS gerando audio...")
            try:
                generate_tts(narration, audio_path)
            except Exception as e:
                print(f"    [ERROR] TTS falhou: {e}")
                continue

            if not os.path.exists(audio_path) or os.path.getsize(audio_path) < 100:
                print(f"    [ERROR] Audio vazio ou nao gerado")
                continue

            # 6b. Get audio duration
            audio_duration = get_audio_duration(audio_path)
            print(f"    Audio: {audio_duration:.1f}s")

            # 6c. Map block to PDF pages
            matched_pages = map_block_to_pages(block, page_topic_map, total_pages)
            matched_image_paths = []
            for pg in matched_pages:
                if pg <= total_pages:
                    matched_image_paths.append(page_images[pg - 1])
            if not matched_image_paths:
                matched_image_paths = [page_images[0]]  # fallback to cover
            print(f"    Paginas PDF: {matched_pages}")

            # 6d. Create video segment
            segment_path = os.path.join(tmp_dir, f"segment_{bi:03d}.mp4")
            if len(matched_image_paths) == 1:
                # Single image: static
                create_segment_video(matched_image_paths[0], audio_path, segment_path, audio_duration)
            else:
                # Multiple images: slideshow
                create_slideshow_segment(matched_image_paths, audio_path, segment_path, audio_duration)

            if os.path.exists(segment_path) and os.path.getsize(segment_path) > 1000:
                segment_paths.append(segment_path)
                blocks_timing.append((current_time, current_time + audio_duration, narration))
                current_time += audio_duration
                print(f"    [OK] Segmento: {audio_duration:.1f}s")
            else:
                print(f"    [WARN] Segmento nao gerado corretamente")

        if not segment_paths:
            print("\n  [ERROR] Nenhum segmento foi gerado.")
            sys.exit(1)

        # ── 7. Generate SRT subtitles ──
        srt_path = os.path.join(tmp_dir, "subtitles.srt")
        generate_srt(blocks_timing, srt_path)
        print(f"\n  [OK] Legendas SRT geradas: {srt_path}")

        # ── 8. Concatenate all segments ──
        raw_video = os.path.join(tmp_dir, "raw_concat.mp4")
        print(f"  Concatenando {len(segment_paths)} segmentos...")
        concatenate_videos(segment_paths, raw_video)

        if not os.path.exists(raw_video) or os.path.getsize(raw_video) < 10000:
            print("  [ERROR] Concatenacao falhou.")
            sys.exit(1)
        print(f"  [OK] Video concatenado: {os.path.getsize(raw_video)/1024/1024:.1f} MB")

        # ── 9. Burn subtitles ──
        os.makedirs(REPORT_DIR, exist_ok=True)
        print(f"  Queimando legendas no video...")
        burn_subtitles(raw_video, srt_path, OUTPUT_MP4)

        # If subtitle burn failed, copy raw video as fallback
        if not os.path.exists(OUTPUT_MP4) or os.path.getsize(OUTPUT_MP4) < 10000:
            print("  [WARN] Burn de legendas falhou, usando video sem legendas")
            shutil.copy2(raw_video, OUTPUT_MP4)

        # Also copy SRT alongside the MP4
        srt_final = OUTPUT_MP4.replace(".mp4", ".srt")
        shutil.copy2(srt_path, srt_final)

        # ── 10. Report ──
        final_size = os.path.getsize(OUTPUT_MP4) / 1024 / 1024
        print(f"\n  {'='*50}")
        print(f"  VIDEO GERADO COM SUCESSO!")
        print(f"  Arquivo: {OUTPUT_MP4}")
        print(f"  Tamanho: {final_size:.1f} MB")
        print(f"  Duracao: ~{current_time:.0f}s ({current_time/60:.1f} min)")
        print(f"  Blocos:  {len(segment_paths)}")
        print(f"  Legendas: {srt_final}")
        print(f"  {'='*50}")

    finally:
        # Cleanup temp dir
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            print(f"  [OK] Temp dir removido")
        except Exception:
            pass


if __name__ == "__main__":
    main()
