"""
Gemini Lyria song generation helper.
"""

import base64
import re
import time

import httpx

from config import get_key, SONGS_DIR

SONG_CMD_PATTERN = re.compile(r"\[SONG\]\s*([\s\S]*?)\s*\[/SONG\]", re.IGNORECASE)
SONG_VISIBLE_SECTION_PATTERN = re.compile(
    r"(?im)^\s*\[(?:Intro|Verse|Pre[- ]?Chorus|Chorus|Hook|Post[- ]?Chorus|Bridge|Final Chorus|Outro|"
    r"主歌|副歌|预副歌|桥段|间奏|尾声|前奏)[^\]]*\]\s*$"
)
SONG_GEN_MODEL = "lyria-3-pro-preview"
SONG_GEN_TIMEOUT = 300


def build_song_gen_ability_text(user_name: str) -> str:
    return (
        f"[SONG]...[/SONG] - When {user_name} explicitly asks you to write, compose, or generate a song, "
        "reply outside the block only with a brief acknowledgement and the song title; do not put full lyrics outside the block. "
        "Then append exactly one [SONG] block for the local music generator. "
        "The block must contain a complete English music-generation prompt, including title, genre/style, mood, tempo or BPM if known, "
        "duration target, singer/vocal style, instrumentation notes, production notes, and a Lyrics section. "
        "Always include a Singer/Vocal line. Infer it from the request and make it explicit, such as male baritone, male bass, "
        "male tenor, female alto, female mezzo-soprano, female soprano, duet, choir, spoken vocal, or instrumental only. "
        "If the user asks for a male singer or writes from a male-singer point of view, specify a male vocal range and avoid female vocals. "
        "If the user asks for a female singer, specify a suitable female vocal range and avoid male vocals. "
        "Use this lyrics format inside the block: Lyrics:\\n[Verse 1]\\n...\\n[Pre-Chorus]\\n...\\n[Chorus]\\n...\\n[Verse 2]\\n...\\n[Bridge]\\n...\\n[Final Chorus]\\n...\\n[Outro]\\n... "
        "Omit sections that do not fit the song, but keep bracketed section labels. "
        "Do not request a specific real artist's voice or copy copyrighted lyrics. "
        "If the user asks for instrumental music, write 'Instrumental only, no vocals' and omit the Lyrics section. "
        "The lyrics should be visible later in the generated song player, not as ordinary chat text. "
        "Example block format: [SONG]\\nTitle: ...\\nStyle: dream pop, warm synths\\nSinger/Vocal: male baritone, intimate Mandarin vocal\\nDuration: about 2 minutes\\nPrompt: Create a complete song...\\nLyrics:\\n[Verse 1]\\n...\\n[Chorus]\\n...\\n[/SONG]"
    )


def clean_song_visible_reply(text: str) -> str:
    """Keep lyrics inside the generated song player instead of the chat bubble."""
    raw = (text or "").strip()
    if not raw:
        return ""
    match = SONG_VISIBLE_SECTION_PATTERN.search(raw)
    if not match:
        return raw
    intro = raw[:match.start()].strip()
    return intro or "歌已经写好，正在生成音频。"


def _audio_ext(mime_type: str) -> str:
    mt = (mime_type or "").lower()
    if "wav" in mt:
        return "wav"
    if "mp4" in mt or "aac" in mt:
        return "m4a"
    if "mpeg" in mt or "mp3" in mt:
        return "mp3"
    return "mp3"


def _extract_title(prompt: str) -> str:
    match = re.search(r"^\s*Title\s*:\s*(.+?)\s*$", prompt or "", re.IGNORECASE | re.MULTILINE)
    if not match:
        return ""
    return match.group(1).strip().strip('"')


def _extract_lyrics(prompt: str) -> str:
    match = re.search(r"^\s*Lyrics\s*:\s*([\s\S]+)$", prompt or "", re.IGNORECASE | re.MULTILINE)
    if not match:
        return ""
    lyrics = match.group(1).strip()
    lyrics = re.sub(r"\n\s*(?:Prompt|Production notes?|Style|Duration|Tempo|BPM|Mood)\s*:\s*[\s\S]*$", "", lyrics, flags=re.IGNORECASE)
    return lyrics.strip()


def _parse_generate_content(data: dict) -> tuple[str | None, str, str]:
    audio_data = None
    mime_type = "audio/mpeg"
    text_parts: list[str] = []
    candidates = data.get("candidates") or []
    for candidate in candidates:
        parts = candidate.get("content", {}).get("parts") or []
        for part in parts:
            if isinstance(part.get("text"), str):
                text_parts.append(part["text"])
            inline = part.get("inlineData") or part.get("inline_data")
            if inline and str(inline.get("mimeType") or inline.get("mime_type") or "").startswith("audio/"):
                audio_data = inline.get("data")
                mime_type = inline.get("mimeType") or inline.get("mime_type") or mime_type
    return audio_data, mime_type, "\n\n".join(t.strip() for t in text_parts if t.strip())


async def generate_song(prompt: str) -> dict | None:
    api_key = get_key("gemini")
    if not api_key:
        print("[song_gen] Missing Gemini API key; cannot generate song")
        return None
    prompt = (prompt or "").strip()
    if not prompt:
        return None

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{SONG_GEN_MODEL}:generateContent"
    headers = {"x-goog-api-key": api_key, "Content-Type": "application/json"}
    payload = {"contents": [{"parts": [{"text": prompt}]}]}

    try:
        async with httpx.AsyncClient(timeout=SONG_GEN_TIMEOUT) as client:
            print(f"[song_gen] Generating song with {SONG_GEN_MODEL}: {prompt[:120]}")
            resp = await client.post(url, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()

        audio_b64, mime_type, text = _parse_generate_content(data)
        if not audio_b64:
            error_msg = data.get("error", {}).get("message", "no audio part in response")
            print(f"[song_gen] No audio returned: {error_msg}")
            return None

        audio_bytes = base64.b64decode(audio_b64)
        ext = _audio_ext(mime_type)
        filename = f"song_gen_{int(time.time() * 1000)}.{ext}"
        filepath = SONGS_DIR / filename
        filepath.write_bytes(audio_bytes)
        print(f"[song_gen] Song saved: {filepath}")
        return {
            "filename": filename,
            "url": f"/songs/{filename}",
            "mime_type": mime_type,
            "model": SONG_GEN_MODEL,
            "title": _extract_title(prompt),
            "lyrics": _extract_lyrics(prompt),
            "prompt": prompt,
            "text": text,
        }
    except httpx.HTTPStatusError as e:
        error_body = e.response.text[:800] if e.response else ""
        print(f"[song_gen] API request failed ({e.response.status_code}): {error_body}")
        return None
    except Exception as e:
        print(f"[song_gen] Song generation error: {e}")
        return None
