"""
Shared helpers for bot.py.
"""

import hashlib
import json
import logging
import math
import mimetypes
import os
import sqlite3
import tempfile
from pathlib import Path

from mastodon import Mastodon
from telethon.tl.types import MessageMediaWebPage

log = logging.getLogger(__name__)

# MAX_MEDIA is a hard Mastodon protocol limit — never configurable per instance
MAX_MEDIA      = 4

# MAX_FILE_BYTES: default fallback if instance query fails or MAX_FILE_SIZE not set
# Actual limit is queried from the instance at startup via fetch_instance_caps()
MAX_FILE_BYTES = 8 * 1024 * 1024
# ── Database ──────────────────────────────────────────────────────────────────
#
# post_map
#   tg_msg_id     INTEGER PK
#   masto_id      TEXT          NULL until posted
#   overflow_ids  TEXT          JSON list of overflow reply masto ids
#   group_id      INTEGER       Telegram grouped_id (albums)
#   chunk_index   INTEGER       0-based index within album
#   chunk_total   INTEGER       total chunks in album
#   source        TEXT          'channel' | 'group'
#   status        TEXT          'pending' | 'posted' | 'deleted' | 'needs_review'
#   content_hash  TEXT          hash of text+media to detect real edits
#   created_at    DATETIME
#
# meta
#   key TEXT PK, value TEXT

# Target schema — all columns the table should have
_SCHEMA_COLUMNS = [
    ("tg_msg_id",    "INTEGER PRIMARY KEY"),
    ("masto_id",     "TEXT"),                        # nullable — pending rows have no masto_id
    ("overflow_ids", "TEXT"),
    ("group_id",     "INTEGER"),
    ("chunk_index",  "INTEGER DEFAULT 0"),
    ("chunk_total",  "INTEGER DEFAULT 1"),
    ("source",       "TEXT    DEFAULT 'channel'"),
    ("status",       "TEXT    DEFAULT 'pending'"),
    ("content_hash", "TEXT"),
    ("queue",        "TEXT    DEFAULT 'archive'"),
    ("created_at",   "DATETIME DEFAULT CURRENT_TIMESTAMP"),
]


def get_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row

    # Create tables if they don't exist yet
    conn.execute("""
        CREATE TABLE IF NOT EXISTS post_map (
            tg_msg_id    INTEGER PRIMARY KEY,
            masto_id     TEXT,
            overflow_ids TEXT,
            group_id     INTEGER,
            chunk_index  INTEGER DEFAULT 0,
            chunk_total  INTEGER DEFAULT 1,
            source       TEXT    DEFAULT 'channel',
            status       TEXT    DEFAULT 'pending',
            content_hash TEXT,
            queue        TEXT    DEFAULT 'archive',
            created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_group  ON post_map(group_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON post_map(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_source ON post_map(source)")
    conn.commit()

    # ── Schema migration ──────────────────────────────────────────────────────
    # Get current columns
    existing = {row[1] for row in conn.execute("PRAGMA table_info(post_map)")}

    # Add any missing columns (safe: SQLite ADD COLUMN never fails on existing cols
    # when guarded by the 'if col not in existing' check)
    for col, typedef in _SCHEMA_COLUMNS:
        if col == "tg_msg_id":
            continue   # PRIMARY KEY, always exists
        if col not in existing:
            # Strip PRIMARY KEY / NOT NULL for ALTER TABLE compatibility
            safe_typedef = typedef.replace("NOT NULL", "").strip()
            try:
                conn.execute(f"ALTER TABLE post_map ADD COLUMN {col} {safe_typedef}")
                log.info("DB migration: added column '%s'", col)
            except sqlite3.OperationalError as exc:
                log.warning("Could not add column '%s': %s", col, exc)
    conn.commit()

    # ── Data migration ────────────────────────────────────────────────────────
    # Old rows that have a masto_id but no status → mark as posted.
    # Never overwrite needs_review.
    conn.execute("""
        UPDATE post_map
           SET status = 'posted'
         WHERE masto_id IS NOT NULL
           AND (status IS NULL OR status = '')
    """)
    conn.commit()

    return conn


def db_save(
    conn, tg_id, masto_id=None, overflow_ids=None,
    group_id=None, chunk_index=0, chunk_total=1,
    source="channel", status="pending", content_hash=None,
    queue="archive",
):
    conn.execute(
        """INSERT OR REPLACE INTO post_map
           (tg_msg_id, masto_id, overflow_ids, group_id,
            chunk_index, chunk_total, source, status, content_hash, queue)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (
            tg_id,
            masto_id,
            json.dumps(overflow_ids) if overflow_ids else None,
            group_id, chunk_index, chunk_total, source, status,
            content_hash, queue,
        ),
    )
    conn.commit()


def db_get(conn, tg_id):
    return conn.execute(
        "SELECT * FROM post_map WHERE tg_msg_id=?", (tg_id,)
    ).fetchone()


def db_get_group(conn, group_id):
    return conn.execute(
        "SELECT * FROM post_map WHERE group_id=? ORDER BY tg_msg_id",
        (group_id,),
    ).fetchall()


def db_distinct_masto_ids(conn, group_id):
    rows = conn.execute(
        """SELECT DISTINCT masto_id FROM post_map
           WHERE group_id=? AND masto_id IS NOT NULL ORDER BY chunk_index""",
        (group_id,),
    ).fetchall()
    return [r["masto_id"] for r in rows]


def db_pending_count(conn, queue: str = None) -> int:
    if queue:
        return conn.execute(
            "SELECT COUNT(*) FROM post_map WHERE status='pending' AND queue=?", (queue,)
        ).fetchone()[0]
    return conn.execute(
        "SELECT COUNT(*) FROM post_map WHERE status='pending'"
    ).fetchone()[0]


def db_pending_units(conn, queue: str = None) -> int:
    """
    Count pending Telegram posts as posting units:
    single messages count as 1, entire albums count as 1 regardless of image count.
    """
    q_filter = "AND queue=?" if queue else ""
    params   = (queue,) if queue else ()
    row = conn.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT tg_msg_id FROM post_map
            WHERE status='pending' AND group_id IS NULL {q_filter}
            UNION
            SELECT MIN(tg_msg_id) FROM post_map
            WHERE status='pending' AND group_id IS NOT NULL {q_filter}
            GROUP BY group_id
        )
    """, params + params).fetchone()
    return row[0] if row else 0


def db_next_pending(conn, limit: int = 1, queue: str = None) -> list:
    """Return oldest pending rows, ordered by tg_msg_id."""
    if queue:
        return conn.execute(
            "SELECT * FROM post_map WHERE status='pending' AND queue=? ORDER BY tg_msg_id LIMIT ?",
            (queue, limit),
        ).fetchall()
    return conn.execute(
        "SELECT * FROM post_map WHERE status='pending' ORDER BY tg_msg_id LIMIT ?",
        (limit,),
    ).fetchall()


def db_mark_posted(conn, tg_id, masto_id, overflow_ids=None):
    conn.execute(
        """UPDATE post_map SET status='posted', masto_id=?, overflow_ids=?
           WHERE tg_msg_id=?""",
        (masto_id, json.dumps(overflow_ids) if overflow_ids else None, tg_id),
    )
    conn.commit()


def db_mark_deleted(conn, tg_id):
    conn.execute(
        "UPDATE post_map SET status='deleted' WHERE tg_msg_id=?", (tg_id,)
    )
    conn.commit()


def db_delete_group(conn, group_id):
    conn.execute("DELETE FROM post_map WHERE group_id=?", (group_id,))
    conn.commit()


def db_overflow_ids(row) -> list[str]:
    if row["overflow_ids"]:
        return json.loads(row["overflow_ids"])
    return []


def db_all_masto_ids(row) -> list[str]:
    ids = []
    if row["masto_id"]:
        ids.append(row["masto_id"])
    ids.extend(db_overflow_ids(row))
    return ids


def db_meta_get(conn, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


def db_meta_set(conn, key: str, value: str):
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?,?)", (key, value)
    )
    conn.commit()


# ── Content hashing ───────────────────────────────────────────────────────────

def content_hash(message) -> str:
    """
    Hash text + media count to detect real edits vs reaction/view noise.
    Media count lets us distinguish text edits from media add/remove.
    """
    from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
    text = message.text or message.message or ""
    # Count media items: grouped messages each have 1, single posts have 0 or 1
    if message.media and not isinstance(message.media, MessageMediaWebPage):
        media_count = 1
    else:
        media_count = 0
    return hashlib.md5(f"{text}|{media_count}".encode()).hexdigest()


def media_count_changed(old_hash: str, new_msg) -> bool:
    """
    Returns True if the media count changed between the stored hash and
    the new message. Used to decide update-in-place vs delete+repost.
    Since we embed media_count in the hash we can't extract it directly,
    so we recompute with just the media part and compare.
    """
    from telethon.tl.types import MessageMediaWebPage
    if new_msg.media and not isinstance(new_msg.media, MessageMediaWebPage):
        new_count = 1
    else:
        new_count = 0
    # We can't decode the old hash, but we can check if a text-only version
    # would match — if yes, the change was media only or media count changed
    text = new_msg.text or new_msg.message or ""
    hash_text_only   = hashlib.md5(f"{text}|0".encode()).hexdigest()
    hash_with_media  = hashlib.md5(f"{text}|1".encode()).hexdigest()
    # If old hash matches text+0 and new has media, or old matches text+1 and new has no media
    # → media count changed
    if old_hash == hash_text_only and new_count == 1:
        return True
    if old_hash == hash_with_media and new_count == 0:
        return True
    return False


# ── Mastodon helpers ──────────────────────────────────────────────────────────

def fetch_instance_caps(masto: Mastodon, post_length_override: str = "auto",
                         max_file_size_override: str = "auto") -> dict:
    """
    Query the Mastodon instance for capabilities and return a dict with:
      char_limit   : int   - max post length
      file_bytes   : int   - max file upload size in bytes
      formats      : list  - supported content_type values (e.g. ["text/plain", "text/markdown"])

    post_length_override: "auto" = query instance, or a number to override
    max_file_size_override: "auto" = query instance, or a number (bytes) to override
    """
    caps = {"char_limit": 500, "file_bytes": MAX_FILE_BYTES, "formats": ["text/plain"]}

    try:
        info   = masto.instance()
        config = info.get("configuration", {})

        # Character limit
        instance_char = int(
            config.get("statuses", {}).get("max_characters")
            or info.get("max_toot_chars")
            or 500
        )
        if post_length_override != "auto":
            try:
                override = int(post_length_override)
                if override > instance_char:
                    log.warning(
                        "POST_LENGTH=%d exceeds instance limit of %d -- using instance limit",
                        override, instance_char
                    )
                    caps["char_limit"] = instance_char
                else:
                    caps["char_limit"] = override
                    log.info("Post length: %d (overridden, instance limit is %d)",
                             override, instance_char)
            except ValueError:
                log.warning("POST_LENGTH='%s' is not a number -- using auto", post_length_override)
                caps["char_limit"] = instance_char
        else:
            caps["char_limit"] = instance_char

        # File size limit
        instance_file = int(
            config.get("media_attachments", {}).get("image_size_limit")
            or MAX_FILE_BYTES
        )
        if max_file_size_override != "auto":
            try:
                caps["file_bytes"] = int(max_file_size_override)
                log.info("Max file size: %d bytes (overridden)", caps["file_bytes"])
            except ValueError:
                log.warning("MAX_FILE_SIZE='%s' is not a number -- using auto", max_file_size_override)
                caps["file_bytes"] = instance_file
        else:
            caps["file_bytes"] = instance_file

        # Supported content types (glitch-soc and some others expose this)
        supported = (
            config.get("statuses", {}).get("supported_mime_types")
            or []
        )
        if supported:
            caps["formats"] = supported
        else:
            caps["formats"] = ["text/plain"]

        log.info("Instance caps: char_limit=%d, file_bytes=%d, formats=%s",
                 caps["char_limit"], caps["file_bytes"], caps["formats"])

    except Exception as exc:
        log.warning("Could not query instance caps (%s) -- using defaults", exc)

    return caps


# ── Split style (configurable via env, read at import time) ──────────────────
import os as _split_os
SPLIT_PREFIX    = _split_os.environ.get("SPLIT_PREFIX", "...").strip()
SPLIT_SUFFIX    = _split_os.environ.get("SPLIT_SUFFIX", "...").strip()
SPLIT_INDICATOR = _split_os.environ.get("SPLIT_INDICATOR", "(%n/%N)").strip()

def _format_indicator(template: str, current: int, total: int) -> str:
    return template.replace("%n", str(current)).replace("%N", str(total))


def split_text(text: str, limit: int, mandatory_suffix: str = "", add_indicators: bool = True) -> list[str]:
    """
    Split text into chunks fitting within `limit` characters.
    If mandatory_suffix is given (e.g. post tags), it is appended to the
    FIRST chunk and the first chunk is shortened to accommodate it.
    Existing hashtags in the text are detected so callers can avoid duplicates.

    Format:
      Chunk 1:  "caption text ...\n(1/3)\n\ntags"
      Chunk 2:  "... continuation\n(2/3)"
      Chunk 3:  "... final chunk\n(3/3)"

    Single-chunk text: "text\n\ntags" (tags always appended if provided)
    """
    _sample_indicator = _format_indicator(SPLIT_INDICATOR, 99, 99)
    indicator_len = len(_sample_indicator) + 1
    prefix_len    = len(SPLIT_PREFIX) + 1
    suffix_block  = f"\n\n{mandatory_suffix}" if mandatory_suffix else ""
    first_limit   = limit - len(suffix_block) - indicator_len
    other_limit   = limit - indicator_len - prefix_len
    first_limit   = max(first_limit, 100)
    other_limit   = max(other_limit, 100)

    if not text:
        return [mandatory_suffix] if mandatory_suffix else []

    # If text fits in one chunk with suffix, return as-is
    single = f"{text}{suffix_block}".strip()
    if len(single) <= limit:
        return [single]


    def find_split_point(s: str, limit: int, total_len: int) -> int:
        """Best split point at or before limit. Paragraph > sentence > word."""
        chunk = s[:limit]
        # 1. Paragraph boundary: use if remainder <= 1/4 of total
        para_idx = chunk.rfind("\n\n")
        if para_idx != -1 and (total_len - (para_idx + 2)) <= total_len // 4:
            return para_idx + 2
        # 2. Sentence boundary
        sent_idx = chunk.rfind(".")
        if sent_idx != -1 and sent_idx > limit // 4:
            return sent_idx + 1
        # 3. Word boundary
        word_idx = chunk.rfind(" ")
        return word_idx if word_idx != -1 else limit

    # Split text into raw chunks using smart split points
    raw_chunks = []
    remaining  = text
    total_len  = len(text)
    effective  = first_limit  # tighter limit for first chunk

    while remaining:
        if len(remaining) <= effective:
            raw_chunks.append(remaining)
            break
        split_at = find_split_point(remaining, effective, total_len)
        raw_chunks.append(remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip()
        effective = other_limit  # subsequent chunks use full limit


    if len(raw_chunks) <= 1:
        return [single]

    total  = len(raw_chunks)
    result = []
    for i, chunk in enumerate(raw_chunks):
        is_last   = i == total - 1
        prefix    = f"{SPLIT_PREFIX} " if i > 0 else ""
        ellipsis  = f" {SPLIT_SUFFIX}" if not is_last else ""
        if add_indicators:
            indicator = f"\n{_format_indicator(SPLIT_INDICATOR, i+1, total)}"
            if i == 0:
                result.append(f"{chunk}{ellipsis}{indicator}{suffix_block}")
            elif is_last:
                result.append(f"{prefix}{chunk}{indicator}")
            else:
                result.append(f"{prefix}{chunk}{ellipsis}{indicator}")
        else:
            if i == 0:
                result.append(f"{chunk}{ellipsis}{suffix_block}".strip())
            elif is_last:
                result.append(f"{prefix}{chunk}".strip())
            else:
                result.append(f"{prefix}{chunk}{ellipsis}".strip())
    return result


def extract_existing_tags(text: str) -> set[str]:
    """Return set of hashtags already present in text (lowercase, with #)."""
    import re
    return {m.lower() for m in re.findall(r"#\w+", text)}


class MastoPostMissing(Exception):
    """Raised when a Mastodon post returns 404."""
    pass


def _check_masto_404(exc) -> bool:
    msg = str(exc).lower()
    return "404" in msg or "not found" in msg or "record not found" in msg


async def delete_masto_posts(masto: Mastodon, ids: list[str], raise_on_missing: bool = False):
    seen = set()
    for mid in ids:
        if not mid or mid in seen:
            continue
        seen.add(mid)
        try:
            masto.status_delete(mid)
            log.info("Deleted Mastodon post %s", mid)
        except Exception as exc:
            if _check_masto_404(exc):
                log.error(
                    "Mastodon post %s not found (404) -- may have been deleted externally", mid
                )
                if raise_on_missing:
                    raise MastoPostMissing(mid) from exc
            else:
                log.warning("Could not delete %s: %s", mid, exc)


def post_with_overflow(
    masto: Mastodon,
    text: str,
    char_limit: int,
    media_ids: list = None,
    in_reply_to_id: str = None,
    visibility: str = "public",
    content_type: str = "text/plain",
    mandatory_suffix: str = "",
) -> tuple[str, list[str]]:
    """
    Post to Mastodon, splitting into a reply chain if text exceeds char_limit.
    mandatory_suffix (e.g. tags) is appended to the first chunk; the first
    chunk is shortened to always accommodate it.
    Returns (primary_masto_id, [overflow_ids]).
    """
    chunks   = split_text(text, char_limit, mandatory_suffix=mandatory_suffix)
    if not chunks:
        chunks = [" "]

    primary_id = None
    overflow   = []
    reply_to   = in_reply_to_id

    for i, chunk in enumerate(chunks):
        status = masto.status_post(
            chunk,
            media_ids=media_ids if i == 0 else None,
            in_reply_to_id=reply_to,
            visibility=visibility,
            content_type=content_type,
        )
        sid = status["id"]
        if i == 0:
            primary_id = sid
        else:
            overflow.append(sid)
        reply_to = sid

    return primary_id, overflow


# ── Media helpers ─────────────────────────────────────────────────────────────

def is_service_message(message) -> bool:
    from telethon.tl.types import MessageService
    return isinstance(message, MessageService)


async def download_media(client, message) -> list[str]:
    if not message.media or isinstance(message.media, MessageMediaWebPage):
        return []
    with tempfile.TemporaryDirectory() as tmpdir:
        path = await client.download_media(message, file=tmpdir)
        if not path:
            return []
        size = os.path.getsize(path)
        if size > MAX_FILE_BYTES:
            log.warning("Skipping oversized file (%d bytes)", size)
            return []
        dest = Path(tempfile.mktemp(suffix=Path(path).suffix))
        Path(path).rename(dest)
        return [str(dest)]


def upload_media(masto: Mastodon, local_paths: list[str]) -> list[dict]:
    media_ids = []
    for path in local_paths:
        mime, _ = mimetypes.guess_type(path)
        try:
            media = masto.media_post(path, mime_type=mime)
            media_ids.append(media)
            log.info("Uploaded media -> %s", media["id"])
        except Exception as exc:
            log.error("Failed to upload %s: %s", path, exc)
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass
    return media_ids


def tg_text(message) -> str:
    return message.text or message.message or ""


def tg_text_md(message) -> str:
    """
    Convert Telegram message text + entities to Markdown.

    Supported entity types:
      italic       -> *text*
      bold         -> **text**
      strikethrough -> ~~text~~
      code         -> `text`
      pre          -> ```text```
      text_url     -> [text](url)
      url          -> url (unchanged)
      mention/hashtag -> unchanged

    Telegram entity offsets are in UTF-16 code units. We convert
    correctly so accented characters (French etc.) don't cause drift.
    """
    from telethon.tl.types import (
        MessageEntityItalic, MessageEntityBold,
        MessageEntityTextUrl, MessageEntityUrl,
        MessageEntityCode, MessageEntityPre,
        MessageEntityStrike,
    )

    text     = message.raw_text or message.text or message.message or ""
    entities = message.entities or []

    if not entities:
        return text

    # Convert to UTF-16-LE for correct offset handling
    utf16 = text.encode("utf-16-le")
    total_units = len(utf16) // 2

    def u16_to_str(start: int, length: int) -> str:
        b_start = start * 2
        b_end   = (start + length) * 2
        return utf16[b_start:b_end].decode("utf-16-le")

    # Build a flat list of tag events sorted by position
    # Each event: (utf16_offset, priority, tag_string)
    # priority: 0=open, 1=close (so closes come after opens at same pos)
    events: list[tuple[int, int, str]] = []

    for ent in entities:
        s = ent.offset
        e = ent.offset + ent.length
        inner = u16_to_str(s, ent.length)

        if isinstance(ent, MessageEntityItalic):
            o, c = "*", "*"
        elif isinstance(ent, MessageEntityBold):
            o, c = "**", "**"
        elif isinstance(ent, MessageEntityStrike):
            o, c = "~~", "~~"
        elif isinstance(ent, MessageEntityCode):
            o, c = "`", "`"
        elif isinstance(ent, MessageEntityPre):
            o, c = "```\n", "\n```"
        elif isinstance(ent, MessageEntityTextUrl):
            o, c = "[", f"]({ent.url})"
        else:
            continue  # url, mention, hashtag — leave as-is

        events.append((s, 0, o))
        events.append((e, 1, c))

    # Sort: by position, closes before opens at same position
    events.sort(key=lambda x: (x[0], x[1]))

    # Walk UTF-16 code units and insert markers
    result = []
    prev   = 0
    for pos, _, tag in events:
        if pos > prev:
            result.append(u16_to_str(prev, pos - prev))
        result.append(tag)
        prev = pos
    if prev < total_units:
        result.append(u16_to_str(prev, total_units - prev))

    return "".join(result)

