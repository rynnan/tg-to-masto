"""
TSF unified Telegram -> Mastodon bot

One process does everything:
  - On startup: scans full channel + group history, adds unseen posts to the
    pending queue (skips anything already in the DB), then backfills missing
    reactions on already-posted messages
  - Scheduler loop: posts pending items at configured DRIP_TIMES
  - Live watchers: new posts -> queue (or direct if queue empty),
    edits -> propagate if posted / skip if pending,
    deletes -> clean up Mastodon + DB
  - Discussion group comments mirrored as Mastodon replies

CLI flags:
  --now          Post the next scheduled item immediately, then exit
  --now N        Post the next N items immediately, then exit
  --backfill     Only run reaction backfill, then exit
"""

import argparse
import asyncio
import logging
import math
import os
from collections import defaultdict
from datetime import datetime, time as dtime, timedelta

from dotenv import load_dotenv
from mastodon import Mastodon
from telethon import TelegramClient, events
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import MessageMediaWebPage

from shared import (
    MAX_MEDIA,
    get_db, db_save, db_get, db_get_group,
    db_distinct_masto_ids, db_pending_count, db_pending_units, db_next_pending,
    db_mark_posted, db_mark_deleted, db_delete_group,
    db_overflow_ids, db_all_masto_ids,
    db_meta_get, db_meta_set,
    fetch_instance_caps, split_text, extract_existing_tags, content_hash, media_count_changed,
    download_media, upload_media, tg_text, tg_text_md,
    delete_masto_posts, post_with_overflow,
    is_service_message, MastoPostMissing, _check_masto_404,
)

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)
logging.getLogger("telethon").setLevel(logging.WARNING)

# ── Config ────────────────────────────────────────────────────────────────────
TG_API_ID      = int(os.environ["TG_API_ID"])
TG_API_HASH    = os.environ["TG_API_HASH"]
TG_CHANNEL     = os.environ["TG_CHANNEL"]
# TG_GROUP: "auto" (default) = auto-resolve from channel's linked chat
#           ""  (empty)       = disable group mirroring entirely
#           anything else     = use as explicit group handle/id
TG_GROUP       = os.environ.get("TG_GROUP", "auto").strip()

MASTO_API_BASE = os.environ["MASTO_API_BASE"]
MASTO_TOKEN    = os.environ["MASTO_ACCESS_TOKEN"]

# ── File locations ────────────────────────────────────────────────────────────
# APP_NAME: used as base name for .db, .session, and .log files.
# Spaces are sanitized to underscores in filenames.
APP_NAME       = os.environ.get("APP_NAME", "telegram mirror").strip()
_app_file_base = APP_NAME.replace(" ", "_")

# DB_PATH: directory for all app files. Defaults to working directory.
# Created if it does not exist. Bot exits with an error if creation fails.
_db_path_raw   = os.environ.get("DB_PATH", "").strip()

def _resolve_app_dir(raw: str) -> str:
    import pathlib
    if not raw:
        return "."
    p = pathlib.Path(raw)
    if p.exists():
        if not p.is_dir():
            print(f"ERROR: DB_PATH '{raw}' exists but is not a directory. Exiting.")
            raise SystemExit(1)
        return str(p)
    try:
        p.mkdir(parents=True, exist_ok=True)
        print(f"Created directory '{raw}' for app files.")
        return str(p)
    except Exception as exc:
        print(f"ERROR: Could not create DB_PATH '{raw}': {exc}. Exiting.")
        raise SystemExit(1)

import os as _os
_app_dir   = _resolve_app_dir(_db_path_raw)
DB_PATH    = _os.path.join(_app_dir, f"{_app_file_base}.db")
LOG_FILE   = _os.path.join(_app_dir, f"{_app_file_base}.log")
SESSION    = _os.path.join(_app_dir, _app_file_base)

# ── New post behaviour ────────────────────────────────────────────────────────
# direct   = mirror new posts immediately as they arrive
# buffered = queue new posts, release on POST_TIMES schedule
# queued   = put new posts at the end of the archive queue (requires ARCHIVE=replay)
NEW_POSTS = os.environ.get("NEW_POSTS", "direct").strip().lower()

def _parse_times(raw: str) -> list[dtime]:
    if not raw or not raw.strip():
        return []
    return [dtime(*map(int, t.strip().split(":"))) for t in raw.strip().split(",") if t.strip()]

_raw_post_times = os.environ.get("POST_TIMES", "").strip()
POST_TIMES: list[dtime] = _parse_times(_raw_post_times)
POST_COUNT = int(os.environ.get("POST_COUNT", "1"))

# ── Archive / backlog behaviour ───────────────────────────────────────────────
# replay = drip historical posts on DRIP_TIMES schedule
# ignore = do not post historical backlog (default)
ARCHIVE = os.environ.get("ARCHIVE", "ignore").strip().lower()

_raw_drip_times = os.environ.get("DRIP_TIMES", "").strip()
DRIP_TIMES: list[dtime] = _parse_times(_raw_drip_times)
DRIP_COUNT = int(os.environ.get("DRIP_COUNT", "1"))


# ── Instance / format settings ───────────────────────────────────────────────
# Post length: "auto" = query instance, or a number to override (cannot exceed instance limit)
POST_LENGTH    = os.environ.get("POST_LENGTH",    "auto").strip()

# Mastodon post format:
#   auto     = detect from instance (default)
#   markdown = use text/markdown, fall back to plain if unsupported
#   plain    = always strip formatting, post as plaintext
MASTODON_FORMAT = os.environ.get("MASTODON_FORMAT", "auto").strip().lower()

# Max file upload size in bytes: "auto" = query instance, or a number to override
MAX_FILE_SIZE  = os.environ.get("MAX_FILE_SIZE",  "auto").strip()

# ── Reply behaviour ──────────────────────────────────────────────────────────
# How to handle Telegram self-replies (replies to your own channel posts):
#   standalone = new Mastodon post with REPLY_TOKEN + link (default)
#   thread     = Mastodon reply/comment to the original post
REPLY_MODE  = os.environ.get("REPLY_MODE",  "standalone").strip().lower()

# Token prepended to the link in standalone mode.
REPLY_TOKEN = os.environ.get("REPLY_TOKEN", "<-").strip()
# Tags appended to channel posts (not group comments).
# Space-separated in .env: POST_TAGS=#TSF #RadioHistory #VintageRadio #HistoryOfTechnology
POST_TAGS = os.environ.get("POST_TAGS", "").strip()

# Comma-separated display names of Telegram accounts whose group comments
# should be mirrored to Mastodon. Quoted names with spaces are supported.
# Example: MIRROR_GROUP_SENDERS="Daily TSF", "Another Account"
# Resolved to entity IDs at startup.
_raw_senders = os.environ.get("MIRROR_GROUP_SENDERS", "")
MIRROR_GROUP_SENDER_NAMES: list[str] = [
    s.strip().strip('"').strip("'")
    for s in _raw_senders.split(",")
    if s.strip().strip('"').strip("'")
]

ALBUM_WAIT = 1.2  # seconds to collect album siblings

# ── Post-config setup ────────────────────────────────────────────────────────
# Add file log handler now that LOG_FILE is resolved
logging.getLogger().addHandler(logging.FileHandler(LOG_FILE, encoding="utf-8"))

def _validate_config():
    errors   = []
    warnings = []

    if NEW_POSTS == "buffered":
        if not POST_TIMES:
            errors.append("NEW_POSTS=buffered requires POST_TIMES to be set")
        if POST_COUNT < 1:
            errors.append("NEW_POSTS=buffered requires POST_COUNT >= 1")

    if ARCHIVE == "replay":
        if not DRIP_TIMES:
            errors.append("ARCHIVE=replay requires DRIP_TIMES to be set")
        if DRIP_COUNT < 1:
            errors.append("ARCHIVE=replay requires DRIP_COUNT >= 1")

    if NEW_POSTS == "queued" and ARCHIVE == "ignore":
        warnings.append(
            "NEW_POSTS=queued but ARCHIVE=ignore -- new posts have nowhere to go. "
            "Treating as NEW_POSTS=direct. Fix your config or Ctrl-C now."
        )

    for w in warnings:
        log.warning(w)
    for e in errors:
        log.error(e)
    if errors:
        raise SystemExit(1)

_validate_config()
# If queued+ignore warning fired, treat as direct
_effective_new_posts = "direct" if (NEW_POSTS == "queued" and ARCHIVE == "ignore") else NEW_POSTS

# ── Globals ───────────────────────────────────────────────────────────────────
masto:             Mastodon       = None
char_limit:        int            = 500
active_format:     str            = "text/plain"   # resolved at startup
db                               = None
tg_channel_entity                = None
tg_group_entity                  = None
my_tg_id:          int            = None
tg_client:         TelegramClient = None
allowed_group_sender_ids: set[int]  = set()

def bare_id(peer_id) -> int | None:
    """Normalize a Telegram peer id by stripping the -100 channel prefix."""
    if peer_id is None:
        return None
    if peer_id < 0:
        return abs(peer_id) - 1000000000000
    return peer_id


# ── Album accumulator ─────────────────────────────────────────────────────────
_album_buffers: dict[int, list]       = defaultdict(list)
_album_tasks:   dict[int, asyncio.Task] = {}


def schedule_album_flush(group_id: int, message, source: str):
    _album_buffers[group_id].append((message, source))
    existing = _album_tasks.get(group_id)
    if existing and not existing.done():
        existing.cancel()
    _album_tasks[group_id] = asyncio.create_task(flush_album_to_queue(group_id))


async def flush_album_to_queue(group_id: int):
    await asyncio.sleep(ALBUM_WAIT)
    items = _album_buffers.pop(group_id, [])
    _album_tasks.pop(group_id, None)
    if not items:
        return

    items.sort(key=lambda x: x[0].id)
    source = items[0][1]

    pending_before = db_pending_count(db)

    for msg, src in items:
        if db_get(db, msg.id) or is_service_message(msg):
            continue
        # Only use group_id as album for channel messages.
        # In linked groups, grouped_id is a thread ID not an album.
        effective_gid = group_id if src == "channel" else None
        _album_q = "archive" if _effective_new_posts == "queued" else "new"
        db_save(db, msg.id, group_id=effective_gid, source=src,
                status="pending", content_hash=content_hash(msg), queue=_album_q)
        log.debug("Queued album message %d (group %d)", msg.id, group_id)

    log.info("Queued album group %d (%d messages, source=%s)", group_id, len(items), source)

    if _effective_new_posts == "direct":
        await post_pending_batch(len(items), queue="new")


# ── Queue a single message ────────────────────────────────────────────────────

async def enqueue(message, source: str):
    if is_service_message(message):
        return
    if db_get(db, message.id):
        return

    pending_before = db_pending_count(db)
    # Only use grouped_id as album indicator for channel messages
    effective_gid = getattr(message, "grouped_id", None) if source == "channel" else None
    q = "archive" if _effective_new_posts == "queued" else "new"
    db_save(db, message.id,
            group_id=effective_gid,
            source=source, status="pending",
            content_hash=content_hash(message),
            queue=q)
    log.info("Queued %s message %d -> %s queue (pending total: %d)",
             source, message.id, q, db_pending_count(db))

    if _effective_new_posts == "direct":
        await post_pending_batch(1, queue="new")


# ── Post a pending batch ──────────────────────────────────────────────────────

async def post_pending_batch(count: int, queue: str = None):
    rows = db_next_pending(db, limit=count, queue=queue)
    if not rows:
        return

    units: list[list] = []
    seen_groups: set  = set()

    for row in rows:
        gid = row["group_id"]
        if gid:
            if gid in seen_groups:
                continue
            seen_groups.add(gid)
            group_rows = [r for r in db_get_group(db, gid) if r["status"] == "pending"]
            if group_rows:
                units.append(group_rows)
        else:
            units.append([row])

    for unit in units:
        await post_unit(unit)


async def post_unit(rows: list):
    is_album = len(rows) > 1 or rows[0]["group_id"] is not None
    source   = rows[0]["source"]
    group_id = rows[0]["group_id"]

    from telethon.tl.types import MessageEmpty
    tg_ids   = [r["tg_msg_id"] for r in rows]
    messages = []
    for tid in tg_ids:
        try:
            entity = tg_channel_entity if source == "channel" else tg_group_entity
            msg    = await tg_client.get_messages(entity, ids=tid)
            if msg and not isinstance(msg, MessageEmpty):
                messages.append(msg)
            elif msg:
                log.info("TG message %d is deleted (MessageEmpty) -- skipping", tid)
        except Exception as exc:
            log.warning("Could not fetch TG message %d: %s", tid, exc)

    if not messages:
        log.warning("No live messages fetched for unit %s -- marking deleted", tg_ids)
        for row in rows:
            db_mark_deleted(db, row["tg_msg_id"])
        return

    messages.sort(key=lambda m: m.id)

    parent_masto_id = None
    if source == "group":
        parent_masto_id = await resolve_group_parent(messages[0])

    caption = next((tg_text_md(m) for m in messages if tg_text(m)), "")

    if is_album:
        all_paths: list[tuple[int, str]] = []
        for msg in messages:
            for p in await download_media(tg_client, msg):
                all_paths.append((msg.id, p))

        total_media  = len(all_paths)
        chunk_total  = max(1, math.ceil(total_media / MAX_MEDIA)) if total_media else 1
        media_chunks = [all_paths[i:i + MAX_MEDIA]
                        for i in range(0, max(total_media, 1), MAX_MEDIA)]

        # Build tags and split caption with tags as mandatory suffix on first chunk
        album_tags    = build_tags(caption, source)
        caption_parts = split_text(caption, char_limit, mandatory_suffix=album_tags, add_indicators=False) if caption else (
            [album_tags] if album_tags else []
        )
        n_posts       = max(chunk_total, len(caption_parts))

        chunk_members: dict[int, set[int]] = defaultdict(set)
        for ci, chunk in enumerate(media_chunks):
            for tmid, _ in chunk:
                chunk_members[ci].add(tmid)
        all_ids  = {m.id for m in messages}
        assigned = {mid for s in chunk_members.values() for mid in s}
        for mid in all_ids - assigned:
            chunk_members[0].add(mid)

        reply_to   = parent_masto_id
        posted_ids: list[str] = []

        for i in range(n_posts):
            text  = caption_parts[i] if i < len(caption_parts) else ""
            chunk = media_chunks[i]  if i < len(media_chunks)  else []

            # Add album position indicator when there are multiple posts.
            # For the first post: append indicator after caption (and after tags if present).
            # For subsequent posts: indicator is the only text unless caption overflows.
            if n_posts > 1:
                indicator = f"({i+1}/{n_posts})"
                if i == 0:
                    # Insert indicator before the tags block (tags are at end of chunk 0)
                    # Split on double newline to find tag block
                    if album_tags and text.endswith(album_tags):
                        body = text[:-len(album_tags)].rstrip()
                        text = f"{body}\n{indicator}\n\n{album_tags}".strip()
                    else:
                        text = f"{text}\n{indicator}".strip() if text.strip() else indicator
                else:
                    text = f"{text}\n{indicator}".strip() if text.strip() else indicator

            media_ids = upload_media(masto, [p for _, p in chunk]) if chunk else None
            if not text:
                text = " "

            try:
                status = masto.status_post(
                    text,
                    media_ids=media_ids or None,
                    in_reply_to_id=reply_to,
                    visibility="public",
                    content_type=active_format,
                )
                sid      = status["id"]
                reply_to = sid
                posted_ids.append(sid)
                log.info("Posted album post %d/%d -> %s", i + 1, n_posts, sid)
            except Exception as exc:
                log.error("Failed album post %d/%d: %s", i + 1, n_posts, exc)
                for future_chunk in media_chunks[i + 1:]:
                    for _, p in future_chunk:
                        try: os.unlink(p)
                        except OSError: pass
                return

        if not posted_ids:
            return

        for ci, tg_ids_in_chunk in chunk_members.items():
            masto_id = posted_ids[ci] if ci < len(posted_ids) else posted_ids[-1]
            for tg_id in tg_ids_in_chunk:
                db_mark_posted(db, tg_id, masto_id)
                db.execute(
                    "UPDATE post_map SET chunk_index=?, chunk_total=? WHERE tg_msg_id=?",
                    (ci, chunk_total, tg_id)
                )
        db.commit()
        first_row = db_get(db, messages[0].id)
        record_last_post(queue=first_row["queue"] if first_row else "archive")

    else:
        msg  = messages[0]
        text = tg_text_md(msg)

        if source == "channel" and msg.reply_to:
            parent_row = db_get(db, msg.reply_to.reply_to_msg_id)
            if parent_row and parent_row["masto_id"]:
                if REPLY_MODE == "thread":
                    parent_masto_id = parent_row["masto_id"]
                else:
                    parent_url = await get_masto_url(parent_row["masto_id"])
                    text = f"{text}\n\n{REPLY_TOKEN} {parent_url}".strip() if text else f"{REPLY_TOKEN} {parent_url}"

        media_ids = upload_media(masto, await download_media(tg_client, msg))
        tags      = build_tags(text, source)

        try:
            primary_id, overflow = post_with_overflow(
                masto, text, char_limit,
                media_ids=media_ids or None,
                in_reply_to_id=parent_masto_id,
                content_type=active_format,
                mandatory_suffix=tags,
            )
            db_mark_posted(db, msg.id, primary_id, overflow_ids=overflow or None)
            row2 = db_get(db, msg.id)
            record_last_post(queue=row2["queue"] if row2 else "archive")
            log.info("Posted %s message %d -> %s", source, msg.id, primary_id)
        except Exception as exc:
            log.error("Failed to post %s message %d: %s", source, msg.id, exc)


# ── needs_review helper ───────────────────────────────────────────────────────

def db_mark_needs_review(tg_id: int, note: str = ""):
    db.execute(
        "UPDATE post_map SET status='needs_review' WHERE tg_msg_id=?", (tg_id,)
    )
    db.commit()
    log.error("[NEEDS REVIEW] TG msg %d: %s", tg_id, note)


def build_tags(text: str, source: str) -> str:
    """
    Build the mandatory tag suffix for channel posts.
    Filters out tags already present in the text so there are no duplicates.
    Returns empty string for group comments.
    """
    if not POST_TAGS or source != "channel":
        return ""
    existing  = extract_existing_tags(text)
    tags      = [t for t in POST_TAGS.split() if t.lower() not in existing]
    return " ".join(tags)


def record_last_post(queue: str = "archive"):
    key = "last_drip_time" if queue == "archive" else "last_post_time"
    db_meta_set(db, key, datetime.now().isoformat())


# ── Scheduler ─────────────────────────────────────────────────────────────────

def seconds_until(times: list[dtime]) -> float:
    """Seconds until the next slot in `times`. Each slot rolls to tomorrow if past."""
    now      = datetime.now()
    upcoming = []
    for t in times:
        candidate = datetime.combine(now.date(), t)
        if candidate <= now:
            candidate += timedelta(days=1)
        upcoming.append(candidate)
    nxt   = min(upcoming)
    delta = (nxt - now).total_seconds()
    return delta


def check_missed(times: list[dtime], meta_key: str) -> bool:
    """Return True if a slot was missed within the last 24h."""
    last_str = db_meta_get(db, meta_key)
    if not last_str:
        return False
    last_post   = datetime.fromisoformat(last_str)
    now         = datetime.now()
    hours_since = (now - last_post).total_seconds() / 3600
    if hours_since > 24:
        return False
    for t in times:
        candidate = datetime.combine(now.date(), t)
        if last_post < candidate <= now:
            return True
        yesterday = datetime.combine(now.date() - timedelta(days=1), t)
        if last_post < yesterday <= now:
            return True
    return False


async def archive_loop():
    """Drip archive (queue='archive') posts on DRIP_TIMES schedule."""
    if ARCHIVE != "replay" or not DRIP_TIMES:
        if ARCHIVE == "replay" and not DRIP_TIMES:
            log.warning("ARCHIVE=replay but DRIP_TIMES not set -- archive will not be posted")
        return

    log.info("Archive loop started (DRIP_TIMES=%s, DRIP_COUNT=%d)",
             [str(t) for t in DRIP_TIMES], DRIP_COUNT)
    while True:
        pending = db_pending_count(db, queue="archive")
        if pending == 0:
            log.info("Archive queue empty -- archive loop done")
            return

        if check_missed(DRIP_TIMES, "last_drip_time"):
            log.info("Missed archive slot -- firing now")
            await post_pending_batch(DRIP_COUNT, queue="archive")
            db_meta_set(db, "last_drip_time", datetime.now().isoformat())
            log.info("Archive catch-up done. %d remaining.", db_pending_units(db, queue="archive"))
            continue

        wait = seconds_until(DRIP_TIMES)
        log.info("Archive: %d pending, next slot in %.0f s / %.1f h",
                 db_pending_units(db, queue="archive"), wait, wait / 3600)
        await asyncio.sleep(wait)

        pending = db_pending_count(db, queue="archive")
        if pending > 0:
            log.info("Archive slot firing: posting %d item(s), %d remaining", DRIP_COUNT, db_pending_units(db, queue="archive"))
            await post_pending_batch(DRIP_COUNT, queue="archive")
            db_meta_set(db, "last_drip_time", datetime.now().isoformat())
            log.info("Archive slot done. %d remaining.", db_pending_units(db, queue="archive"))


async def post_loop():
    """Post buffered new posts (queue='new') on POST_TIMES schedule."""
    if _effective_new_posts != "buffered" or not POST_TIMES:
        if _effective_new_posts == "buffered" and not POST_TIMES:
            log.warning("NEW_POSTS=buffered but POST_TIMES not set -- new posts will not be released")
        return

    log.info("Post loop started (POST_TIMES=%s, POST_COUNT=%d)",
             [str(t) for t in POST_TIMES], POST_COUNT)
    while True:
        pending = db_pending_count(db, queue="new")
        if pending == 0:
            await asyncio.sleep(60)
            continue

        if check_missed(POST_TIMES, "last_post_time"):
            log.info("Missed post slot -- firing now")
            await post_pending_batch(POST_COUNT, queue="new")
            db_meta_set(db, "last_post_time", datetime.now().isoformat())
            continue

        wait = seconds_until(POST_TIMES)
        log.info("Post queue: %d pending, next slot in %.0f s / %.1f h",
                 pending, wait, wait / 3600)
        await asyncio.sleep(wait)

        pending = db_pending_count(db, queue="new")
        if pending > 0:
            log.info("Post slot firing: posting %d item(s)", POST_COUNT)
            await post_pending_batch(POST_COUNT, queue="new")
            db_meta_set(db, "last_post_time", datetime.now().isoformat())
            log.info("Post slot done. %d remaining.", db_pending_units(db, queue="new"))


# ── History scanner ───────────────────────────────────────────────────────────

async def scan_history():
    log.info("Scanning channel history...")
    added = 0
    async for msg in tg_client.iter_messages(tg_channel_entity, reverse=True):
        if is_service_message(msg) or db_get(db, msg.id):
            continue
        db_save(db, msg.id, group_id=msg.grouped_id, source="channel",
                status="pending", content_hash=content_hash(msg), queue="archive")
        added += 1
    log.info("Channel scan: %d new messages added", added)

    if tg_group_entity:
        log.info("Scanning group history...")
        added_g = 0
        async for msg in tg_client.iter_messages(tg_group_entity, reverse=True):
            if is_service_message(msg):
                continue
            if bare_id(msg.sender_id) not in allowed_group_sender_ids:
                continue
            if db_get(db, msg.id):
                continue
            # Don't use grouped_id for group messages — in linked groups,
            # grouped_id is a thread ID not an album indicator.
            db_save(db, msg.id, group_id=None, source="group",
                    status="pending", content_hash=content_hash(msg),
                    queue="archive")
            added_g += 1
        log.info("Group scan: %d new messages added", added_g)

    log.info("Pending after scan: %d | Posted: %d",
             db_pending_units(db),
             db.execute("SELECT COUNT(*) FROM post_map WHERE status='posted'").fetchone()[0])


# ── Edit handler ──────────────────────────────────────────────────────────────

async def handle_edit(event, source: str):
    msg = event.message
    if is_service_message(msg):
        return
    if source == "group" and bare_id(msg.sender_id) not in allowed_group_sender_ids:
        return

    row = db_get(db, msg.id)
    if not row:
        return

    new_hash = content_hash(msg)
    if row["content_hash"] and row["content_hash"] == new_hash:
        log.debug("Edit on TG %d is noise (reaction/view count) -- ignoring", msg.id)
        return

    db.execute("UPDATE post_map SET content_hash=? WHERE tg_msg_id=?", (new_hash, msg.id))
    db.commit()

    if row["status"] in ("pending", "deleted", "needs_review"):
        log.info("Edit on %s message %d -- updated hash only (status=%s)",
                 source, msg.id, row["status"])
        return

    gid = row["group_id"]

    if gid:
        # For albums: if media count changed, delete+repost (can't compare images).
        # Otherwise update text in place.
        group_rows   = db_get_group(db, gid)
        old_count    = len([r for r in group_rows if r["status"] == "posted"])
        # Re-fetch all current group messages to count current media
        new_group_msgs = []
        for gr in group_rows:
            try:
                entity = tg_channel_entity if source == "channel" else tg_group_entity
                m = await tg_client.get_messages(entity, ids=gr["tg_msg_id"])
                if m:
                    new_group_msgs.append(m)
            except Exception:
                pass
        new_count = len(new_group_msgs)
        if new_count != old_count:
            log.info("Album group %d media count changed (%d->%d) -- reposting", gid, old_count, new_count)
            await repost_album_group(gid, msg, source)
        else:
            # Same count — update text only on the first Mastodon post
            first_masto_id = db_distinct_masto_ids(db, gid)[0] if db_distinct_masto_ids(db, gid) else None
            if first_masto_id:
                new_caption = next((tg_text_md(m) for m in sorted(new_group_msgs, key=lambda m: m.id) if tg_text(m)), "")
                tags        = build_tags(new_caption, source)
                chunks      = split_text(new_caption, char_limit, mandatory_suffix=tags)
                try:
                    masto.status_update(first_masto_id, chunks[0] if chunks else " ")
                    log.info("Updated album caption on Mastodon %s for TG group %d", first_masto_id, gid)
                except Exception as exc:
                    if _check_masto_404(exc):
                        db_mark_needs_review(group_rows[0]["tg_msg_id"],
                                             f"Mastodon post {first_masto_id} missing during album text edit")
                    else:
                        log.error("Failed to update album caption %s: %s", first_masto_id, exc)
        return

    # Single post edit.
    # Fetch the current Mastodon post to get its existing media attachment IDs.
    # We pass those back to status_update so the image is preserved.
    # If media was added or removed, we re-upload new media via status_update
    # (no delete needed — Mastodon supports changing media via the edit API).
    old_overflow = db_overflow_ids(row)
    if old_overflow:
        await delete_masto_posts(masto, old_overflow)

    text = tg_text_md(msg)
    tags = build_tags(text, source)

    # Get existing media attachment IDs from Mastodon
    existing_media_ids = []
    try:
        current_status     = masto.status(row["masto_id"])
        existing_media_ids = [a["id"] for a in current_status.get("media_attachments", [])]
    except Exception as exc:
        log.warning("Could not fetch current Mastodon post %s: %s", row["masto_id"], exc)

    # Determine which media IDs to use for the update
    if media_count_changed(row["content_hash"] or "", msg):
        # Media was added or removed — re-upload from Telegram
        log.info("Media changed on TG %d -- re-uploading for status_update", msg.id)
        new_paths     = await download_media(tg_client, msg)
        new_media_ids = [m["id"] for m in upload_media(masto, new_paths)]
        update_media  = new_media_ids or None
    else:
        # Text-only edit — reuse existing Mastodon media attachment IDs
        update_media = existing_media_ids or None

    chunks = split_text(text, char_limit, mandatory_suffix=tags)
    try:
        masto.status_update(row["masto_id"], chunks[0] if chunks else " ",
                            media_ids=update_media)
        new_overflow = []
        reply_to     = row["masto_id"]
        for chunk in (chunks[1:] if chunks else []):
            s = masto.status_post(chunk, in_reply_to_id=reply_to,
                                  content_type=active_format)
            new_overflow.append(s["id"])
            reply_to = s["id"]
        db_mark_posted(db, msg.id, row["masto_id"], overflow_ids=new_overflow or None)
        log.info("Updated Mastodon post %s for TG %d", row["masto_id"], msg.id)
    except Exception as exc:
        if _check_masto_404(exc):
            db_mark_needs_review(
                msg.id,
                f"Mastodon post {row['masto_id']} missing during edit"
            )
        else:
            log.error("Failed to update post %s: %s", row["masto_id"], exc)


# ── Delete handler ────────────────────────────────────────────────────────────

async def handle_delete(event, source: str):
    seen_groups: set = set()
    for tg_id in event.deleted_ids:
        row = db_get(db, tg_id)
        if not row or row["source"] != source:
            continue
        gid = row["group_id"]
        if gid:
            if gid in seen_groups:
                continue
            seen_groups.add(gid)
            if row["status"] == "posted":
                ids = db_distinct_masto_ids(db, gid)
                for r in db_get_group(db, gid):
                    ids.extend(db_overflow_ids(r))
                try:
                    await delete_masto_posts(masto, ids, raise_on_missing=True)
                    db_delete_group(db, gid)
                except MastoPostMissing as exc:
                    for r in db_get_group(db, gid):
                        db_mark_needs_review(
                            r["tg_msg_id"],
                            f"Mastodon post {exc} missing during TG delete of album group {gid}"
                        )
            else:
                db_delete_group(db, gid)
        else:
            if row["status"] == "posted":
                try:
                    await delete_masto_posts(masto, db_all_masto_ids(row), raise_on_missing=True)
                    db_mark_deleted(db, tg_id)
                except MastoPostMissing as exc:
                    db_mark_needs_review(
                        tg_id,
                        f"Mastodon post {exc} missing during TG delete"
                    )
            else:
                db_mark_deleted(db, tg_id)


# ── Album repost ──────────────────────────────────────────────────────────────

async def repost_album_group(group_id: int, trigger_msg, source: str):
    group_rows = db_get_group(db, group_id)
    posted_ids = []
    for r in group_rows:
        posted_ids.extend(db_all_masto_ids(r))
    await delete_masto_posts(masto, posted_ids)
    db.execute(
        "UPDATE post_map SET status='pending', masto_id=NULL, overflow_ids=NULL "
        "WHERE group_id=?", (group_id,)
    )
    db.commit()
    log.info("Album group %d reset to pending for repost", group_id)
    await post_unit(db_get_group(db, group_id))


# ── Group parent resolver ─────────────────────────────────────────────────────

async def resolve_group_parent(message) -> str | None:
    try:
        if not message.reply_to:
            return None
        seed = await tg_client.get_messages(
            tg_group_entity, ids=message.reply_to.reply_to_msg_id
        )
        if not seed or not seed.fwd_from:
            return None
        channel_post_id = seed.fwd_from.channel_post
        if not channel_post_id:
            return None
        row = db_get(db, channel_post_id)
        if not row or not row["masto_id"]:
            return None
        return row["masto_id"]
    except Exception as exc:
        log.warning("Could not resolve group parent: %s", exc)
        return None


async def get_masto_url(masto_id: str) -> str:
    try:
        return masto.status(masto_id)["url"]
    except Exception:
        return f"{MASTO_API_BASE.rstrip('/')}/@me/{masto_id}"


# ── Linked group resolver ─────────────────────────────────────────────────────

async def resolve_linked_group(channel_entity):
    try:
        full      = await tg_client(GetFullChannelRequest(channel_entity))
        linked_id = full.full_chat.linked_chat_id
        if not linked_id:
            log.warning("Channel has no linked discussion group")
            return None
        entity = await tg_client.get_entity(linked_id)
        log.info("Auto-resolved linked group: '%s' (id=%d)",
                 getattr(entity, "title", linked_id), linked_id)
        return entity
    except Exception as exc:
        log.error("Failed to resolve linked group: %s", exc)
        return None


# ── Event handlers ────────────────────────────────────────────────────────────

async def on_channel_new(event):
    msg = event.message
    if msg.grouped_id:
        schedule_album_flush(msg.grouped_id, msg, "channel")
    else:
        await enqueue(msg, "channel")

async def on_channel_edit(event):   await handle_edit(event, "channel")
async def on_channel_delete(event): await handle_delete(event, "channel")

async def on_group_new(event):
    msg = event.message
    if bare_id(msg.sender_id) not in allowed_group_sender_ids:
        return
    # Never treat grouped_id as album for group messages —
    # in linked groups it's a thread ID not an album indicator.
    await enqueue(msg, "group")

async def on_group_edit(event):   await handle_edit(event, "group")
async def on_group_delete(event): await handle_delete(event, "group")




# ── Main ──────────────────────────────────────────────────────────────────────

async def main(args):
    global masto, char_limit, db, tg_channel_entity, tg_group_entity
    global my_tg_id, tg_client

    masto      = Mastodon(access_token=MASTO_TOKEN, api_base_url=MASTO_API_BASE)
    db         = get_db(DB_PATH)

    # Query instance capabilities
    global char_limit, active_format
    caps       = fetch_instance_caps(masto, POST_LENGTH, MAX_FILE_SIZE)
    char_limit = caps["char_limit"]

    # Update shared MAX_FILE_BYTES with instance value
    import shared as _shared
    _shared.MAX_FILE_BYTES = caps["file_bytes"]

    # Resolve active post format
    supported = caps["formats"]
    if MASTODON_FORMAT == "plain":
        active_format = "text/plain"
        log.info("Post format: plain (forced)")
    elif MASTODON_FORMAT == "markdown":
        if "text/markdown" in supported:
            active_format = "text/markdown"
            log.info("Post format: markdown")
        else:
            active_format = "text/plain"
            log.warning("Post format: markdown requested but not supported by instance -- using plain")
    else:  # auto
        if "text/markdown" in supported:
            active_format = "text/markdown"
        elif "text/html" in supported:
            active_format = "text/html"
        else:
            active_format = "text/plain"
        log.info("Post format: %s (auto-detected)", active_format)

    tg_client = TelegramClient(SESSION, TG_API_ID, TG_API_HASH,
                                connection_retries=-1, retry_delay=5)
    await tg_client.start()

    me                = await tg_client.get_me()
    my_tg_id          = me.id
    tg_channel_entity = await tg_client.get_entity(TG_CHANNEL)

    if TG_GROUP == "":
        log.info("TG_GROUP is empty -- group mirroring disabled")
        tg_group_entity = None
    elif TG_GROUP == "auto":
        tg_group_entity = await resolve_linked_group(tg_channel_entity)
        if tg_group_entity is None:
            log.warning("Could not auto-resolve linked group -- group mirroring disabled")
    else:
        tg_group_entity = await tg_client.get_entity(TG_GROUP)
        log.info("Group from TG_GROUP: %s", getattr(tg_group_entity, "title", TG_GROUP))

    log.info("Channel : %s", getattr(tg_channel_entity, "title", TG_CHANNEL))
    log.info("Group   : %s", getattr(tg_group_entity, "title", "none") if tg_group_entity else "none")
    log.info("TG user : %d", my_tg_id)
    log.info("App: %s | Files in: %s", APP_NAME, _app_dir)

    # Human-readable operation mode summary
    _mode_new = {
        "direct":   "new posts -> Mastodon immediately",
        "buffered": "new posts -> Mastodon on schedule",
        "queued":   "new posts -> end of archive queue",
    }.get(_effective_new_posts, _effective_new_posts)

    log.info("Mode: %s", _mode_new)
    if _effective_new_posts == "buffered":
        log.info("Post schedule: %s (%d per slot)", ", ".join(str(t) for t in POST_TIMES), POST_COUNT)
    if ARCHIVE == "replay":
        log.info("Archive is being played back (%d posts remaining)", db_pending_units(db, queue="archive"))
        log.info("Archive drip: %s (%d per slot)", ", ".join(str(t) for t in DRIP_TIMES), DRIP_COUNT)
    elif ARCHIVE == "ignore":
        log.info("Archive: ignored")

    # Resolve MIRROR_GROUP_SENDERS names to Telegram entity IDs
    global allowed_group_sender_ids
    _resolved_sender_names = []
    if MIRROR_GROUP_SENDER_NAMES:
        for name in MIRROR_GROUP_SENDER_NAMES:
            try:
                entity = await tg_client.get_entity(name)
                allowed_group_sender_ids.add(bare_id(entity.id))
                display = getattr(entity, "title", None) or getattr(entity, "username", name)
                _resolved_sender_names.append(display)
            except Exception as exc:
                log.warning("Could not resolve MIRROR_GROUP_SENDERS name '%s': %s", name, exc)
    else:
        # Fallback: mirror from channel identity only
        channel_bare = bare_id(getattr(tg_channel_entity, "id", None))
        if channel_bare:
            allowed_group_sender_ids.add(channel_bare)
            _resolved_sender_names.append(getattr(tg_channel_entity, "title", "channel"))
    if tg_group_entity:
        log.info("Discussion mirror: %s", ", ".join(_resolved_sender_names) if _resolved_sender_names else "none")

    await scan_history()

    # --now N: fire N scheduler slots immediately, then continue normal operation
    if args.now is not None:
        slots       = args.now if args.now > 0 else 1
        has_archive = ARCHIVE == 'replay' and db_pending_count(db, queue='archive') > 0
        has_new     = _effective_new_posts == 'buffered' and db_pending_count(db, queue='new') > 0
        if not has_archive and not has_new:
            log.warning('--now: no scheduled queues active in current mode -- nothing to do')
        else:
            log.info('--now: firing %d slot(s) immediately', slots)
            for _ in range(slots):
                if has_archive:
                    await post_pending_batch(DRIP_COUNT, queue='archive')
                    db_meta_set(db, 'last_drip_time', datetime.now().isoformat())
                if has_new:
                    await post_pending_batch(POST_COUNT, queue='new')
                    db_meta_set(db, 'last_post_time', datetime.now().isoformat())
            log.info('--now done. Resuming normal operation.')

    # --force N: post Telegram message N without changing DB state, then exit
    if args.force is not None:
        tg_id = args.force
        log.warning('--force %d: posting regardless of DB state', tg_id)
        log.warning('Remember to remove --force once done -- message %d will post again via normal queue', tg_id)
        try:
            from telethon.tl.types import MessageEmpty
            msg = await tg_client.get_messages(tg_channel_entity, ids=tg_id)
            if not msg or isinstance(msg, MessageEmpty):
                log.error('--force: message %d not found in channel', tg_id)
            else:
                text      = tg_text_md(msg)
                tags      = build_tags(text, 'channel')
                media_ids = upload_media(masto, await download_media(tg_client, msg))
                try:
                    primary_id, overflow = post_with_overflow(
                        masto, text, char_limit,
                        media_ids=media_ids or None,
                        mandatory_suffix=tags,
                        content_type=active_format,
                    )
                    log.info('--force: posted message %d -> Mastodon %s', tg_id, primary_id)
                    log.warning('DB state for message %d was NOT changed', tg_id)
                except Exception as exc:
                    log.error('--force: failed to post message %d: %s', tg_id, exc)
        except Exception as exc:
            log.error('--force: could not fetch message %d: %s', tg_id, exc)
        await tg_client.disconnect()
        return

    # --review: list all needs_review rows with links, then exit
    if args.review:
        rows = db.execute(
            "SELECT * FROM post_map WHERE status='needs_review' ORDER BY tg_msg_id"
        ).fetchall()
        if not rows:
            print('No items need review.')
        else:
            print(f'{len(rows)} item(s) need review:\n')
            channel_username = getattr(tg_channel_entity, 'username', None)
            for row in rows:
                tg_link = f'https://t.me/{channel_username}/{row["tg_msg_id"]}' if channel_username else f'TG msg id: {row["tg_msg_id"]}'
                masto_link = 'not posted'
                if row['masto_id']:
                    try:
                        status = masto.status(row['masto_id'])
                        masto_link = status['url']
                    except Exception:
                        masto_link = f"{MASTO_API_BASE.rstrip('/')}/web/statuses/{row['masto_id']} (may be deleted)"
                print(f'  TG:    {tg_link}')
                print(f'  Masto: {masto_link}')
                print(f'  Issue: Mastodon post missing or inaccessible (404)')
                print()
        await tg_client.disconnect()
        return

    # Normal run: register handlers and run scheduler + listener concurrently
    tg_client.add_event_handler(on_channel_new,    events.NewMessage(chats=tg_channel_entity))
    tg_client.add_event_handler(on_channel_edit,   events.MessageEdited(chats=tg_channel_entity))
    tg_client.add_event_handler(on_channel_delete, events.MessageDeleted(chats=tg_channel_entity))

    if tg_group_entity:
        tg_client.add_event_handler(on_group_new,    events.NewMessage(chats=tg_group_entity))
        tg_client.add_event_handler(on_group_edit,   events.MessageEdited(chats=tg_group_entity))
        tg_client.add_event_handler(on_group_delete, events.MessageDeleted(chats=tg_group_entity))

    log.info('Bot running. Ctrl+C to stop.')

    await asyncio.gather(
        archive_loop(),
        post_loop(),
        tg_client.run_until_disconnected(),
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Telegram Mirror Bot')
    parser.add_argument(
        '--now', nargs='?', const=1, type=int, metavar='N',
        help='Fire N scheduler slots immediately then resume normal operation (default 1)'
    )
    parser.add_argument(
        '--force', type=int, metavar='N',
        help='Post Telegram message number N regardless of DB state, then exit. DB is not updated.'
    )
    parser.add_argument(
        '--review', action='store_true',
        help='List all items needing manual review with Telegram and Mastodon links, then exit'
    )
    args = parser.parse_args()
    asyncio.run(main(args))
