# tg-to-masto
A bot that copies telegram channel posts (and optionally comments) to mastodon

Please be aware of possible TOS issues.

Mirrors a Telegram channel (and its linked discussion group) to Mastodon.
Supports independent control over new post mirroring and historical backlog drip.

---
## Files

| File                 | Purpose 

| `bot.py`             | The bot — run this continuously 
| `shared.py`          | Shared helpers — required by bot.py 
| `.env`               | Your credentials and settings — never commit this 
| `{APP_NAME}.db`      | SQLite database tracking post state (auto-created) 
| `{APP_NAME}.session` | Telegram login session (auto-created, keep private) 
| `{APP_NAME}.log`     | Log file (auto-created) 

File locations are controlled by `APP_NAME` and `DB_PATH`.

---
## Setup

### 1. Install dependencies

```
pip install telethon Mastodon.py python-dotenv
```

Python 3.11 or newer required.

### 2. Telegram API credentials

1. Go to https://my.telegram.org
2. Click API development tools
3. Create an app (name and description don't matter)
4. Copy App api_id and App api_hash

### 3. Mastodon access token

1. Log into your Mastodon instance
2. Go to Settings → Development → New Application
3. Enable scopes: read and write
4. Copy Your access token

### 4. Configure

```
cp .env.example .env
```

Edit `.env` with your values.

### 5. First run

```
python bot.py
```

First run asks for your Telegram phone number and a login code.
A session file is saved so you only do this once.

---

## Running

```
python bot.py
```

On startup the bot scans your channel history, adds unposted messages to
the archive queue, registers event handlers, then runs the configured loops.

### Command-line flags

| Parameter | Effect

| `--now` | Post the next item in the queue immediately, then run normally
| `--now N` | Post the next N item(s) in the queue immediately, then run normally
| `--force n` | Post telegram message number n regardless of status, then exit (no db update)
| `--review` | Lists all items in the db that need manual review

---

## Configuration reference

### Required

```
TG_API_ID=12345678
TG_API_HASH=your_api_hash
TG_CHANNEL=@your_channel
MASTO_API_BASE=https://yourinstance.social
MASTO_ACCESS_TOKEN=your_token
```

### App identity and file locations

```
# Base name for .db, .session, and .log files.
# Spaces become underscores in filenames.
# Default: "telegram mirror" -> telegram_mirror.db / .session / .log
APP_NAME=my channel mirror

# Directory for all app files. Defaults to working directory.
# Created automatically if it does not exist.
# DB_PATH=/path/to/data
```

Note: on Windows use forward slashes or relative paths (e.g. `DB_PATH=data`).
Absolute Windows paths (`C:\data`) will not work if you move the bot to Linux.

### Discussion group

```
# auto (default) = auto-resolve from channel's linked chat
# (empty string) = disable group mirroring entirely
# @handle or id  = use this specific group
TG_GROUP=auto
```

### New post behaviour

Controls what happens when a new post arrives in your Telegram channel.

```
# direct   = mirror to Mastodon immediately (default)
# buffered = queue and release on POST_TIMES schedule
# queued   = put at the end of the archive queue (requires ARCHIVE=replay)
NEW_POSTS=direct

# Required when NEW_POSTS=buffered:
POST_TIMES=10:00,18:00   # HH:MM comma-separated, local time, midnight slots work
POST_COUNT=1             # posts per slot
```

### Archive / backlog behaviour

Controls whether and how historical channel posts are dripped out.

```
# ignore = ignore backlog, only handle new posts (default)
# replay = drip historical posts on DRIP_TIMES schedule
ARCHIVE=ignore

# Required when ARCHIVE=replay:
DRIP_TIMES=10:00,18:00,02:00   # HH:MM comma-separated, local time
DRIP_COUNT=1                    # posts per slot
```

Times past midnight (e.g. 02:00) work correctly. If the bot was asleep and
missed a slot within the last 24 hours, it fires immediately on wake.
Longer absences resume from the normal schedule without catching up.

### The five modes

| NEW_POSTS | ARCHIVE | Effect 

| direct    | ignore  | Simple live mirror. New posts go out immediately. No backlog. 
| buffered  | ignore  | Paced live mirror. New posts released on schedule. No backlog. 
| direct    | replay  | Backlog drips on DRIP_TIMES. New posts go out immediately in parallel. 
| buffered  | replay  | Both paced on their own independent schedules. 
| queued    | replay  | New posts join the back of the archive queue, dripped on DRIP_TIMES. 

Note: `NEW_POSTS=queued` with `ARCHIVE=ignore` is a soft error. The bot
warns you and treats it as `direct`.

### Startup validation

The bot exits with a clear error if:
- `NEW_POSTS=buffered` but `POST_TIMES` or `POST_COUNT` is missing
- `ARCHIVE=replay` but `DRIP_TIMES` or `DRIP_COUNT` is missing

### Post content

```
# Hashtags appended to every channel post (not group comments).
# Must be quoted because of the # characters.
POST_TAGS="#Tag1 #Tag2 #Tag3"

# Accounts to mirror from the discussion group.
# Comma-separated, quoted for names with spaces.
# Empty = mirror from main channel identity only (recommended default).
MIRROR_GROUP_SENDERS=
```

### Reply behaviour

Controls how Telegram self-replies (replies to your own channel posts) appear on Mastodon.

```
# standalone = new Mastodon post with REPLY_TOKEN + link to original (default)
# thread     = Mastodon reply/comment to the original post (lower visibility)
REPLY_MODE=standalone

# Token shown before the link in standalone mode.
# Default: <-   Other examples: ↩  re:  via  →
REPLY_TOKEN=<-
```

In `standalone` mode the post appears in followers' timelines normally.
In `thread` mode it appears as a reply — visible on your profile but not
in most followers' timelines by default.

### Split style

When a post exceeds the character limit, it is split into a reply chain.
The following settings control how splits are marked.

```
# Marker appended to chunks that continue (default: ...)
SPLIT_SUFFIX="..."

# Marker prepended to continuation chunks / 2nd post onwards (default: ...)
SPLIT_PREFIX="..."

# Chunk position indicator. %n = current, %N = total. Default: (%n/%N)
# Examples:
#   SPLIT_INDICATOR="[%n/%N]"
#   SPLIT_INDICATOR="post %n of %N"
#   SPLIT_INDICATOR="%n/%N"
SPLIT_INDICATOR="(%n/%N)"
```

---

## How the bot works

### Pending count

The bot counts pending posts as Telegram posting units: a single message
counts as 1, an entire album (regardless of image count) also counts as 1.
This matches what you see in Telegram — one album is one post.

### Albums

Telegram albums (multiple images sent together) are posted as a single
Mastodon post with up to 4 images. Larger albums are split into a reply
chain: (1/3), (2/3), (3/3) — or whatever SPLIT_INDICATOR is set to.

### Long captions

If a post exceeds your instance's character limit, the bot splits it
using this priority:

1. **Paragraph break** (blank line in Telegram): used if the second part
   is no more than 1/4 of the total text. Put a blank line where you
   want the break and the bot will use it.
2. **Sentence boundary**: last full stop before the limit.
3. **Word boundary**: last space before the limit.

Split posts form a reply chain. SPLIT_SUFFIX is appended to chunks that
continue, SPLIT_PREFIX is prepended to continuation chunks, and
SPLIT_INDICATOR shows the position. Hashtags appear at the end of the
first post only.

### Missed slots

If the computer was asleep and a slot was missed within the last 24 hours,
the bot fires immediately on wake-up. Longer absences resume normally.

### Edits

- **Text-only**: Mastodon post updated in place. Likes and boosts preserved.
- **Media changed**: new image uploaded, post updated in place.
- **Album with different image count**: delete and repost.
- **Edit to unposted message**: current version stored, used when dripped.

### Deletions

Deleting a Telegram post also deletes the Mastodon post.
For albums, all related posts are deleted together.

### Discussion group comments

Posts from accounts in `MIRROR_GROUP_SENDERS` are mirrored as Mastodon
replies to the original channel post. Same splitting, album, edit, and
delete logic applies. Group comments do not get hashtags.

### Needs review

If a Mastodon post goes missing externally, the DB row is flagged as
`needs_review` and a `[NEEDS REVIEW]` error is logged.

---

## Database status values

| Status | Meaning 

| pending      | Not yet posted to Mastodon 
| posted       | Successfully mirrored 
| deleted      | Deleted on both sides 
| needs_review | Something went wrong — check the log 

---

## Notes

- The `.session` file is your Telegram login token. Keep it private.
  It survives Mastodon account switches — only delete it to log into
  a different Telegram account.
- If you switch Mastodon accounts, delete the `.db` file to start fresh.
  The `.session` file can be kept.
- All three files share the base name from `APP_NAME` and live in the
  directory set by `DB_PATH`.
