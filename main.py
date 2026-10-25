import os
import sys
import json
import time
import asyncio
import shutil
import urllib.request
from datetime import datetime, timezone

import yt_dlp
from telethon import TelegramClient, events, helpers
from telethon.errors import FloodWaitError
from telethon.tl.functions.upload import SaveBigFilePartRequest
from telethon.tl.types import DocumentAttributeVideo, InputFileBig

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ================= Configuration =================
def env_required(name):
    val = os.getenv(name, "").strip()
    if not val:
        sys.exit(f"Error: environment variable {name} set nahi hai (.env file check karo)")
    return val


def parse_chat(raw):
    raw = raw.strip()
    return int(raw) if raw.lstrip("-").isdigit() else raw


API_ID = int(env_required("TG_API_ID"))
API_HASH = env_required("TG_API_HASH")
BOT_TOKEN = env_required("TG_BOT_TOKEN")
CHAT_ID = parse_chat(env_required("TG_CHAT_ID"))          # main channel: @username ya -100xxxxxxxxxx

_backup = os.getenv("TG_BACKUP_CHAT_ID", "").strip()      # optional backup channel
BACKUP_CHAT_ID = parse_chat(_backup) if _backup else None
CHATS = [CHAT_ID] + ([BACKUP_CHAT_ID] if BACKUP_CHAT_ID is not None else [])

# Optional: /update sirf inhi user IDs ko reply dega (comma separated). Khali = sab ko.
ALLOWED_USERS = {int(x) for x in os.getenv("TG_ALLOWED_USERS", "").replace(" ", "").split(",") if x.isdigit()}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "progress_state.json")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
SESSION_NAME = os.path.join(BASE_DIR, "vps_uploader_bot")

# JSON files isi order me process hongi (ek khatam, phir agli)
# (.env me JSON_FILES=a.json,b.json likhoge to ye list ignore hogi)
DEFAULT_JSON_FILES = [
    "dsa.json",
    "placement.json",
]
JSON_FILES = [x.strip() for x in os.getenv("JSON_FILES", "").split(",") if x.strip()] or DEFAULT_JSON_FILES

MB = 1024 * 1024
MAX_UPLOAD_BYTES = 2000 * MB      # Telegram MTProto limit (bot) = 2 GB
QUEUE_SIZE = int(os.getenv("QUEUE_SIZE", "1"))   # disk par max files = QUEUE_SIZE + 2
DOWNLOAD_RETRIES = 3
UPLOAD_RETRIES = 3
UPLOAD_GAP = 3                    # seconds between two uploads (flood se bachne ke liye)

# ---- Fast upload (parallel parts) ----
FAST_UPLOAD = os.getenv("FAST_UPLOAD", "1").strip() != "0"            # 0 = purana normal upload
UPLOAD_WORKERS = max(1, int(os.getenv("UPLOAD_WORKERS", "8")))        # ek saath kitne parts
PART_SIZE = 512 * 1024            # Telegram ka max part size
FAST_MIN_BYTES = 10 * MB          # isse chhoti file normal upload se jayegi

# ---- Download ----
DOWNLOAD_FRAGMENTS = max(1, int(os.getenv("DOWNLOAD_FRAGMENTS", "8")))   # HLS (m3u8) ke liye parallel fragments

BOLD_LABELS = True                # False karoge to saare labels plain text ho jayenge

FFMPEG = shutil.which("ffmpeg")   # video ka cover/thumbnail banane ke liye (apt install ffmpeg -y)
FFPROBE = shutil.which("ffprobe")


# ================= Font helpers =================
def bold(text):
    """ASCII text ko Unicode bold (𝐀𝐛𝐜 𝟏𝟐𝟑) me convert karta hai."""
    out = []
    for ch in str(text):
        if "A" <= ch <= "Z":
            out.append(chr(0x1D400 + ord(ch) - 65))
        elif "a" <= ch <= "z":
            out.append(chr(0x1D41A + ord(ch) - 97))
        elif "0" <= ch <= "9":
            out.append(chr(0x1D7CE + ord(ch) - 48))
        else:
            out.append(ch)
    return "".join(out)


def label(text):
    return bold(text) if BOLD_LABELS else text


# ================= State (resume) =================
# progress_state.json:
# {"files": {"alphabatch1.json": {"uploaded": [...], "failed": [...], "intro_sent": true,
#                            "msgs": {lecture_id: main_message_id}, "backup_failed": [...],
#                            "summary_sent": false,
#                            "stats": {"size": 0, "duration": 0, "upload_secs": 0.0}}},
#  "legacy_uploaded": [...]}     <- purane (single data.json) version ki uploaded IDs
def new_file_state():
    return {
        "uploaded": [],
        "failed": [],
        "intro_sent": False,
        "msgs": {},
        "backup_failed": [],
        "summary_sent": False,
        "stats": {"size": 0, "duration": 0, "upload_secs": 0.0},
    }


def load_state():
    st = {"files": {}, "legacy_uploaded": [], "_legacy_intro": False}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
        except Exception as e:
            # Chup-chap reset karne se sab dobara upload ho jata, isliye ruk jao
            sys.exit(f"Error: {STATE_FILE} read nahi hui ({e}). File check/fix karo, phir run karo.")
        for fname, fs in (loaded.get("files") or {}).items():
            merged = new_file_state()
            merged.update(fs)
            st["files"][fname] = merged
        st["legacy_uploaded"] = list(loaded.get("legacy_uploaded", [])) + list(loaded.get("uploaded", []))
        st["_legacy_intro"] = bool(loaded.get("intro_sent", False))
    return st


state = load_state()


def fstate(fname):
    return state["files"].setdefault(fname, new_file_state())


def save_state():
    tmp = STATE_FILE + ".tmp"
    out = {"files": state["files"], "legacy_uploaded": state["legacy_uploaded"]}
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, STATE_FILE)      # atomic: beech me kill hone par bhi file kharab nahi hogi


def uploaded_ids(fname):
    return set(fstate(fname)["uploaded"]) | set(state["legacy_uploaded"])


def mark_uploaded(fname, lec_id, msg_id, size=0, duration=0, upload_secs=0.0):
    fs = fstate(fname)
    if lec_id not in fs["uploaded"]:
        fs["uploaded"].append(lec_id)
        st = fs.setdefault("stats", {})
        st["size"] = st.get("size", 0) + size
        st["duration"] = st.get("duration", 0) + duration
        st["upload_secs"] = st.get("upload_secs", 0.0) + upload_secs
    fs["msgs"][str(lec_id)] = msg_id
    fs["failed"] = [f for f in fs["failed"] if f["id"] != lec_id]
    save_state()


def mark_failed(fname, lec_id, title, error, stage):
    fs = fstate(fname)
    fs["failed"] = [f for f in fs["failed"] if f["id"] != lec_id]
    fs["failed"].append({
        "id": lec_id,
        "title": title,
        "error": str(error),
        "stage": stage,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
    save_state()
    print(f"\n[FAILED] {title} -> {stage}: {error}")


def mark_backup_failed(fname, lec_id):
    fs = fstate(fname)
    if lec_id not in fs["backup_failed"]:
        fs["backup_failed"].append(lec_id)
    save_state()


# ================= Runtime status (/update ke liye) =================
status = {
    "started": time.time(),
    "downloading": None,
    "download_pct": None,
    "download_size": "",
    "download_speed": "",
    "download_eta": "",
    "uploading": None,
    "upload_pct": 0.0,
    "upload_size": "",
    "upload_speed": "",
    "upload_eta": "",
    "flood_until": 0.0,
    "done_this_run": 0,
    "current_file": None,
}

contexts = []     # har JSON file ka ek context: {"fname", "idx", "data", "name", "lectures"}


def sanitize_title(title):
    clean = "".join(c for c in str(title) if c.isalnum() or c in (" ", "-", "_")).strip()
    return clean or "video"


def short_name(fname):
    return fname[:-5] if fname.lower().endswith(".json") else fname


def fmt_duration(sec):
    sec = int(max(sec, 0))
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def fmt_hms(sec):
    sec = int(max(sec, 0))
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def fmt_size(num_bytes):
    if num_bytes >= 1024 * MB:
        return f"{num_bytes / (1024 * MB):.2f} GB"
    return f"{num_bytes / MB:.1f} MB"


# ================= Counts =================
def file_counts(ctx):
    """(total, done, failed_list) us JSON file ke liye."""
    ids = {l["id"] for l in ctx["lectures"]}
    done = ids & uploaded_ids(ctx["fname"])
    failed = [f for f in fstate(ctx["fname"])["failed"] if f["id"] in ids and f["id"] not in done]
    return len(ids), len(done), failed


def overall_counts():
    total = done = failed = 0
    for ctx in contexts:
        t, d, f = file_counts(ctx)
        total += t
        done += d
        failed += len(f)
    return total, done, failed


# ================= Captions =================
def build_caption(lec, batch_name):
    title = str(lec["title"])
    if len(title) > 300:
        title = title[:297] + "..."
    caption = (
        f"➭ {label('Index')} » {lec['orderIdx']}\n"
        f"➭ {label('Title')} » {title}\n"
        f"➭ {label('Batch')} » {batch_name}\n"
        f"✨ {label('DOWNLOADED BY')} : CODExTRMS\n"
        f"━━━━━━━✦✗✦━━━━━━━"
    )
    return caption[:1024]            # Telegram caption limit


def build_batch_summary(ctx):
    """Poori batch (ek JSON) khatam hone par bhejne wali ek summary."""
    total, done, _ = file_counts(ctx)
    o_total, o_done, _ = overall_counts()
    fs = fstate(ctx["fname"])
    st = fs.get("stats", {})
    pct = (done / total * 100) if total else 100.0

    lines = [
        f"📝 {bold('BATCH SUMMARY')}",
        "━━━━━━━✦✗✦━━━━━━━",
        f"➭ {label('Batch')} » {ctx['name']}",
        f"➭ {label('Total Videos')} » {total}",
        f"➭ {label('Uploaded')} » {done}/{total} ({pct:.1f}%)",
    ]
    if st.get("duration"):
        lines.append(f"➭ {label('Total Duration')} » {fmt_hms(st['duration'])}")
    if st.get("size"):
        lines.append(f"➭ {label('Total Size')} » {fmt_size(st['size'])}")
    if st.get("upload_secs"):
        lines.append(f"➭ {label('Upload Time')} » {fmt_duration(st['upload_secs'])}")
    if BACKUP_CHAT_ID is not None:
        bf = len(fs["backup_failed"])
        backup_txt = "All Done ✅" if bf == 0 else f"{bf} pending ❌"
        lines.append(f"➭ {label('Backup')} » {backup_txt}")
    lines += [
        f"➭ {label('Overall')} » {o_done}/{o_total}",
        f"✨ {label('DOWNLOADED BY')} : CODExTRMS",
    ]
    return "\n".join(lines)


def fmt_sync_time(value):
    if not value:
        return "N/A"
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except ValueError:
        return str(value)


def build_batch_caption(data, batch_name):
    price = data.get("originalPrice")
    price_txt = "Free" if price in (0, None, "0") else str(price)
    caption = (
        f"➭ {label('Batch')} » {batch_name}\n"
        f"➭ {label('Total Videos')} » {data.get('videoCount', 'N/A')}\n"
        f"➭ {label('Total PDFs')} » {data.get('pdfCount', 'N/A')}\n"
        f"➭ {label('Popular')} » {'Yes' if data.get('isPopular') else 'No'}\n"
        f"➭ {label('Price')} » {price_txt}\n"
        f"➭ {label('Start Date')} » {data.get('startDate') or 'N/A'}\n"
        f"➭ {label('Last Synced')} » {fmt_sync_time(data.get('lastSyncedAt'))}\n"
        f"✨ {label('DOWNLOADED BY')} : CODExTRMS\n"
        f"━━━━━━━✦✗✦━━━━━━━"
    )
    return caption[:1024]


# ================= Telegram helpers =================
async def safe_call(factory, retries=3):
    """FloodWait par wait karke wahi call dobara karta hai (attempt count nahi hota).
    Baaki errors par `retries` baar try karta hai."""
    attempt = 0
    while True:
        try:
            return await factory()
        except FloodWaitError as e:
            wait = e.seconds + 5
            status["flood_until"] = time.time() + wait
            print(f"\n[FloodWait] {wait} seconds sleep...")
            await asyncio.sleep(wait)
        except Exception as e:
            attempt += 1
            if attempt >= retries:
                raise
            print(f"\n[Retry] {e} -> {attempt}/{retries - 1}")
            await asyncio.sleep(5 * attempt)


def first_msg(res):
    return res[0] if isinstance(res, (list, tuple)) else res


async def send_backup_media(client, main_msg, caption):
    """Main channel me gayi file ko backup channel me bhejta hai (dobara upload nahi hota)."""
    try:
        return first_msg(await client.send_file(BACKUP_CHAT_ID, main_msg.media, caption=caption))
    except FloodWaitError:
        raise
    except Exception as e:
        print(f"[Backup] direct send nahi hua ({e}), forward try kar raha hoon...")
        return first_msg(await client.forward_messages(BACKUP_CHAT_ID, main_msg, drop_author=True))


def download_image(url, path):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp, open(path, "wb") as f:
        f.write(resp.read())


async def pin_quietly(client, chat, msg):
    try:
        await safe_call(lambda: client.pin_message(chat, msg, notify=False))
    except Exception as e:
        print(f"[Pin] pin nahi hua ({e}). Bot ko channel me 'Pin Messages' permission do.")


async def send_batch_intro(client, ctx):
    """Batch ki image + details bhejta hai, phir PIN karta hai (har JSON ke liye sirf ek baar)."""
    caption = build_batch_caption(ctx["data"], ctx["name"])
    image = ctx["data"].get("image")
    msg = None

    if image:
        try:
            msg = first_msg(await safe_call(lambda: client.send_file(CHAT_ID, image, caption=caption)))
        except Exception as e:
            print(f"[Intro] URL se direct image nahi gayi ({e}), download karke bhej raha hoon...")
            path = os.path.join(DOWNLOAD_DIR, "batch_cover.jpg")
            try:
                await asyncio.get_running_loop().run_in_executor(None, download_image, image, path)
                msg = first_msg(await safe_call(lambda: client.send_file(CHAT_ID, path, caption=caption)))
            except Exception as e2:
                print(f"[Intro] Image fail ({e2}), sirf text bhej raha hoon...")
            finally:
                if os.path.exists(path):
                    os.remove(path)

    if msg is None:
        msg = first_msg(await safe_call(lambda: client.send_message(CHAT_ID, caption)))

    fs = fstate(ctx["fname"])
    fs["intro_sent"] = True
    save_state()
    await pin_quietly(client, CHAT_ID, msg)

    if BACKUP_CHAT_ID is not None:
        try:
            if getattr(msg, "media", None):
                bmsg = first_msg(await safe_call(
                    lambda: client.send_file(BACKUP_CHAT_ID, msg.media, caption=caption)))
            else:
                bmsg = first_msg(await safe_call(lambda: client.send_message(BACKUP_CHAT_ID, caption)))
            await pin_quietly(client, BACKUP_CHAT_ID, bmsg)
        except Exception as e:
            print(f"[Intro] Backup channel me intro nahi gaya ({e})")
    print(f"[Intro] {ctx['name']}: image + details bhej diye aur pin kar diye.\n")


async def send_batch_summary(client, ctx):
    """Batch ki saari videos upload hone ke baad ek summary (main + backup channel)."""
    text = build_batch_summary(ctx)
    try:
        await safe_call(lambda: client.send_message(CHAT_ID, text))
    except Exception as e:
        print(f"[Summary] main channel me nahi gayi: {e}")
        return
    if BACKUP_CHAT_ID is not None:
        try:
            await safe_call(lambda: client.send_message(BACKUP_CHAT_ID, text))
        except Exception as e:
            print(f"[Summary] backup channel me nahi gayi: {e}")
    fstate(ctx["fname"])["summary_sent"] = True
    save_state()
    print(f"[Summary] {ctx['name']}: batch summary bhej di.\n")


# ================= Private channel support =================
async def ensure_chat(client, chat):
    """Private channel (numeric ID) ho aur bot use pehchane nahi, to ek message ka wait karta hai."""
    try:
        await client.get_input_entity(chat)
        return
    except Exception as e:
        print(f"[Chat] {chat} resolve nahi hua ({e}).")

    if isinstance(chat, str):
        sys.exit(f"Error: {chat} galat hai ya channel ab private hai. "
                 ".env me numeric ID daalo, jaise -1001234567890")

    print(f"[Chat] Bot ko channel {chat} me ADMIN banao, phir us channel me koi bhi ek message bhejo. "
          "Main wait kar raha hoon...")
    seen = asyncio.Event()

    async def _on_msg(event):
        if event.chat_id == chat:
            seen.set()

    client.add_event_handler(_on_msg, events.NewMessage())
    try:
        await seen.wait()
    finally:
        client.remove_event_handler(_on_msg)
    print("[Chat] Channel mil gaya, aage badh raha hoon.\n")


# ================= /update text =================
def progress_bar(pct, width=12):
    pct = max(0.0, min(float(pct), 100.0))
    filled = int(round(width * pct / 100))
    return "█" * filled + "░" * (width - filled)


def progress_lines(pct, size, speed, eta):
    out = []
    if pct is None:
        out.append("   ⏳ Starting...")
    else:
        out.append(f"   {progress_bar(pct)} {pct:.1f}%")
    extras = [x for x in (size, speed, f"ETA {eta}" if eta else "") if x]
    if extras:
        out.append("   " + " • ".join(extras))
    return out


def build_update_text():
    total, uploaded, failed = overall_counts()
    remaining = total - uploaded
    pending = max(remaining - failed, 0)
    pct = (uploaded / total * 100) if total else 100.0

    lines = [
        f"📊 {bold('UPLOAD STATUS')}",
        "━━━━━━━✦✗✦━━━━━━━",
        f"➭ {label('Total')} » {total} videos ({len(contexts)} batches)",
        f"➭ {label('Uploaded')} » {uploaded} ({pct:.1f}%)",
        f"➭ {label('Remaining')} » {remaining} (Pending {pending} + Failed {failed})",
        f"{progress_bar(pct)} {pct:.1f}%",
        "",
        f"📚 {bold('BATCHES')}",
    ]

    all_failed = []
    for ctx in contexts:
        t, d, f_list = file_counts(ctx)
        if t and d == t:
            mark = "✅"
        elif ctx["fname"] == status["current_file"]:
            mark = "▶️"
        elif d:
            mark = "🔸"
        else:
            mark = "⏳"
        lines.append(f"{mark} {short_name(ctx['fname'])} — {d}/{t}")
        all_failed += [(ctx, f) for f in f_list]
    lines.append("")

    if status["downloading"]:
        lines.append(f"⬇️ {label('Downloading')} » {status['downloading']}")
        lines += progress_lines(status["download_pct"], status["download_size"],
                                status["download_speed"], status["download_eta"])
    if status["uploading"]:
        lines.append(f"⬆️ {label('Uploading')} » {status['uploading']}")
        lines += progress_lines(status["upload_pct"], status["upload_size"],
                                status["upload_speed"], status["upload_eta"])
    if status["flood_until"] > time.time():
        lines.append(f"⏳ {label('FloodWait')} » {fmt_duration(status['flood_until'] - time.time())} baki")
    if not status["downloading"] and not status["uploading"]:
        lines.append("💤 Abhi koi video process nahi ho rahi")

    if status["done_this_run"] > 0 and pending > 0:
        per_video = (time.time() - status["started"]) / status["done_this_run"]
        lines.append(f"🕒 {label('ETA')} » ~{fmt_duration(per_video * pending)}")

    if all_failed:
        lines.append("")
        lines.append(f"❌ {bold('FAILED')} ({len(all_failed)})")
        for ctx, f in all_failed[:15]:
            lines.append(f"• {f['title']} ({short_name(ctx['fname'])}) — Failed [{f.get('stage', '?')}]")
        if len(all_failed) > 15:
            lines.append(f"... +{len(all_failed) - 15} more")

    lines.append("")
    lines.append(f"✨ {label('DOWNLOADED BY')} : CODExTRMS")
    return "\n".join(lines)[:4000]


# ================= yt-dlp (async wrapper) =================
def make_download_hook():
    def hook(d):
        s = d.get("status")
        if s == "downloading":
            done = d.get("downloaded_bytes") or 0
            total = d.get("total_bytes") or d.get("total_bytes_estimate")
            if total:
                pct = done / total * 100
                status["download_size"] = f"{done / MB:.0f}MB/{total / MB:.0f}MB"
            elif d.get("fragment_count"):          # HLS/m3u8 me total bytes nahi hota
                pct = (d.get("fragment_index") or 0) / d["fragment_count"] * 100
                status["download_size"] = f"{done / MB:.0f}MB"
            else:
                pct = None
                status["download_size"] = f"{done / MB:.0f}MB"
            status["download_pct"] = pct
            speed = d.get("speed")
            status["download_speed"] = f"{speed / MB:.1f} MB/s" if speed else ""
            eta = d.get("eta")
            status["download_eta"] = fmt_duration(eta) if eta else ""
        elif s == "finished":
            status["download_pct"] = 100.0
    return hook


def reset_download_status():
    status["download_pct"] = None
    status["download_size"] = ""
    status["download_speed"] = ""
    status["download_eta"] = ""


async def download_video(url, output_path):
    ydl_opts = {
        "format": "best",
        "outtmpl": output_path,
        "merge_output_format": "mp4",
        "continuedl": True,          # .part file se download resume hota hai
        "retries": 10,
        "fragment_retries": 10,
        "concurrent_fragment_downloads": DOWNLOAD_FRAGMENTS,   # sirf HLS/DASH par asar, direct mp4 par nahi
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "progress_hooks": [make_download_hook()],
    }

    def _run_yt():
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            return ydl.extract_info(url, download=True)

    return await asyncio.get_running_loop().run_in_executor(None, _run_yt)


# ================= Video cover (thumbnail) + metadata =================
async def run_cmd(*args):
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    out, err = await proc.communicate()
    return proc.returncode, out, err


async def probe_video(path):
    """(duration_sec, width, height) deta hai, fail hone par None."""
    if not FFPROBE:
        return None
    try:
        rc, out, _ = await run_cmd(
            FFPROBE, "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height:format=duration", "-of", "json", path,
        )
        if rc != 0:
            return None
        info = json.loads(out)
        stream = info["streams"][0]
        return int(float(info["format"]["duration"])), int(stream["width"]), int(stream["height"])
    except Exception as e:
        print(f"[Probe] metadata nahi mila: {e}")
        return None


async def make_thumbnail(path, duration):
    """Video ke ek frame se cover (jpg) banata hai. ffmpeg na ho to None."""
    if not FFMPEG:
        return None
    thumb = path + ".thumb.jpg"
    ts = min(max((duration or 0) * 0.1, 1.0), 30.0)       # ~10% par, 1s se 30s ke beech
    try:
        rc, _, _ = await run_cmd(
            FFMPEG, "-y", "-loglevel", "error", "-ss", f"{ts:.2f}", "-i", path,
            "-frames:v", "1",
            "-vf", "scale='if(gt(iw,ih),320,-2)':'if(gt(iw,ih),-2,320)'",
            "-q:v", "5", thumb,
        )
        if rc == 0 and os.path.exists(thumb) and os.path.getsize(thumb) > 0:
            return thumb
    except Exception as e:
        print(f"[Thumb] cover nahi bana: {e}")
    if os.path.exists(thumb):
        os.remove(thumb)
    return None


# ================= Upload progress =================
def get_progress_callback(title):
    last_print = 0
    t0 = time.time()

    def progress(current, total):
        nonlocal last_print
        if not total:
            return
        percent = current / total * 100
        speed = current / max(time.time() - t0, 0.001)
        status["upload_pct"] = percent
        status["upload_size"] = f"{current / MB:.0f}MB/{total / MB:.0f}MB"
        status["upload_speed"] = f"{speed / MB:.1f} MB/s"
        status["upload_eta"] = fmt_duration((total - current) / speed) if speed > 0 else ""
        if percent - last_print >= 5 or percent >= 100:
            last_print = percent
            sys.stdout.write(f"\r[Telegram Uploading] {title[:25]}... -> {percent:.1f}% "
                             f"({current / MB:.1f}MB/{total / MB:.1f}MB)")
            sys.stdout.flush()

    return progress


# ================= Fast upload (parallel parts) =================
async def fast_upload(client, path, progress_callback=None, workers=None):
    """File ke parts ek ke baad ek ki jagah `workers` ek saath bhejta hai.
    Telegram har part ka number (file_part) dekhta hai, isliye order matter nahi karta.
    InputFileBig wapas deta hai, jise send_file seedha file ki tarah le leta hai."""
    workers = workers or UPLOAD_WORKERS
    file_size = os.path.getsize(path)
    part_count = (file_size + PART_SIZE - 1) // PART_SIZE
    if part_count > 4000:
        raise ValueError("file Telegram ke 4000 parts se badi hai")

    file_id = helpers.generate_random_long()
    counters = {"next": 0, "sent": 0}
    f = open(path, "rb")

    async def worker():
        while True:
            idx = counters["next"]
            if idx >= part_count:
                return
            counters["next"] = idx + 1
            f.seek(idx * PART_SIZE)          # seek+read ke beech await nahi, isliye safe
            chunk = f.read(PART_SIZE)
            request = SaveBigFilePartRequest(file_id, idx, part_count, chunk)

            attempt = 0
            while True:
                try:
                    ok = await client(request)
                    break
                except FloodWaitError as e:
                    wait = e.seconds + 2
                    status["flood_until"] = time.time() + wait
                    print(f"\n[FloodWait] {wait}s (fast upload)...")
                    await asyncio.sleep(wait)
                except Exception as e:
                    attempt += 1
                    if attempt >= 3:
                        raise
                    await asyncio.sleep(2 * attempt)
            if not ok:
                raise RuntimeError(f"part {idx} Telegram ne accept nahi kiya")

            counters["sent"] += len(chunk)
            if progress_callback:
                progress_callback(counters["sent"], file_size)

    tasks = [asyncio.create_task(worker()) for _ in range(min(workers, part_count))]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    finally:
        f.close()

    return InputFileBig(file_id, part_count, os.path.basename(path))


# ================= JSON loading =================
def load_contexts():
    names = list(JSON_FILES)
    if not any(os.path.exists(os.path.join(BASE_DIR, n)) for n in names):
        if os.path.exists(os.path.join(BASE_DIR, "data.json")):
            print("[Info] JSON files nahi mili, data.json use kar raha hoon.")
            names = ["data.json"]

    legacy = set(state["legacy_uploaded"])
    ctxs = []
    for fname in names:
        path = os.path.join(BASE_DIR, fname)
        if not os.path.exists(path):
            print(f"[Skip] {fname} nahi mila (upload karna bhool gaye?)")
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            sys.exit(f"Error: {fname} padh nahi paya ({e})")

        lectures = data.get("lectures", [])
        if not lectures:
            print(f"[Warning] {fname} me 'lectures' nahi mila (keys: {list(data.keys())[:10]})")
        for i, lec in enumerate(lectures):
            lec.setdefault("orderIdx", i)            # JSON ka orderIdx hi Index banega
            lec.setdefault("title", f"Lecture {lec['orderIdx']}")
        lectures.sort(key=lambda l: l["orderIdx"])

        ctx = {
            "fname": fname,
            "idx": len(ctxs),
            "data": data,
            "name": data.get("name") or short_name(fname),
            "lectures": lectures,
        }
        ctxs.append(ctx)

        fs = fstate(fname)
        ids = {l["id"] for l in lectures}
        if state["_legacy_intro"] and (legacy & ids) and not fs["intro_sent"]:
            fs["intro_sent"] = True          # purane version me is batch ka intro ja chuka tha
    save_state()
    return ctxs


# ================= Main pipeline =================
async def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    contexts.extend(load_contexts())
    if not contexts:
        print("Error: koi JSON file nahi mili! Files VPS par isi folder me rakho:")
        for n in JSON_FILES:
            print(f"  - {n}")
        return

    o_total, o_done, o_failed = overall_counts()
    print("JSON order:")
    for ctx in contexts:
        t, d, _ = file_counts(ctx)
        print(f"  {ctx['idx'] + 1}. {ctx['fname']} -> {ctx['name']} ({d}/{t} uploaded)")
    print(f"Total: {o_total}, Uploaded: {o_done}, Failed (retry hoga): {o_failed}")
    print(f"Backup channel: {'ON' if BACKUP_CHAT_ID is not None else 'OFF'}")
    print(f"Fast upload: {'ON (' + str(UPLOAD_WORKERS) + ' parts ek saath)' if FAST_UPLOAD else 'OFF'}\n")

    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    while True:
        try:
            await client.start(bot_token=BOT_TOKEN)
            break
        except FloodWaitError as e:
            print(f"[FloodWait on login] {e.seconds}s ruk raha hoon...")
            await asyncio.sleep(e.seconds + 2)
    print("Telegram Bot Connected Successfully!\n")

    if not FFMPEG or not FFPROBE:
        print("[Warning] ffmpeg/ffprobe nahi mila -> videos par cover nahi aayega. "
              "Fix: apt install ffmpeg -y\n")

    for chat in CHATS:
        await ensure_chat(client, chat)

    # /update command
    @client.on(events.NewMessage(pattern=r"(?i)^/update(@\w+)?\s*$"))
    async def update_handler(event):
        from_channel_admin = event.is_channel and not event.is_group
        if ALLOWED_USERS and event.sender_id not in ALLOWED_USERS and not from_channel_admin:
            return
        await event.respond(build_update_text())

    # Pichhli baar jo videos backup channel me nahi gayi thi unhe ab bhej do
    if BACKUP_CHAT_ID is not None:
        await retry_backups(client)

    upload_queue = asyncio.Queue(maxsize=QUEUE_SIZE)

    async def post_upload(ctx, lec, msg, caption):
        """Video jaane ke baad sirf backup channel me copy (summary ab batch ke end par jayegi)."""
        if BACKUP_CHAT_ID is None:
            return
        try:
            await safe_call(lambda: send_backup_media(client, msg, caption))
        except Exception as e:
            print(f"\n[Backup Failed] {lec['title']}: {e} (agli run me dobara try hoga)")
            mark_backup_failed(ctx["fname"], lec["id"])

    # Consumer: upload worker (download ke saath parallel chalta hai)
    async def upload_worker():
        while True:
            task = await upload_queue.get()
            if task is None:
                upload_queue.task_done()
                break

            ctx, lec, video_path, caption = task
            fname, title, lec_id = ctx["fname"], lec["title"], lec["id"]
            status["uploading"] = title
            status["upload_pct"] = 0.0
            status["upload_size"] = ""
            status["upload_speed"] = ""
            status["upload_eta"] = ""
            thumb = None

            try:
                size = os.path.getsize(video_path)
                # Cover + video info (duration/size) taaki Telegram me proper video ban kar dikhe
                meta = await probe_video(video_path)
                thumb = await make_thumbnail(video_path, meta[0] if meta else 0)
                attributes = None
                if meta:
                    attributes = [DocumentAttributeVideo(
                        duration=meta[0], w=meta[1], h=meta[2], supports_streaming=True
                    )]

                print(f"\n[Starting Upload] {short_name(fname)} | Index {lec['orderIdx']}: {title}")
                t0 = time.time()
                progress_cb = get_progress_callback(title)

                # Step 1: fast (parallel) upload. Fail hua to normal upload par chale jayenge.
                uploaded = None
                if FAST_UPLOAD and size > FAST_MIN_BYTES:
                    try:
                        uploaded = await fast_upload(client, video_path, progress_cb)
                    except Exception as e:
                        print(f"\n[FastUpload] fail hua ({e}), normal upload par ja raha hoon...")
                        uploaded = None
                        status["upload_pct"] = 0.0
                        status["upload_size"] = ""
                        status["upload_speed"] = ""
                        status["upload_eta"] = ""
                        progress_cb = get_progress_callback(title)

                async def send_video(file_arg, cb):
                    return first_msg(await safe_call(
                        lambda: client.send_file(
                            CHAT_ID,
                            file_arg,
                            caption=caption,
                            thumb=thumb,
                            attributes=attributes,
                            supports_streaming=True,
                            part_size_kb=512,
                            progress_callback=cb,
                        ),
                        retries=UPLOAD_RETRIES,
                    ))

                # Step 2: message bhejna (fast upload ho chuka ho to ye turant hota hai)
                msg = None
                try:
                    if uploaded is not None:
                        try:
                            msg = await send_video(uploaded, None)
                        except Exception as e:
                            print(f"\n[FastUpload] message nahi gaya ({e}), normal upload se dobara...")
                            status["upload_pct"] = 0.0
                            msg = await send_video(video_path, get_progress_callback(title))
                    else:
                        msg = await send_video(video_path, progress_cb)
                except Exception as e:
                    mark_failed(fname, lec_id, title, e, "telegram_upload")
                    msg = None

                if msg is not None:
                    upload_secs = time.time() - t0
                    print(f"\n[Upload Done] {title} ({size / MB / max(upload_secs, 0.001):.1f} MB/s avg)")
                    mark_uploaded(fname, lec_id, msg.id, size,
                                  meta[0] if meta else 0, upload_secs)
                    status["done_this_run"] += 1
                    await post_upload(ctx, lec, msg, caption)
            finally:
                status["uploading"] = None
                if thumb and os.path.exists(thumb):
                    try:
                        os.remove(thumb)
                    except OSError:
                        pass
                if os.path.exists(video_path):
                    try:
                        os.remove(video_path)       # disk free
                    except OSError:
                        pass
                upload_queue.task_done()

            await asyncio.sleep(UPLOAD_GAP)

    uploader_task = asyncio.create_task(upload_worker())

    # Producer: JSON files ek ke baad ek, har file ki videos ek-ek karke download
    for ctx in contexts:
        fname = ctx["fname"]
        fs = fstate(fname)
        done_ids = uploaded_ids(fname)
        todo = [l for l in ctx["lectures"] if l["id"] not in done_ids]

        if not todo:
            t, d, _ = file_counts(ctx)
            if d == t and not fs["summary_sent"]:
                await upload_queue.join()
                await send_batch_summary(client, ctx)
            else:
                print(f"[Skip] {fname}: sab videos pehle hi upload ho chuki hain.")
            continue

        # Pichhli file ki saari videos khatam hone do, phir is batch ka intro
        await upload_queue.join()
        status["current_file"] = fname
        print(f"\n===== Batch: {ctx['name']} ({fname}) | {len(todo)} videos baki =====")

        if not fs["intro_sent"]:
            try:
                await send_batch_intro(client, ctx)
            except Exception as e:
                print(f"[Intro] Bhejna fail hua ({e}), videos ka kaam chalu rakhta hoon.\n")

        for lec in todo:
            lec_id, title = lec["id"], lec["title"]

            url = lec.get("link")
            if not url:
                mark_failed(fname, lec_id, title, "link missing in JSON", "download")
                continue

            filename = f"{ctx['idx']:02d}_{lec['orderIdx']:03d}_{sanitize_title(title)}.mp4"
            video_path = os.path.join(DOWNLOAD_DIR, filename)

            print(f"\n[Download Start] {short_name(fname)} | Index {lec['orderIdx']}: {title}")
            status["downloading"] = title
            info, last_err = None, None

            for attempt in range(1, DOWNLOAD_RETRIES + 1):
                reset_download_status()
                try:
                    info = await download_video(url, video_path)
                    if os.path.exists(video_path) and os.path.getsize(video_path) > 0:
                        break
                    raise Exception("File empty or not created")
                except Exception as e:
                    info, last_err = None, e
                    print(f"[Download Error] {title}: {e} (try {attempt}/{DOWNLOAD_RETRIES})")
                    if attempt < DOWNLOAD_RETRIES:
                        await asyncio.sleep(5 * attempt)

            status["downloading"] = None
            reset_download_status()

            if info is None:
                mark_failed(fname, lec_id, title, last_err, "download")
                continue

            size = os.path.getsize(video_path)
            if size > MAX_UPLOAD_BYTES:
                mark_failed(fname, lec_id, title, f"File {size / MB:.0f}MB > 2GB Telegram limit", "size_limit")
                os.remove(video_path)
                continue

            caption = build_caption(lec, ctx["name"])
            print(f"[Download Done] Index {lec['orderIdx']}: {title} ({size / MB:.1f}MB, queued for upload)")
            await upload_queue.put((ctx, lec, video_path, caption))

        # Is batch ki saari videos upload hone do, phir ek hi summary
        await upload_queue.join()
        t, d, f_list = file_counts(ctx)
        if d == t and not fs["summary_sent"]:
            await send_batch_summary(client, ctx)
        elif f_list:
            print(f"[Summary] {ctx['name']}: {len(f_list)} video fail hui, "
                  "summary tab jayegi jab batch poora upload ho jaye.")

    await upload_queue.put(None)
    await uploader_task
    status["current_file"] = None

    o_total, o_done, o_failed = overall_counts()
    print("\n" + "=" * 40)
    print(f"Uploaded: {o_done}/{o_total}")
    if o_failed:
        print(f"Failed: {o_failed} (dobara run karoge to inhe retry kiya jayega)")
        for ctx in contexts:
            for f in file_counts(ctx)[2]:
                print(f"  - {f['title']} ({short_name(ctx['fname'])}) — Failed [{f.get('stage')}]: {f.get('error')}")
    else:
        print("Sabhi videos successfully process ho gayi hain!")

    await client.disconnect()


async def retry_backups(client):
    for ctx in contexts:
        fs = fstate(ctx["fname"])
        for lid in list(fs["backup_failed"]):
            msg_id = fs["msgs"].get(str(lid))
            if not msg_id:
                continue
            try:
                msg = await client.get_messages(CHAT_ID, ids=msg_id)
                if not msg:
                    continue
                await safe_call(lambda: send_backup_media(client, msg, msg.message or ""))
                fs["backup_failed"].remove(lid)
                save_state()
                print(f"[Backup Retry] {ctx['fname']} id {lid}: backup channel me bhej diya")
            except Exception as e:
                print(f"[Backup Retry] {ctx['fname']} id {lid} fail: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nPaused. Dobara run karo, jahan roka tha wahin se resume hoga.")
