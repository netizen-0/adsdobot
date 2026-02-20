"""
ADS Pro Broadcast Bot — Production Build (FIXED)
All parse-entity errors resolved.
All post-restart crashes resolved.
"""

import asyncio
import os
import random
import re
import traceback
from datetime import datetime, timedelta
from pathlib import Path

from motor.motor_asyncio import AsyncIOMotorClient

from telethon import TelegramClient, Button
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import Channel, Chat, User
from telethon.sessions import StringSession

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters, ConversationHandler, Defaults
)
from telegram.constants import ParseMode

try:
    from telethon.tl.functions.channels import GetForumTopicsRequest
    HAS_FORUM = True
except ImportError:
    HAS_FORUM = False

try:
    from telethon.tl.functions.messages import ForwardMessagesRequest
    HAS_FWD_REQ = True
except ImportError:
    HAS_FWD_REQ = False

try:
    from telethon.errors import SessionPasswordNeededError
except ImportError:
    SessionPasswordNeededError = Exception

from config import Config


# ═══════════════════════════════════════════
#  CONSTANTS & GLOBALS
# ═══════════════════════════════════════════
PHONE, CODE, PASSWORD = range(3)
GEN_PHONE, GEN_CODE, GEN_PASSWORD = range(3, 6)
P_CH, P_MEDIA, P_CAP, P_BTN, P_POS, P_CONFIRM = range(10, 16)

DELAY = 9
scheds = {}
tasks = {}
cancels = {}
AUTH = {}


def _int(v, d=0, *, lo=None, hi=None):
    try:
        i = int(v)
    except Exception:
        i = int(d)
    if lo is not None:
        i = max(i, lo)
    if hi is not None:
        i = min(i, hi)
    return i


def _err(e, n=150):
    s = str(e).replace('\n', ' ').replace('\r', ' ').strip()
    return s[:n] if s else "Unknown error"


def _sent_ok(res):
    if not res:
        return False
    if getattr(res, "id", None):
        return True
    if isinstance(res, (list, tuple)):
        return any(getattr(x, "id", None) for x in res if x)
    ups = getattr(res, "updates", None)
    if ups:
        for u in ups:
            m = getattr(u, "message", None)
            if m and getattr(m, "id", None):
                return True
            if getattr(u, "id", None):
                return True
    return False


def _escape_md(text):
    """Escape Markdown V1 special characters to prevent parse errors."""
    if not text:
        return text
    # Characters that need escaping in Markdown V1: _ * [ ] ( ) ~ ` > # + - = | { } . !
    # But we only escape the ones that commonly break: unmatched _ * ` [
    # Strategy: if the count of a delimiter is odd, escape all occurrences
    result = text
    for ch in ['*', '_', '`']:
        if result.count(ch) % 2 != 0:
            result = result.replace(ch, '\\' + ch)
    return result


def _strip_md(text):
    """Remove all Markdown formatting characters entirely."""
    if not text:
        return text
    for ch in ['*', '_', '`', '[', ']', '(', ')']:
        text = text.replace(ch, '')
    return text


def cancelled(u):
    return cancels.get(u, False)


def cancel(u, v=True):
    cancels[u] = v


def uncancel(u):
    cancels[u] = False


# ═══════════════════════════════════════════
#  SAFE SEND HELPER
#  Tries with parse_mode=Markdown first,
#  falls back to plain text on ANY parse error.
# ═══════════════════════════════════════════
async def safe_send(bot, chat_id, text, **kwargs):
    """Send message with Markdown, auto-fallback to plain on parse error."""
    try:
        return await bot.send_message(
            chat_id=chat_id, text=text,
            parse_mode=ParseMode.MARKDOWN, **kwargs)
    except Exception as e:
        if _is_entity_parse_error(e):
            clean = _strip_md(text)
            kw = {k: v for k, v in kwargs.items()
                  if k not in ('parse_mode', 'entities', 'caption_entities')}
            return await bot.send_message(
                chat_id=chat_id, text=clean,
                parse_mode=None, **kw)
        raise


async def safe_edit(bot_or_msg, text, **kwargs):
    """Edit message with Markdown, auto-fallback to plain on parse error."""
    try:
        if hasattr(bot_or_msg, 'edit_message_text'):
            return await bot_or_msg.edit_message_text(
                text=text, parse_mode=ParseMode.MARKDOWN, **kwargs)
        else:
            return await bot_or_msg.edit_text(
                text=text, parse_mode=ParseMode.MARKDOWN, **kwargs)
    except Exception as e:
        if _is_entity_parse_error(e):
            clean = _strip_md(text)
            kw = {k: v for k, v in kwargs.items()
                  if k not in ('parse_mode', 'entities', 'caption_entities')}
            if hasattr(bot_or_msg, 'edit_message_text'):
                return await bot_or_msg.edit_message_text(
                    text=clean, parse_mode=None, **kw)
            else:
                return await bot_or_msg.edit_text(
                    text=clean, parse_mode=None, **kw)
        if "not modified" in str(e).lower():
            return None
        raise


def _is_entity_parse_error(e):
    s = str(e).lower()
    return (
        "can't parse entities" in s
        or "can't parse entity" in s
        or "can't find end of the entity" in s
        or "can't find end tag" in s
        or "unsupported start tag" in s
    )


# ═══════════════════════════════════════════
#  CORE FIX: Never store PTB Message objects
# ═══════════════════════════════════════════
def extract_content(msg):
    if not msg:
        return {}
    d = {
        "message_id": getattr(msg, 'message_id', None),
        "text": getattr(msg, 'text', None),
        "caption": getattr(msg, 'caption', None),
        "media_type": None,
        "file_id": None,
    }
    if getattr(msg, 'photo', None):
        d["media_type"] = "photo"
        d["file_id"] = msg.photo[-1].file_id
    elif getattr(msg, 'video', None):
        d["media_type"] = "video"
        d["file_id"] = msg.video.file_id
    elif getattr(msg, 'document', None):
        d["media_type"] = "document"
        d["file_id"] = msg.document.file_id
    elif getattr(msg, 'audio', None):
        d["media_type"] = "audio"
        d["file_id"] = msg.audio.file_id
    elif getattr(msg, 'animation', None):
        d["media_type"] = "animation"
        d["file_id"] = msg.animation.file_id
    return d


class BgContext:
    """Lightweight context for background tasks."""
    def __init__(self, app):
        self.bot = app.bot
        self.application = app
        self.user_data = {}


# ═══════════════════════════════════════════
#  MONGODB
# ═══════════════════════════════════════════
class DB:
    def __init__(self):
        self.client = AsyncIOMotorClient(
            Config.MONGO_URI,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000,
            retryWrites=True)
        self._db = self.client[Config.DB_NAME]
        self.users = self._db.users
        self.stats = self._db.stats
        self.schedules = self._db.schedules

    async def init(self):
        await self.users.create_index("user_id", unique=True)
        await self.schedules.create_index("user_id", unique=True)
        if not await self.stats.find_one({"_id": "g"}):
            await self.stats.insert_one({"_id": "g", "users": 0, "msgs": 0})

    async def add_user(self, uid, uname, fname):
        if not await self.users.find_one({"user_id": uid}):
            await self.users.insert_one({
                "user_id": uid, "username": uname or "N/A",
                "first_name": fname or "?",
                "joined": datetime.now().isoformat(),
                "last_active": datetime.now().isoformat(),
                "sent": 0, "authed": False,
                "authorized": uid == Config.OWNER_ID,
                "pending": False, "phone": None,
                "log_ch": None, "session": None})
            await self.stats.update_one({"_id": "g"}, {"$inc": {"users": 1}})
            if uid == Config.OWNER_ID:
                AUTH[uid] = True

    async def user(self, uid):
        return await self.users.find_one({"user_id": uid})

    async def all_users(self):
        return await self.users.find({}).to_list(None)

    async def touch(self, uid):
        await self.users.update_one(
            {"user_id": uid},
            {"$set": {"last_active": datetime.now().isoformat()}})

    async def set_auth(self, uid, ss=None):
        if ss:
            await self.users.update_one(
                {"user_id": uid},
                {"$set": {"authed": True, "session": ss}})
        else:
            await self.users.update_one(
                {"user_id": uid},
                {"$set": {"authed": False, "session": None}})

    async def authorize(self, uid, ok=True):
        await self.users.update_one(
            {"user_id": uid},
            {"$set": {"authorized": ok, "pending": False}})
        AUTH[uid] = ok

    async def set_pending(self, uid):
        await self.users.update_one(
            {"user_id": uid}, {"$set": {"pending": True}})

    async def session(self, uid):
        u = await self.user(uid)
        return u.get("session") if u else None

    async def set_phone(self, uid, p):
        await self.users.update_one({"user_id": uid}, {"$set": {"phone": p}})

    async def set_log(self, uid, ch):
        await self.users.update_one({"user_id": uid}, {"$set": {"log_ch": ch}})

    async def get_log(self, uid):
        u = await self.user(uid)
        return u.get("log_ch") if u else None

    async def inc_msgs(self, uid):
        await self.users.update_one({"user_id": uid}, {"$inc": {"sent": 1}})
        await self.stats.update_one({"_id": "g"}, {"$inc": {"msgs": 1}})

    async def get_stats(self):
        return await self.stats.find_one({"_id": "g"})

    async def save_sched(self, uid, d):
        old = await self.schedules.find_one({"user_id": uid})
        cd = d.get("content_data") or (old or {}).get("content_data")
        nr = d.get("next_run")
        if not nr and old and old.get("active"):
            nr = old.get("next_run")
        doc = {
            "user_id": uid, "interval": d.get("interval"),
            "topic": d.get("topic"), "mode": d.get("mode"),
            "buttons": d.get("buttons"), "link": d.get("link"),
            "backup": d.get("backup"), "content_data": cd,
            "last_run": d.get("last_run", datetime.now().isoformat()),
            "next_run": nr, "active": d.get("active", True)}
        await self.schedules.update_one(
            {"user_id": uid}, {"$set": doc}, upsert=True)

    async def active_scheds(self):
        return await self.schedules.find({"active": True}).to_list(None)

    async def del_sched(self, uid):
        await self.schedules.delete_one({"user_id": uid})


db = DB()


# ═══════════════════════════════════════════
#  TELETHON CLIENT
# ═══════════════════════════════════════════
async def get_cl(uid):
    ss = await db.session(uid)
    kw = dict(
        api_id=Config.API_ID, api_hash=Config.API_HASH,
        device_model=f"ADS_{uid}_{random.randint(100, 999)}",
        system_version="4.16.30", app_version="2.0",
        connection_retries=5, retry_delay=3)
    if ss:
        return TelegramClient(StringSession(ss), **kw)
    sf = str(Config.SESSIONS_DIR / f"u_{uid}")
    return TelegramClient(sf, **kw)


async def check_sess(uid):
    try:
        c = await get_cl(uid)
        await c.connect()
        ok = await c.is_user_authorized()
        await c.disconnect()
        return ok
    except Exception:
        return False


# ═══════════════════════════════════════════
#  NOTIFICATIONS (with safe send)
# ═══════════════════════════════════════════
async def log_ch(ctx, uid, msg):
    ch = await db.get_log(uid)
    if not ch:
        return
    try:
        await safe_send(ctx.bot, ch, msg)
    except Exception as e:
        print(f"[LOG] {uid}: {e}")


async def tell(ctx, uid, txt):
    try:
        await safe_send(ctx.bot, uid, txt)
    except Exception as e:
        print(f"[TELL] {uid}: {e}")


# ═══════════════════════════════════════════
#  LINK / TOPIC / BUTTON HELPERS
# ═══════════════════════════════════════════
def parse_link(link):
    try:
        if '/c/' in link:
            m = re.search(r'/c/(\d+)/(\d+)', link)
            if m:
                return int("-100" + m.group(1)), int(m.group(2))
        else:
            m = re.search(r't\.me/([a-zA-Z_]\w*)/(\d+)', link)
            if m:
                return m.group(1), int(m.group(2))
    except Exception:
        pass
    return None, None


async def fetch_msg(cl, link):
    cid, mid = parse_link(link)
    if not cid or not mid:
        return None, None
    try:
        ent = None
        if isinstance(cid, str):
            for a in [f"@{cid}", cid]:
                try:
                    ent = await cl.get_entity(a)
                    break
                except Exception:
                    pass
        else:
            try:
                ent = await cl.get_entity(cid)
            except Exception:
                pass
        if not ent:
            return None, None
        msg = await cl.get_messages(ent, ids=mid)
        return (ent, msg) if msg else (None, None)
    except Exception as e:
        print(f"[FETCH] {e}")
        return None, None


async def find_topic(cl, grp, name):
    if not name or not HAS_FORUM:
        return None
    try:
        r = await cl(GetForumTopicsRequest(
            channel=grp, offset_date=None,
            offset_id=0, offset_topic=0, limit=100, q=''))
        s = name.lower().strip()
        for t in r.topics:
            if hasattr(t, 'title') and t.title and t.title.lower().strip() == s:
                return t.id
        for t in r.topics:
            if hasattr(t, 'title') and t.title and s in t.title.lower():
                return t.id
        for t in r.topics:
            if hasattr(t, 'title') and t.title and t.title.lower().strip() in s:
                return t.id
    except Exception as e:
        print(f"[TOPIC] {e}")
    return None


def parse_btns(text):
    btns = []
    if not text:
        return btns
    for line in text.strip().split('\n'):
        line = line.strip()
        if not line:
            continue
        bt = bu = None
        md = re.match(r'\[(.+?)\]\((.+?)\)', line)
        if md:
            bt, bu = md.group(1).strip(), md.group(2).strip()
        elif ' - ' in line:
            p = line.split(' - ', 1)
            bt, bu = p[0].strip(), p[1].strip()
        elif ' | ' in line:
            p = line.split(' | ', 1)
            bt, bu = p[0].strip(), p[1].strip()
        if bt and bu:
            if not bu.startswith(('http://', 'https://', 'tg://')):
                bu = 'https://' + bu
            btns.append([bt, bu])
    return btns


def tl_btns(bl):
    return [[Button.url(t, u)] for t, u in bl] if bl else None


def ptb_kb(bl):
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(t, url=u)] for t, u in bl]) if bl else None


# ═══════════════════════════════════════════
#  ADD BUTTONS TO EXISTING POST
# ═══════════════════════════════════════════
async def add_btns_to_post(uid, ctx, link, btns):
    cl = None
    try:
        cl = await get_cl(uid)
        if not cl.is_connected():
            await cl.connect()
        if await cl.is_user_authorized():
            ent, msg = await fetch_msg(cl, link)
            if ent and msg:
                kw = {"entity": ent, "message": msg.id, "buttons": tl_btns(btns)}
                txt = msg.text or msg.raw_text
                if txt:
                    kw["text"] = txt
                if msg.entities:
                    kw["formatting_entities"] = msg.entities
                try:
                    await cl.edit_message(**kw)
                    await cl.disconnect()
                    return True, f"Done! {len(btns)} button row(s) added via your account."
                except Exception as e:
                    if "not modified" in str(e).lower():
                        await cl.disconnect()
                        return True, "Buttons already identical - no changes."
                    print(f"[BTN-TL] {e}")
        if cl.is_connected():
            await cl.disconnect()
    except Exception as e:
        print(f"[BTN-TL] {e}")
        if cl:
            try:
                await cl.disconnect()
            except:
                pass

    try:
        cid, mid = parse_link(link)
        if not cid or not mid:
            return False, "Could not parse the link."
        if isinstance(cid, str) and not cid.startswith('@'):
            cid = f"@{cid}"
        await ctx.bot.edit_message_reply_markup(
            chat_id=cid, message_id=mid, reply_markup=ptb_kb(btns))
        return True, f"Done! {len(btns)} button row(s) added via bot."
    except Exception as e:
        es = str(e).lower()
        if "not modified" in es:
            return True, "Buttons already identical."
        if "chat not found" in es or "not a member" in es:
            return False, "Cannot access channel. Your account or this bot must be admin."
        return False, f"Error: {_err(e)}"


# ═══════════════════════════════════════════
#  BROADCAST ENGINE
# ═══════════════════════════════════════════
async def do_broadcast(uid, content, ctx,
                       topic=None, mode='copy',
                       buttons=None, link=None):
    sent = failed = skipped = 0
    ok_g, fail_g, skip_g = [], [], []
    cl = None
    uncancel(uid)
    content = content or {}

    try:
        try:
            cl = await asyncio.wait_for(get_cl(uid), timeout=15)
            if not cl.is_connected():
                await asyncio.wait_for(cl.connect(), timeout=15)
            if not await cl.is_user_authorized():
                await tell(ctx, uid, "Session expired - reconnect from /start")
                try:
                    await db.schedules.update_one(
                        {"user_id": uid}, {"$set": {"active": False}})
                except:
                    pass
                return "Session expired"
        except Exception as e:
            await tell(ctx, uid, "Connection failed - will retry next cycle.")
            return "Connection error"

        if cancelled(uid):
            return "Cancelled"

        tl = None
        src = None
        txt = content.get("text") or content.get("caption")

        if link:
            src, tl = await fetch_msg(cl, link)
            if not tl:
                await tell(ctx, uid, "Could not fetch that post. Check link & access.")
                return "Link fetch failed"

        if not tl and content.get("message_id"):
            try:
                bm = await ctx.bot.get_me()
                try:
                    be = await cl.get_input_entity(f"@{bm.username}")
                except:
                    be = await cl.get_entity(bm.username)
                f = await cl.get_messages(be, ids=content["message_id"])
                if f:
                    tl = f
                    src = be
            except Exception as e:
                print(f"[CONTENT-B] {e}")

        if not tl and not txt:
            try:
                sc = await db.schedules.find_one({"user_id": uid})
                if sc:
                    cd = sc.get("content_data") or {}
                    txt = cd.get("text") or sc.get("backup")
                    if not txt:
                        sl = sc.get("link")
                        if sl:
                            src, tl = await fetch_msg(cl, sl)
            except:
                pass

        if not tl and not txt:
            await tell(ctx, uid,
                       "No content found. Create a new broadcast from /start.")
            return "No content"

        has_tl = tl is not None
        if cancelled(uid):
            return "Cancelled"

        actual = mode
        tb = tl_btns(buttons) if buttons else None
        if actual == 'forward' and tb:
            actual = 'copy'

        if has_tl and actual == 'forward' and not src:
            try:
                if hasattr(tl, 'fwd_from') and tl.fwd_from:
                    if hasattr(tl.fwd_from, 'from_id') and tl.fwd_from.from_id:
                        src = await cl.get_entity(tl.fwd_from.from_id)
                if not src and hasattr(tl, 'peer_id'):
                    src = await cl.get_entity(tl.peer_id)
            except Exception as e:
                print(f"[SRC] {e}")

        groups = []
        total = sk_ch = sk_pr = sk_ban = 0
        try:
            async for d in cl.iter_dialogs(limit=None):
                total += 1
                e = d.entity
                if isinstance(e, Channel):
                    if not e.megagroup:
                        sk_ch += 1
                        continue
                elif isinstance(e, User):
                    sk_pr += 1
                    continue
                elif not isinstance(e, Chat):
                    continue
                groups.append(e)
        except Exception as e:
            print(f"[SCAN] {e}")

        if not groups:
            await tell(ctx, uid, f"No groups found ({total} dialogs scanned).")
            return "No groups"

        for i, grp in enumerate(groups):
            if cancelled(uid):
                await tell(ctx, uid,
                           f"Stopped! Sent: {sent} | Failed: {failed} | Skipped: {skipped}")
                break

            title = getattr(grp, 'title', f"ID:{grp.id}")
            try:
                is_forum = hasattr(grp, 'forum') and grp.forum
                reply_to = None

                if is_forum:
                    if topic is None:
                        reply_to = 1
                    else:
                        tid = await find_topic(cl, grp, topic)
                        reply_to = tid if tid else 1

                if actual == 'forward' and has_tl:
                    try:
                        if HAS_FWD_REQ:
                            from_peer = src if src else await cl.get_input_entity(tl.peer_id)
                            fr = await cl(ForwardMessagesRequest(
                                from_peer=from_peer, to_peer=grp,
                                id=[tl.id],
                                random_id=[random.randint(0, 2 ** 63)],
                                top_msg_id=reply_to if reply_to else None,
                                silent=False, noforwards=False,
                                drop_author=False))
                            if not _sent_ok(fr):
                                raise RuntimeError("forward returned no message id")
                        else:
                            fk = {'entity': grp, 'messages': tl.id}
                            if src:
                                fk['from_peer'] = src
                            fr = await cl.forward_messages(**fk)
                            if not _sent_ok(fr):
                                raise RuntimeError("forward returned no message id")
                        sent += 1
                        ok_g.append(title)
                    except Exception as fe:
                        fs = str(fe).lower()
                        if any(x in fs for x in (
                                'forward', 'restricted', 'noforwards',
                                'message_id_invalid', 'peer_id_invalid')):
                            try:
                                kw = {"entity": grp, "reply_to": reply_to,
                                      "formatting_entities": tl.entities}
                                if tl.media:
                                    kw["message"] = tl.text or ''
                                    kw["file"] = tl.media
                                else:
                                    kw["message"] = tl.text or tl.raw_text or ''
                                sr = await cl.send_message(**kw)
                                if not _sent_ok(sr):
                                    raise RuntimeError("copy fallback failed")
                                sent += 1
                                ok_g.append(f"{title} (copy)")
                            except Exception as ce:
                                failed += 1
                                fail_g.append(f"{title}: {str(ce)[:40]}")
                        else:
                            failed += 1
                            fail_g.append(f"{title}: {str(fe)[:40]}")

                elif has_tl:
                    kw = {"entity": grp, "reply_to": reply_to,
                          "buttons": tb, "formatting_entities": tl.entities}
                    if tl.media:
                        kw["message"] = tl.text or ''
                        kw["file"] = tl.media
                    else:
                        kw["message"] = tl.text or tl.raw_text or ''
                    sr = await cl.send_message(**kw)
                    if not _sent_ok(sr):
                        raise RuntimeError("copy returned no message id")
                    sent += 1
                    ok_g.append(title)

                elif txt:
                    sr = await cl.send_message(
                        grp, txt, reply_to=reply_to, buttons=tb)
                    if not _sent_ok(sr):
                        raise RuntimeError("text send returned no message id")
                    sent += 1
                    ok_g.append(title)

                if i < len(groups) - 1:
                    await asyncio.sleep(max(2, DELAY + random.uniform(-1, 1)))

            except FloodWaitError as e:
                failed += 1
                fail_g.append(f"{title}: Flood {e.seconds}s")
                await asyncio.sleep(e.seconds + 2)
            except Exception as e:
                failed += 1
                fail_g.append(f"{title}: {str(e)[:40]}")
                if failed >= 20 and sent == 0:
                    break

        try:
            if cl and cl.is_connected():
                await cl.disconnect()
        except:
            pass

        if sent > 0:
            await db.inc_msgs(uid)

        ms = "Copy" if actual == 'copy' else "Forward"
        ts = f"Topic: {topic}\n" if topic else ""
        summary = (
            f"Sent: {sent}\n"
            f"Failed: {failed}\n"
            f"Skipped: {skipped}\n"
            f"Groups: {len(groups)}")

        log_text = (
            f"--- Report ({ms}) ---\n{ts}\n{summary}\n\n"
            f"{total} dialogs | {sk_ch} ch | {sk_pr} DMs | {sk_ban} restricted\n")
        if ok_g[:15]:
            log_text += "\nDelivered:\n" + "\n".join(f"  - {g}" for g in ok_g[:15])
        if fail_g[:10]:
            log_text += "\n\nFailed:\n" + "\n".join(f"  - {g}" for g in fail_g[:10])

        await log_ch(ctx, uid, log_text)

        smp = "\n".join(f"  - {g}" for g in ok_g[:5]) if ok_g else "  - (none)"
        await tell(ctx, uid,
                   f"Broadcast Complete!\n\n{summary}\n\nSample delivered groups:\n{smp}")
        uncancel(uid)
        return summary

    except Exception as e:
        print(f"CRITICAL: {traceback.format_exc()}")
        await tell(ctx, uid, f"Error: {_err(e)}")
        return "Error"
    finally:
        try:
            if cl and cl.is_connected():
                await cl.disconnect()
        except:
            pass
        tasks[uid] = max(0, tasks.get(uid, 1) - 1)
        uncancel(uid)


# ═══════════════════════════════════════════
#  SCHEDULE LOOP
# ═══════════════════════════════════════════
async def sched_loop(uid, interval, ctx,
                     topic=None, mode='copy',
                     buttons=None, link=None):
    interval = _int(interval, 1, lo=1, hi=10080)
    errors = 0

    try:
        sc = await db.schedules.find_one({"user_id": uid})
        if not sc or not sc.get("next_run"):
            await db.save_sched(uid, {
                "interval": interval, "topic": topic,
                "mode": mode, "buttons": buttons, "link": link,
                "backup": ctx.user_data.get('bc_backup') if hasattr(ctx, 'user_data') else None,
                "content_data": ctx.user_data.get('bc_content') if hasattr(ctx, 'user_data') else None,
                "next_run": (datetime.now() + timedelta(minutes=interval)).isoformat(),
                "active": True})

        print(f"[SCHED] uid={uid} every {interval}m")

        while True:
            sc = None
            for retry in range(5):
                try:
                    sc = await db.schedules.find_one({"user_id": uid})
                    break
                except Exception as e:
                    w = (retry + 1) * 15
                    print(f"[SCHED] DB retry {retry + 1}/5 uid={uid} in {w}s: {e}")
                    await asyncio.sleep(w)

            if sc is None:
                print(f"[SCHED] DB down for {uid}, sleep 5m")
                await asyncio.sleep(300)
                continue

            if not sc.get("active") or cancelled(uid):
                break

            d_iv = _int(sc.get("interval", interval), interval, lo=1)
            d_topic = sc.get("topic", topic)
            d_mode = sc.get("mode", mode)
            d_btns = sc.get("buttons", buttons)
            d_link = sc.get("link", link)
            d_cd = sc.get("content_data") or {}

            nrs = sc.get("next_run")
            if not nrs:
                nrd = datetime.now() + timedelta(minutes=d_iv)
                try:
                    await db.schedules.update_one(
                        {"user_id": uid}, {"$set": {"next_run": nrd.isoformat()}})
                except:
                    pass
                nrs = nrd.isoformat()

            try:
                nrd = datetime.fromisoformat(nrs)
            except:
                nrd = datetime.now() + timedelta(minutes=d_iv)

            diff = (nrd - datetime.now()).total_seconds()
            if diff > 0:
                await asyncio.sleep(min(diff, 30))
                continue

            try:
                rpt = await asyncio.wait_for(
                    do_broadcast(uid, d_cd, ctx, d_topic, d_mode, d_btns, d_link),
                    timeout=3600)
                errors = 0

                tgt = nrd + timedelta(minutes=d_iv)
                while tgt < datetime.now():
                    tgt += timedelta(minutes=d_iv)

                for _ in range(3):
                    try:
                        await db.schedules.update_one(
                            {"user_id": uid},
                            {"$set": {"next_run": tgt.isoformat(),
                                      "last_run": datetime.now().isoformat()}})
                        break
                    except:
                        await asyncio.sleep(5)

                await tell(ctx, uid,
                           f"Scheduled run complete\n\n{rpt}\n\n"
                           f"Next: {tgt.strftime('%Y-%m-%d %H:%M')}")

            except asyncio.TimeoutError:
                errors += 1
                tgt = datetime.now() + timedelta(minutes=d_iv)
                try:
                    await db.schedules.update_one(
                        {"user_id": uid}, {"$set": {"next_run": tgt.isoformat()}})
                except:
                    pass
                await log_ch(ctx, uid, f"Timed out. Next: {tgt.isoformat()}")

            except Exception as e:
                errors += 1
                tgt = datetime.now() + timedelta(minutes=d_iv)
                try:
                    await db.schedules.update_one(
                        {"user_id": uid}, {"$set": {"next_run": tgt.isoformat()}})
                except:
                    pass
                await log_ch(ctx, uid, f"Error #{errors}: {_err(e)}")
                if errors >= 5:
                    await tell(ctx, uid, "5 failures. Slowing down. /stop to cancel.")
                    await asyncio.sleep(d_iv * 60)
                if errors >= 20:
                    try:
                        await db.schedules.update_one(
                            {"user_id": uid}, {"$set": {"active": False}})
                    except:
                        pass
                    await tell(ctx, uid,
                               "Auto-stopped after 20 failures. Check session and restart.")
                    break

            await asyncio.sleep(5)

    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[SCHED] FATAL {uid}: {e}")
        try:
            await log_ch(ctx, uid, f"Loop crash: {_err(e, 200)}")
        except:
            pass


# ═══════════════════════════════════════════
#  /start
# ═══════════════════════════════════════════
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if not u:
        return
    uid = u.id
    msg = update.effective_message
    if not msg:
        return

    await db.init()
    await db.add_user(uid, u.username, u.first_name)
    await db.touch(uid)

    ok = AUTH.get(uid)
    if ok is None:
        ud = await db.user(uid)
        if uid == Config.OWNER_ID:
            await db.authorize(uid, True)
            ok = True
        else:
            ok = ud.get("authorized", False) if ud else False
        AUTH[uid] = ok

    if not ok:
        ud = await db.user(uid)
        pend = ud.get("pending", False) if ud else False
        if pend:
            t = (
                "Request Pending\n\n"
                "Your access request is under review.\n"
                "You'll be notified once approved."
            )
            kb = [[InlineKeyboardButton("Check Status", callback_data="start")]]
        else:
            t = (
                "Access Required\n\n"
                "This bot requires owner approval.\n"
                "Tap below to request access."
            )
            kb = [[InlineKeyboardButton("Request Access", callback_data="req_access")]]
        await safe_reply(msg, t, reply_markup=InlineKeyboardMarkup(kb))
        return

    kb = [
        [InlineKeyboardButton("Broadcast", callback_data="menu")],
        [InlineKeyboardButton("Add Buttons to Post", callback_data="tool_btns")],
        [InlineKeyboardButton("Help", callback_data="help"),
         InlineKeyboardButton("Dev", url="https://t.me/knwnasguru")],
    ]
    if uid == Config.OWNER_ID:
        kb.insert(0, [InlineKeyboardButton("Admin Panel", callback_data="admin")])

    await safe_reply(msg,
                     f"Hey {u.first_name}!\n\n"
                     "ADS Pro Broadcast Bot\n\n"
                     "- Broadcast to all your groups\n"
                     "- Anonymous copy or forward mode\n"
                     "- Inline URL buttons on any post\n"
                     "- Forum topic targeting (any language)\n"
                     "- Scheduled auto-repeat broadcasts\n"
                     "- Personal log channel for reports\n\n"
                     "Tap Broadcast to get started!",
                     reply_markup=InlineKeyboardMarkup(kb))


async def safe_reply(msg, text, **kwargs):
    """Reply with Markdown, auto-fallback to plain text."""
    try:
        return await msg.reply_text(text, parse_mode=ParseMode.MARKDOWN, **kwargs)
    except Exception as e:
        if _is_entity_parse_error(e):
            clean = _strip_md(text)
            kw = {k: v for k, v in kwargs.items()
                  if k not in ('parse_mode', 'entities', 'caption_entities')}
            return await msg.reply_text(clean, parse_mode=None, **kw)
        raise


async def safe_edit_text(query, text, **kwargs):
    """Edit callback query message with Markdown, auto-fallback to plain."""
    try:
        return await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, **kwargs)
    except Exception as e:
        if _is_entity_parse_error(e):
            clean = _strip_md(text)
            kw = {k: v for k, v in kwargs.items()
                  if k not in ('parse_mode', 'entities', 'caption_entities')}
            return await query.edit_message_text(clean, parse_mode=None, **kw)
        if "not modified" in str(e).lower():
            return None
        raise


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    cancel(uid, True)
    for k in ('client', 'gen_client'):
        c = context.user_data.get(k)
        if c:
            try:
                if c.is_connected():
                    await c.disconnect()
            except:
                pass
    stopped = 0
    if uid in scheds:
        for t in scheds[uid]:
            if not t.done():
                t.cancel()
                stopped += 1
        scheds[uid] = []
    await db.del_sched(uid)
    tasks[uid] = 0
    context.user_data.clear()
    m = "All operations stopped.\n"
    if stopped:
        m += f"  {stopped} schedule(s) cancelled\n"
    m += "\nUse /start to begin again."
    await safe_reply(update.message, m)
    return ConversationHandler.END


# ═══════════════════════════════════════════
#  CALLBACK HANDLER
# ═══════════════════════════════════════════
async def on_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = update.effective_user
    if not u:
        return
    uid = u.id
    d = q.data
    await db.touch(uid)

    if d.startswith("mode_"):
        context.user_data['bc_mode'] = d.split("_", 1)[1]
        await safe_edit_text(q,
                             "Topic Targeting\n\n"
                             "Type a topic name to target:\n"
                             "  Forum groups: sends to matching topic\n"
                             "  Non-forum groups: sends normally\n"
                             "  Partial match in any language\n\n"
                             "Type 'skip' to send to all groups.\n\n"
                             "Send your choice:")
        context.user_data['w_topic'] = True
        return

    elif d == "start":
        return await start(update, context)

    elif d == "req_access":
        if uid == Config.OWNER_ID:
            await db.authorize(uid, True)
            return await start(update, context)
        ud = await db.user(uid)
        if ud and ud.get("authorized"):
            AUTH[uid] = True
            await safe_edit_text(q, "You're already authorized!")
            return
        await db.set_pending(uid)
        try:
            await safe_send(context.bot, Config.OWNER_ID,
                            f"New Access Request\n\n"
                            f"  Name: {u.first_name}\n"
                            f"  ID: {uid}\n"
                            f"  Username: @{u.username or 'N/A'}",
                            reply_markup=InlineKeyboardMarkup([[
                                InlineKeyboardButton("Approve", callback_data=f"apr_{uid}"),
                                InlineKeyboardButton("Deny", callback_data=f"den_{uid}")]]))
        except:
            pass
        await safe_edit_text(q,
                             "Request Sent\n\n"
                             "Your request has been forwarded to the admin.\n"
                             "You'll be notified once reviewed.")
        return

    elif d.startswith(("apr_", "den_")):
        if uid != Config.OWNER_ID:
            return
        tid = int(d.split("_")[1])
        ap = d.startswith("apr_")
        await db.authorize(tid, ap)
        AUTH[tid] = ap
        await safe_edit_text(q, f"{'Approved' if ap else 'Denied'} - {tid}")
        try:
            if ap:
                await safe_send(context.bot, tid, "Access approved!\n\nUse /start to begin.")
            else:
                await safe_send(context.bot, tid, "Access denied by admin.")
        except:
            pass
        return

    elif d == "admin":
        if uid != Config.OWNER_ID:
            return
        st = await db.get_stats()
        kb = [
            [InlineKeyboardButton("Pending Requests", callback_data="pending"),
             InlineKeyboardButton("All Users", callback_data="users")],
            [InlineKeyboardButton("Back", callback_data="back")],
        ]
        await safe_edit_text(q,
                             f"Admin Panel\n\n"
                             f"  Users: {st.get('users', 0)}\n"
                             f"  Messages sent: {st.get('msgs', 0)}",
                             reply_markup=InlineKeyboardMarkup(kb))
        return

    elif d == "pending":
        if uid != Config.OWNER_ID:
            return
        us = await db.all_users()
        p = [u2 for u2 in us if u2.get("pending") and not u2.get("authorized")]
        if not p:
            await q.answer("No pending requests")
            return
        t = "Pending Requests\n\n"
        kb = []
        for u2 in p[:10]:
            t += f"  - {u2['first_name']} -- {u2['user_id']}\n"
            kb.append([
                InlineKeyboardButton(f"Approve {u2['first_name']}", callback_data=f"apr_{u2['user_id']}"),
                InlineKeyboardButton("Deny", callback_data=f"den_{u2['user_id']}")])
        kb.append([InlineKeyboardButton("Back", callback_data="admin")])
        await safe_edit_text(q, t, reply_markup=InlineKeyboardMarkup(kb))
        return

    elif d == "users":
        if uid != Config.OWNER_ID:
            return
        us = await db.all_users()
        t = "Users\n\n"
        for u2 in us[:20]:
            s = "OK" if u2.get("authorized") else ("Pending" if u2.get("pending") else "No")
            a = "Auth" if u2.get("authed") else "NoAuth"
            t += f"  [{s}][{a}] {u2['first_name']} {u2['user_id']}\n"
        kb = [[InlineKeyboardButton("Back", callback_data="admin")]]
        await safe_edit_text(q, t, reply_markup=InlineKeyboardMarkup(kb))
        return

    elif d == "menu":
        ud = await db.user(uid)
        if not ud or not ud.get("authed"):
            kb = [[InlineKeyboardButton("Connect Account", callback_data="auth")]]
            await safe_edit_text(q,
                                 "Account Connection Required\n\n"
                                 "Connect your Telegram account to\n"
                                 "start broadcasting to your groups.",
                                 reply_markup=InlineKeyboardMarkup(kb))
        else:
            await safe_edit_text(q, "Verifying session...")
            ok = await check_sess(uid)
            if ok:
                lc = await db.get_log(uid)
                ls = "Set" if lc else "Not set"
                kb = [
                    [InlineKeyboardButton("New Broadcast", callback_data="new_bc")],
                    [InlineKeyboardButton(f"Log Channel [{ls}]", callback_data="log_setup"),
                     InlineKeyboardButton("Export Session", callback_data="export")],
                    [InlineKeyboardButton("Refresh", callback_data="refresh"),
                     InlineKeyboardButton("Disconnect", callback_data="logout")],
                    [InlineKeyboardButton("Main Menu", callback_data="back")],
                ]
                await safe_edit_text(q,
                                     f"Session Active\n\n"
                                     f"  Phone: {ud.get('phone', '?')}\n"
                                     f"  Broadcasts: {ud.get('sent', 0)}",
                                     reply_markup=InlineKeyboardMarkup(kb))
            else:
                kb = [[InlineKeyboardButton("Reconnect", callback_data="auth")],
                      [InlineKeyboardButton("Back", callback_data="back")]]
                await safe_edit_text(q,
                                     "Session Expired\n\nReconnect to continue broadcasting.",
                                     reply_markup=InlineKeyboardMarkup(kb))

    elif d == "log_setup":
        lc = await db.get_log(uid)
        kb = [[InlineKeyboardButton("How to Get ID", callback_data="log_id")],
              [InlineKeyboardButton("Set Channel", callback_data="log_set")]]
        if lc:
            kb.insert(0, [InlineKeyboardButton("Remove", callback_data="log_rm")])
        kb.append([InlineKeyboardButton("Back", callback_data="menu")])
        await safe_edit_text(q,
                             f"Log Channel Setup\n\n"
                             f"Current: {str(lc) if lc else 'not configured'}\n\n"
                             "The bot sends detailed delivery reports here\nafter each broadcast.",
                             reply_markup=InlineKeyboardMarkup(kb))

    elif d == "log_id":
        kb = [[InlineKeyboardButton("Back", callback_data="log_setup")]]
        await safe_edit_text(q,
                             "How to Get Channel ID\n\n"
                             "1. Forward any message from your channel\n"
                             "   to @JsonDumpBot\n"
                             "2. Copy the chat.id value\n"
                             "   (e.g. -1001234567890)\n\n"
                             "This bot must be admin in that channel.",
                             reply_markup=InlineKeyboardMarkup(kb))

    elif d == "log_set":
        await safe_edit_text(q,
                             "Set Log Channel\n\n"
                             "Send the channel ID:\n-1001234567890\n\n"
                             "Use /cancel to abort.")
        context.user_data['w_log'] = True

    elif d == "log_rm":
        await db.set_log(uid, None)
        await safe_edit_text(q, "Log channel removed.\n\nUse /start to continue.")

    elif d == "auth":
        kb = [
            [InlineKeyboardButton("Generate Session", callback_data="auth_gen")],
            [InlineKeyboardButton("Phone + OTP", callback_data="auth_otp")],
            [InlineKeyboardButton("Paste Session String", callback_data="auth_paste")],
            [InlineKeyboardButton("Back", callback_data="menu")],
        ]
        await safe_edit_text(q,
                             "Connect Your Account\n\n"
                             "- Generate Session (Recommended)\n"
                             "- Phone + OTP (Traditional)\n"
                             "- Paste Session (Existing string)\n",
                             reply_markup=InlineKeyboardMarkup(kb))

    elif d == "auth_gen":
        await safe_edit_text(q,
                             "Generate Session\n\n"
                             "Send your phone number:\n+1234567890\n\n"
                             "Use /cancel to abort.")
        return GEN_PHONE

    elif d == "auth_otp":
        await safe_edit_text(q,
                             "Phone + OTP\n\n"
                             "Send your phone number:\n+1234567890\n\n"
                             "Use /cancel to abort.")
        return PHONE

    elif d == "auth_paste":
        await safe_edit_text(q,
                             "Paste Session String\n\n"
                             "Send your Telethon session string.\n\n"
                             "Use /cancel to abort.")
        context.user_data['w_sess'] = True

    elif d == "new_bc":
        for k in list(context.user_data.keys()):
            if k.startswith(('bc_', 'w_')):
                context.user_data.pop(k, None)
        kb = [
            [InlineKeyboardButton("Compose Message", callback_data="bc_direct")],
            [InlineKeyboardButton("From Post Link", callback_data="bc_link")],
            [InlineKeyboardButton("Cancel", callback_data="cancel_bc")],
        ]
        await safe_edit_text(q,
                             "New Broadcast\n\n"
                             "- Compose Message: Send any content directly\n"
                             "  (text, photo, video, etc.)\n\n"
                             "- From Post Link: Copy/forward from a t.me link",
                             reply_markup=InlineKeyboardMarkup(kb))

    elif d == "bc_direct":
        await safe_edit_text(q,
                             "Send Your Content\n\n"
                             "Send any message now:\n"
                             "  - Text (links in text preserved)\n"
                             "  - Photo with caption\n"
                             "  - Video / Document / Audio\n\n"
                             "Use /cancel to abort.")
        context.user_data['w_content'] = True

    elif d == "bc_link":
        await safe_edit_text(q,
                             "Source Post Link\n\n"
                             "Send the link to the post:\n"
                             "https://t.me/channel/123\n\n"
                             "The bot will fetch and broadcast it.\n\n"
                             "Use /cancel to abort.")
        context.user_data['w_link'] = True

    elif d == "add_btns":
        await safe_edit_text(q,
                             "Inline Buttons\n\n"
                             "Send one button per line:\n"
                             "Button Text - https://url.com\n\n"
                             "Supported formats:\n"
                             "  Text - URL\n"
                             "  Text | URL\n"
                             "  [Text](URL)\n\n"
                             "Use /cancel to abort.")
        context.user_data['w_btns'] = True

    elif d == "edit_content":
        m = context.user_data.get('bc_mode', 'copy')
        if m == 'forward':
            await safe_edit_text(q, "Send the new post link.\n\nUse /cancel to abort.")
        else:
            await safe_edit_text(q, "Send updated content.\n\nUse /cancel to abort.")
        context.user_data['w_edit'] = True

    elif d == "tool_btns":
        await safe_edit_text(q,
                             "Add Buttons to Channel Post\n\n"
                             "Your account or this bot must be\nadmin in the channel.\n\n"
                             "Send the post link:\nhttps://t.me/mychannel/123\n\n"
                             "Use /cancel to abort.")
        context.user_data['w_il'] = True

    elif d == "reset_bc":
        for k in ['bc_btns', 'bc_mode', 'bc_interval', 'bc_topic']:
            context.user_data.pop(k, None)
        kb = [
            [InlineKeyboardButton("Copy (Anonymous)", callback_data="mode_copy")],
            [InlineKeyboardButton("Forward (With Header)", callback_data="mode_forward")],
            [InlineKeyboardButton("Cancel", callback_data="cancel_bc")],
        ]
        await safe_edit_text(q, "Settings Reset\n\nChoose delivery mode:",
                             reply_markup=InlineKeyboardMarkup(kb))

    elif d == "refresh":
        await safe_edit_text(q, "Checking session...")
        ok = await check_sess(uid)
        if ok:
            await safe_edit_text(q, "Session is active.\n\nUse /start to continue.")
        else:
            kb = [[InlineKeyboardButton("Reconnect", callback_data="auth")]]
            await safe_edit_text(q, "Session expired.",
                                 reply_markup=InlineKeyboardMarkup(kb))

    elif d == "logout":
        await db.set_auth(uid, None)
        await safe_edit_text(q, "Disconnected\n\nUse /start to reconnect.")

    elif d == "export":
        ss = await db.session(uid)
        if ss:
            # Session strings are safe alphanumeric — send without Markdown
            try:
                await q.edit_message_text(
                    f"Your Session String\n\n{ss}\n\n"
                    "Keep this private!\n"
                    "Anyone with this string can access your Telegram account.",
                    parse_mode=None)
            except Exception as e:
                if "not modified" not in str(e).lower():
                    raise
        else:
            await safe_edit_text(q, "No session found.")

    elif d == "help":
        kb = [[InlineKeyboardButton("Back", callback_data="back")]]
        await safe_edit_text(q,
                             "Help & Guide\n\n"
                             "Broadcast Flow:\n\n"
                             "  1. Connect your Telegram account\n"
                             "  2. Compose message or pick post link\n"
                             "  3. Choose Copy or Forward mode\n"
                             "  4. Set topic targeting\n"
                             "  5. Set interval (0 = send once)\n"
                             "  6. Confirm & launch\n\n"
                             "Topic Targeting:\n\n"
                             "  - Type a topic name:\n"
                             "    Forum groups -> targets that topic\n"
                             "    Non-forum groups -> normal delivery\n"
                             "    Works in ANY language (partial match)\n\n"
                             "  - Type 'skip':\n"
                             "    Forum groups use General topic\n"
                             "    Non-forum groups receive normally\n\n"
                             "Delivery Modes:\n\n"
                             "  - Copy: anonymous, supports buttons\n"
                             "  - Forward: shows source header\n\n"
                             "Button Format:\n\n"
                             "  Button Text - https://url.com\n\n"
                             "Commands:\n\n"
                             "  /start - Main menu\n"
                             "  /cancel - Stop everything\n"
                             "  /stop - Stop broadcasts\n"
                             "  /fix - Reset stuck state",
                             reply_markup=InlineKeyboardMarkup(kb))

    elif d == "back":
        return await start(update, context)


# ═══════════════════════════════════════════
#  PREVIEW
# ═══════════════════════════════════════════
async def show_preview(obj, ctx):
    c = ctx.user_data.get('bc_content', {})
    pl = ctx.user_data.get('bc_link')
    tn = ctx.user_data.get('bc_topic')
    iv = _int(ctx.user_data.get('bc_interval', 0), 0)
    bt = ctx.user_data.get('bc_btns', [])
    md = ctx.user_data.get('bc_mode', 'copy')

    if c.get('text'):
        pv = c['text'][:200]
    elif c.get('caption'):
        pv = f"[{c.get('media_type', 'media')}] {c['caption'][:150]}"
    elif c.get('media_type'):
        pv = f"[{c['media_type']}]"
    elif pl:
        pv = f"Link: {pl}"
    else:
        pv = "- no content -"

    # Sanitize preview text to avoid Markdown issues
    pv = _strip_md(pv)

    td = f"'{tn}' + all non-forum groups" if tn else "General topic in forums + all non-forum groups"
    ivd = "one-time, now" if iv == 0 else f"every {iv} minute(s)"
    mdd = "Copy (anonymous)" if md == 'copy' else "Forward (with header)"
    warn = "\n(Buttons force Copy mode)" if bt and md == 'forward' else ""

    bl = "Send Now" if iv == 0 else f"Start Schedule ({iv}m)"
    kb = [
        [InlineKeyboardButton(bl, callback_data="confirm_bc")],
        [InlineKeyboardButton(f"Buttons ({len(bt)})", callback_data="add_btns"),
         InlineKeyboardButton("Edit", callback_data="edit_content")],
        [InlineKeyboardButton("Reset", callback_data="reset_bc"),
         InlineKeyboardButton("Cancel", callback_data="cancel_bc")],
    ]

    t = (
        f"Broadcast Preview\n\n"
        f"{pv}\n\n"
        f"---\n"
        f"  Target: {td}\n"
        f"  Mode: {mdd}{warn}\n"
        f"  Buttons: {len(bt)} row(s)\n"
        f"  Interval: {ivd}\n"
        f"  Delay: {DELAY}s between groups\n"
        f"---\n\n"
        f"Ready to launch?"
    )

    mk = InlineKeyboardMarkup(kb)
    try:
        if hasattr(obj, 'callback_query') and obj.callback_query:
            await safe_edit_text(obj.callback_query, t, reply_markup=mk)
        elif hasattr(obj, 'message') and obj.message:
            await safe_reply(obj.message, t, reply_markup=mk)
    except Exception:
        if hasattr(obj, 'effective_message') and obj.effective_message:
            await safe_reply(obj.effective_message, t, reply_markup=mk)


# ═══════════════════════════════════════════
#  MESSAGE HANDLER
# ═══════════════════════════════════════════
async def on_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if not update.effective_user:
        return
    uid = update.effective_user.id
    msg = update.message

    if context.user_data.get('w_sess'):
        context.user_data['w_sess'] = False
        ss = (msg.text or "").strip()
        if not ss:
            await safe_reply(msg, "Empty session string. /start to retry.")
            return
        await safe_reply(msg, "Validating session...")
        try:
            c = TelegramClient(StringSession(ss), Config.API_ID, Config.API_HASH)
            await c.connect()
            if not await c.is_user_authorized():
                await c.disconnect()
                await safe_reply(msg, "Invalid or expired session.")
                return
            me = await c.get_me()
            await c.disconnect()
            await db.set_auth(uid, ss)
            await safe_reply(msg, f"Connected as {me.first_name}!\n\nUse /start to continue.")
        except Exception as e:
            await safe_reply(msg, f"Error: {_err(e)}\n\n/start to retry.")
        context.user_data.clear()
        return

    if context.user_data.get('w_log'):
        context.user_data['w_log'] = False
        ch = (msg.text or "").strip()
        if not ch.lstrip('-').isdigit():
            await safe_reply(msg, "Invalid ID. Use: -1001234567890")
            return
        try:
            await context.bot.send_message(chat_id=int(ch), text="Log channel linked!",
                                           parse_mode=None)
            await db.set_log(uid, ch)
            await safe_reply(msg, f"Log channel set: {ch}")
        except Exception as e:
            await safe_reply(msg, f"Error: {_err(e, 100)}\n\nBot must be admin in the channel.")
        return

    if context.user_data.get('w_content'):
        context.user_data['w_content'] = False
        cd = extract_content(msg)
        context.user_data['bc_content'] = cd
        context.user_data['bc_link'] = None
        context.user_data['bc_backup'] = cd.get('text') or cd.get('caption')
        ct = (cd.get('media_type') or 'text').capitalize()
        kb = [
            [InlineKeyboardButton("Copy (Anonymous)", callback_data="mode_copy")],
            [InlineKeyboardButton("Forward (With Header)", callback_data="mode_forward")],
            [InlineKeyboardButton("Cancel", callback_data="cancel_bc")],
        ]
        await safe_reply(msg,
                         f"{ct} Captured\n\n"
                         "Choose delivery mode:\n\n"
                         "  Copy - anonymous, supports buttons\n"
                         "  Forward - shows 'Forwarded from' header",
                         reply_markup=InlineKeyboardMarkup(kb))
        return

    if context.user_data.get('w_link'):
        context.user_data['w_link'] = False
        t = (msg.text or "").strip()
        if 't.me/' not in t:
            await safe_reply(msg,
                             "Not a post link.\n\n"
                             "Expected: https://t.me/channel/123\n\n"
                             "If your message contains links,\n"
                             "use Compose Message instead.")
            context.user_data['w_link'] = True
            return
        ci, mi = parse_link(t)
        if not ci or not mi:
            await safe_reply(msg, "Could not parse. Check the link.")
            context.user_data['w_link'] = True
            return
        context.user_data['bc_link'] = t
        context.user_data['bc_content'] = {}
        context.user_data['bc_backup'] = None
        kb = [
            [InlineKeyboardButton("Copy", callback_data="mode_copy")],
            [InlineKeyboardButton("Forward", callback_data="mode_forward")],
            [InlineKeyboardButton("Cancel", callback_data="cancel_bc")],
        ]
        await safe_reply(msg, "Link Captured\n\nChoose delivery mode:",
                         reply_markup=InlineKeyboardMarkup(kb))
        return

    if context.user_data.get('w_il'):
        lnk = (msg.text or "").strip()
        if 't.me/' not in lnk:
            await safe_reply(msg, "Send a valid t.me post link.")
            return
        ci, mi = parse_link(lnk)
        if not ci or not mi:
            await safe_reply(msg, "Could not parse link.")
            return
        context.user_data['il_link'] = lnk
        context.user_data['w_il'] = False
        await safe_reply(msg,
                         "Link Received\n\n"
                         "Now send buttons, one per line:\n"
                         "Button Text - https://url.com\n\n"
                         "Use /cancel to abort.")
        context.user_data['w_il_btns'] = True
        return

    if context.user_data.get('w_il_btns'):
        btns = parse_btns((msg.text or "").strip())
        if not btns:
            await safe_reply(msg, "Invalid format.\nUse: Text - URL")
            return
        lnk = context.user_data.get('il_link')
        context.user_data['w_il_btns'] = False
        await safe_reply(msg, f"Adding {len(btns)} button(s)...")
        ok, result_msg = await add_btns_to_post(uid, context, lnk, btns)
        await safe_reply(msg, result_msg)
        return

    if context.user_data.get('w_topic'):
        context.user_data['w_topic'] = False
        ti = (msg.text or "").strip()
        if not ti:
            await safe_reply(msg, "Please type a topic name or 'skip'.")
            context.user_data['w_topic'] = True
            return
        if ti.lower() == 'skip':
            context.user_data['bc_topic'] = None
            desc = "General topic in forums + all non-forum groups"
        else:
            context.user_data['bc_topic'] = ti.lower()
            desc = f"Topic '{ti}' in forums + all non-forum groups"
        await safe_reply(msg,
                         f"Target: {desc}\n\n"
                         "Broadcast Interval\n\n"
                         "  0 - send once, right now\n"
                         "  5 - repeat every 5 minutes\n"
                         "  60 - repeat every hour\n\n"
                         "Send a number:")
        context.user_data['w_iv'] = True
        return

    if context.user_data.get('w_iv'):
        try:
            iv = int((msg.text or "").strip())
            if iv < 0:
                await safe_reply(msg, "Must be 0 or positive.")
                return
            context.user_data['bc_interval'] = iv
            context.user_data['w_iv'] = False
            await show_preview(update, context)
        except ValueError:
            await safe_reply(msg, "Send a number.")
        return

    if context.user_data.get('w_btns'):
        btns = parse_btns((msg.text or "").strip())
        if not btns:
            await safe_reply(msg, "Use: Text - URL")
            return
        context.user_data['bc_btns'] = btns
        context.user_data['w_btns'] = False
        await safe_reply(msg, f"{len(btns)} button row(s) added!")
        await show_preview(update, context)
        return

    if context.user_data.get('w_edit'):
        m = context.user_data.get('bc_mode', 'copy')
        if m == 'forward':
            lnk = (msg.text or "").strip()
            if 't.me/' not in lnk:
                await safe_reply(msg, "Forward mode requires a t.me link.")
                return
            context.user_data['bc_link'] = lnk
        else:
            cd = extract_content(msg)
            context.user_data['bc_content'] = cd
            context.user_data['bc_backup'] = cd.get('text') or cd.get('caption')
        context.user_data['w_edit'] = False
        await safe_reply(msg, "Content updated!")
        await show_preview(update, context)
        return


# ═══════════════════════════════════════════
#  CONFIRM BROADCAST
# ═══════════════════════════════════════════
async def confirm_bc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "cancel_bc":
        for k in list(context.user_data.keys()):
            if k.startswith(('bc_', 'w_')):
                context.user_data.pop(k, None)
        await safe_edit_text(q, "Cancelled.\n\nUse /start to try again.")
        return

    uid = update.effective_user.id
    cd = context.user_data.get('bc_content', {})
    iv = _int(context.user_data.get('bc_interval', 0), 0)
    md = context.user_data.get('bc_mode', 'copy')
    pl = context.user_data.get('bc_link')
    tn = context.user_data.get('bc_topic')
    bt = context.user_data.get('bc_btns', [])

    if md == 'copy' and not cd.get('message_id') and not cd.get('text') and not pl:
        await safe_edit_text(q, "No content found.\n\nUse /start to create a new broadcast.")
        return
    if md == 'forward' and not pl:
        await safe_edit_text(q, "Forward mode requires a post link.")
        return
    if bt and md == 'forward':
        md = 'copy'

    await safe_edit_text(q,
                         "Broadcasting...\n\n"
                         "  Scanning groups & sending\n"
                         "  Report will arrive when done\n\n"
                         "Use /cancel to stop at any time.")

    store = dict(cd)
    store["link"] = pl
    try:
        bm = await context.bot.get_me()
        store["source_peer"] = bm.username or str(bm.id)
    except:
        pass

    tasks[uid] = tasks.get(uid, 0) + 1
    uncancel(uid)

    asyncio.create_task(do_broadcast(uid, cd, context, tn, md, bt, pl))

    if iv > 0:
        if uid in scheds:
            for t in scheds[uid]:
                if not t.done():
                    t.cancel()
            scheds[uid] = []
        await db.del_sched(uid)
        await db.save_sched(uid, {
            "interval": iv, "topic": tn, "mode": md,
            "buttons": bt, "link": pl,
            "backup": cd.get("text") or cd.get("caption"),
            "content_data": store,
            "next_run": (datetime.now() + timedelta(minutes=iv)).isoformat(),
            "active": True})
        task = asyncio.create_task(
            sched_loop(uid, iv, context, tn, md, bt, pl))
        scheds[uid] = [task]
        await tell(context, uid,
                   f"Scheduled! Repeating every {iv} minute(s).\nUse /cancel to stop.")


# ═══════════════════════════════════════════
#  AUTH HANDLERS
# ═══════════════════════════════════════════
async def h_phone(u, c):
    p = (u.message.text or "").strip()
    if not p:
        await safe_reply(u.message, "Please send your phone number.")
        return PHONE
    await safe_reply(u.message, "Sending verification code...")
    try:
        cl = TelegramClient(StringSession(), Config.API_ID, Config.API_HASH)
        if not cl.is_connected():
            await cl.connect()
        r = await cl.send_code_request(p)
        c.user_data.update({'phone': p, 'hash': r.phone_code_hash, 'client': cl})
        await safe_reply(u.message,
                         "Code Sent!\n\n"
                         "Check your Telegram app or SMS.\n"
                         "Send the code here (spaces OK).")
        return CODE
    except Exception as e:
        await safe_reply(u.message, f"Error: {_err(e)}\n\n/start to retry.")
        return ConversationHandler.END


async def h_code(u, c):
    code = ''.join(filter(str.isdigit, (u.message.text or "").strip()))
    uid = u.effective_user.id
    cl = c.user_data.get('client')
    if not cl:
        await safe_reply(u.message, "Session expired. /start")
        return ConversationHandler.END
    await safe_reply(u.message, "Verifying...")
    try:
        if not cl.is_connected():
            await cl.connect()
        try:
            await cl.sign_in(c.user_data['phone'], code, phone_code_hash=c.user_data['hash'])
        except SessionPasswordNeededError:
            await safe_reply(u.message, "2FA Enabled\n\nSend your cloud password:")
            return PASSWORD
        ss = cl.session.save()
        await db.set_auth(uid, ss)
        await db.set_phone(uid, c.user_data['phone'])
        await cl.disconnect()
        c.user_data.clear()
        await safe_reply(u.message, "Connected!\n\nUse /start to continue.")
        # Send session as plain text to avoid any parse issues
        await u.message.reply_text(f"Session backup:\n{ss}\n\nKeep this private!", parse_mode=None)
        return ConversationHandler.END
    except Exception as e:
        await safe_reply(u.message, f"Error: {_err(e)}\n\n/start to retry.")
        try:
            if cl.is_connected():
                await cl.disconnect()
        except:
            pass
        c.user_data.clear()
        return ConversationHandler.END


async def h_pw(u, c):
    uid = u.effective_user.id
    cl = c.user_data.get('client')
    if not cl:
        return ConversationHandler.END
    try:
        if not cl.is_connected():
            await cl.connect()
        await cl.sign_in(password=u.message.text)
        ss = cl.session.save()
        await db.set_auth(uid, ss)
        await db.set_phone(uid, c.user_data.get('phone'))
        await cl.disconnect()
        c.user_data.clear()
        await safe_reply(u.message, "Connected!\n\n/start")
        await u.message.reply_text(f"Session: {ss}\n\nKeep private!", parse_mode=None)
        return ConversationHandler.END
    except Exception as e:
        await safe_reply(u.message, f"Error: {_err(e)}\n/start")
        c.user_data.clear()
        return ConversationHandler.END


async def hg_phone(u, c):
    p = (u.message.text or "").strip()
    if not p:
        await safe_reply(u.message, "Please send your phone number.")
        return GEN_PHONE
    await safe_reply(u.message, "Creating session...")
    try:
        cl = TelegramClient(StringSession(), Config.API_ID, Config.API_HASH)
        await cl.connect()
        r = await cl.send_code_request(p)
        c.user_data.update({'gp': p, 'gh': r.phone_code_hash, 'gc': cl})
        await safe_reply(u.message, "Code sent! Send it now:")
        return GEN_CODE
    except Exception as e:
        await safe_reply(u.message, f"Error: {_err(e)}\n/start")
        return ConversationHandler.END


async def hg_code(u, c):
    code = ''.join(filter(str.isdigit, (u.message.text or "").strip()))
    uid = u.effective_user.id
    cl = c.user_data.get('gc')
    if not cl:
        return ConversationHandler.END
    await safe_reply(u.message, "Generating...")
    try:
        if not cl.is_connected():
            await cl.connect()
        try:
            await cl.sign_in(c.user_data['gp'], code, phone_code_hash=c.user_data['gh'])
        except SessionPasswordNeededError:
            await safe_reply(u.message, "Send cloud password:")
            return GEN_PASSWORD
        ss = cl.session.save()
        me = await cl.get_me()
        await cl.disconnect()
        await db.set_auth(uid, ss)
        await db.set_phone(uid, c.user_data['gp'])
        c.user_data.clear()
        await safe_reply(u.message, f"{me.first_name} connected!\n/start")
        await u.message.reply_text(f"Session: {ss}\n\nKeep private!", parse_mode=None)
        return ConversationHandler.END
    except Exception as e:
        await safe_reply(u.message, f"Error: {_err(e)}\n/start")
        c.user_data.clear()
        return ConversationHandler.END


async def hg_pw(u, c):
    uid = u.effective_user.id
    cl = c.user_data.get('gc')
    if not cl:
        return ConversationHandler.END
    try:
        if not cl.is_connected():
            await cl.connect()
        await cl.sign_in(password=u.message.text)
        ss = cl.session.save()
        me = await cl.get_me()
        await cl.disconnect()
        await db.set_auth(uid, ss)
        await db.set_phone(uid, c.user_data.get('gp'))
        c.user_data.clear()
        await safe_reply(u.message, f"{me.first_name} connected!\n/start")
        await u.message.reply_text(f"Session: {ss}\n\nKeep private!", parse_mode=None)
        return ConversationHandler.END
    except Exception as e:
        await safe_reply(u.message, f"Error: {_err(e)}\n/start")
        c.user_data.clear()
        return ConversationHandler.END


# ═══════════════════════════════════════════
#  ADVANCED POST (OWNER)
# ═══════════════════════════════════════════
async def ps(u, c):
    if u.effective_user.id != Config.OWNER_ID:
        return ConversationHandler.END
    await safe_reply(u.message, "Channel ID or @username.\n/cancel")
    return P_CH


async def pc(u, c):
    c.user_data['pc'] = (u.message.text or "").strip()
    await safe_reply(u.message, "Photo/video or /skip")
    return P_MEDIA


async def pm(u, c):
    if u.message.photo:
        c.user_data['pt'] = 'photo'
        c.user_data['pf'] = u.message.photo[-1].file_id
    elif u.message.video:
        c.user_data['pt'] = 'video'
        c.user_data['pf'] = u.message.video.file_id
    elif u.message.text and u.message.text.lower() == '/skip':
        c.user_data['pt'] = None
        c.user_data['pf'] = None
    else:
        await safe_reply(u.message, "Photo/video or /skip")
        return P_MEDIA
    await safe_reply(u.message, "Caption or /skip")
    return P_CAP


async def pcap(u, c):
    t = (u.message.text or "").strip()
    c.user_data['ptxt'] = None if t.lower() == '/skip' else t
    await safe_reply(u.message, "Buttons: Text - URL (one per line) or /skip")
    return P_BTN


async def pbtn(u, c):
    t = (u.message.text or "").strip()
    btns = []
    if t.lower() != '/skip':
        btns = parse_btns(t)
        if not btns:
            await safe_reply(u.message, "Use: Text - URL or /skip")
            return P_BTN
    c.user_data['pb'] = btns
    if not c.user_data.get('pf'):
        return await pprev(u, c)
    kb = [[InlineKeyboardButton("Above", callback_data="pos_a"),
           InlineKeyboardButton("Below", callback_data="pos_b")]]
    await safe_reply(u.message, "Caption position?", reply_markup=InlineKeyboardMarkup(kb))
    return P_POS


async def ppos(u, c):
    q = u.callback_query
    await q.answer()
    c.user_data['pabove'] = q.data == "pos_a"
    return await pprev(q, c)


async def pprev(obj, c):
    ch = c.user_data.get('pc')
    txt = c.user_data.get('ptxt', '-')
    mt = c.user_data.get('pt', 'text')
    bc = len(c.user_data.get('pb', []))
    p = (f"Post Preview\n\n"
         f"  Channel: {ch}\n  Media: {mt}\n"
         f"  Text: {(txt or '-')[:80]}\n  Buttons: {bc}")
    kb = [[InlineKeyboardButton("Post", callback_data="p_now"),
           InlineKeyboardButton("Cancel", callback_data="p_no")]]
    if hasattr(obj, 'edit_message_text'):
        await safe_edit_text(obj, p, reply_markup=InlineKeyboardMarkup(kb))
    else:
        await safe_reply(obj.message, p, reply_markup=InlineKeyboardMarkup(kb))
    return P_CONFIRM


async def pconf(u, c):
    q = u.callback_query
    await q.answer()
    if q.data == "p_no":
        await safe_edit_text(q, "Cancelled.")
        return ConversationHandler.END
    await safe_edit_text(q, "Posting...")
    ch = c.user_data.get('pc')
    mt = c.user_data.get('pt')
    mf = c.user_data.get('pf')
    txt = c.user_data.get('ptxt')
    b = c.user_data.get('pb', [])
    ab = c.user_data.get('pabove', False)
    mk = ptb_kb(b)
    try:
        if mt == 'photo':
            await c.bot.send_photo(chat_id=ch, photo=mf, caption=txt,
                                   reply_markup=mk, show_caption_above_media=ab,
                                   parse_mode=None)
        elif mt == 'video':
            await c.bot.send_video(chat_id=ch, video=mf, caption=txt,
                                   reply_markup=mk, show_caption_above_media=ab,
                                   parse_mode=None)
        else:
            await c.bot.send_message(chat_id=ch, text=txt or "Post",
                                     reply_markup=mk, parse_mode=None)
        await safe_edit_text(q, f"Posted to {ch}!")
    except Exception as e:
        await safe_edit_text(q, f"Error: {_err(e, 200)}")
    return ConversationHandler.END


# ═══════════════════════════════════════════
#  STARTUP
# ═══════════════════════════════════════════
async def resume(app):
    print("Resuming schedules...")
    try:
        ss = await db.active_scheds()
    except Exception as e:
        print(f"  Resume error: {e}")
        return
    for s in ss:
        try:
            uid = s["user_id"]
            iv = _int(s.get("interval"), 0, lo=1)
            if iv <= 0:
                await db.del_sched(uid)
                continue
            ctx = BgContext(app)
            ctx.user_data['bc_content'] = s.get("content_data") or {}
            ctx.user_data['bc_backup'] = s.get("backup")
            t = asyncio.create_task(sched_loop(
                uid, iv, ctx, s.get("topic"),
                s.get("mode", "copy"), s.get("buttons"), s.get("link")))
            scheds.setdefault(uid, []).append(t)
            print(f"  Resumed uid={uid} every {iv}m")
        except Exception as e:
            print(f"  Resume error: {e}")

    try:
        us = await db.all_users()
        for u in us:
            if u.get("authorized"):
                AUTH[u["user_id"]] = True
        AUTH[Config.OWNER_ID] = True
        print(f"  Auth cache: {len(AUTH)} users")
    except Exception as e:
        print(f"  Auth cache error: {e}")
        AUTH[Config.OWNER_ID] = True


async def post_init(app):
    await db.init()
    asyncio.create_task(resume(app))


async def on_err(update, context):
    e = str(context.error)
    el = e.lower()
    # Suppress common non-critical errors
    if any(x in el for x in (
        "httpx", "network", "timed out", "not modified",
        "can't parse entities", "can't find end of the entity",
        "query is too old", "message is not modified"
    )):
        print(f"[SUPPRESSED] {e[:100]}")
        return
    print(f"Error: {context.error}")
    try:
        await context.bot.send_message(
            Config.OWNER_ID,
            f"Bot error: {e[:200]}",
            parse_mode=None)
    except:
        pass


# ═══════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════
def main():
    print("=" * 50)
    print("  ADS Pro Broadcast Bot - Production")
    print(f"  Forum API: {'Yes' if HAS_FORUM else 'No'}")
    print(f"  Forward API: {'Yes' if HAS_FWD_REQ else 'No'}")
    print(f"  Database: MongoDB")
    print("=" * 50)

    Config.validate()

    # NO default parse_mode — we handle it per-call
    app = (Application.builder()
           .token(Config.BOT_TOKEN)
           .connect_timeout(30)
           .read_timeout(30)
           .post_init(post_init)
           .build())

    auth = ConversationHandler(
        entry_points=[CallbackQueryHandler(on_cb, pattern="^(auth_otp|auth_gen|auth_paste)$")],
        states={
            PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_phone)],
            CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_code)],
            PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, h_pw)],
            GEN_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, hg_phone)],
            GEN_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, hg_code)],
            GEN_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, hg_pw)],
        },
        fallbacks=[CommandHandler("cancel", cancel_cmd)],
        allow_reentry=True, per_message=False)

    post = ConversationHandler(
        entry_points=[CommandHandler("post", ps)],
        states={
            P_CH: [MessageHandler(filters.TEXT & ~filters.COMMAND, pc)],
            P_MEDIA: [MessageHandler((filters.PHOTO | filters.VIDEO | filters.TEXT) & ~filters.COMMAND, pm)],
            P_CAP: [MessageHandler(filters.TEXT & ~filters.COMMAND, pcap)],
            P_BTN: [MessageHandler(filters.TEXT & ~filters.COMMAND, pbtn)],
            P_POS: [CallbackQueryHandler(ppos, pattern="^pos_")],
            P_CONFIRM: [CallbackQueryHandler(pconf, pattern="^p_")],
        },
        fallbacks=[CommandHandler("cancel", cancel_cmd)],
        allow_reentry=True, per_message=False)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel_cmd))

    async def apr(u, c):
        if u.effective_user.id != Config.OWNER_ID:
            return
        if not c.args:
            await safe_reply(u.message, "/approve <id>")
            return
        tid = int(c.args[0])
        await db.authorize(tid, True)
        AUTH[tid] = True
        await safe_reply(u.message, f"Approved {tid}")
        try:
            await safe_send(c.bot, tid, "Access approved!\n\nUse /start")
        except:
            pass

    async def den(u, c):
        if u.effective_user.id != Config.OWNER_ID:
            return
        if not c.args:
            return
        tid = int(c.args[0])
        await db.authorize(tid, False)
        AUTH[tid] = False
        await safe_reply(u.message, f"Denied {tid}")

    app.add_handler(CommandHandler("approve", apr))
    app.add_handler(CommandHandler("deny", den))
    app.add_handler(post)
    app.add_error_handler(on_err)
    app.add_handler(CallbackQueryHandler(on_cb, pattern="^mode_"))
    app.add_handler(auth)
    app.add_handler(CallbackQueryHandler(confirm_bc, pattern="^(confirm|cancel)_bc$"))
    app.add_handler(CallbackQueryHandler(on_cb, pattern="^export$"))
    app.add_handler(CallbackQueryHandler(on_cb, pattern="^(?!auth_).*$"))

    async def stop_cmd(u, c):
        uid = u.effective_user.id
        cancel(uid, True)
        if uid in scheds:
            for t in scheds[uid]:
                if not t.done():
                    t.cancel()
            scheds[uid] = []
        await db.del_sched(uid)
        tasks[uid] = 0
        await safe_reply(u.message, "All broadcasts & schedules stopped.")

    async def fix_cmd(u, c):
        uid = u.effective_user.id
        cancel(uid, True)
        tasks[uid] = 0
        if uid in scheds:
            for t in scheds[uid]:
                if not t.done():
                    t.cancel()
            scheds[uid] = []
        await db.del_sched(uid)
        c.user_data.clear()
        await safe_reply(u.message, "State reset.\n\nUse /start to continue.")

    async def sys_cmd(u, c):
        if u.effective_user.id != Config.OWNER_ID:
            return
        s = sum(sum(1 for t in ts if not t.done()) for ts in scheds.values())
        await safe_reply(u.message,
                         f"System Info\n\n"
                         f"  Forum API: {'Yes' if HAS_FORUM else 'No'}\n"
                         f"  Forward API: {'Yes' if HAS_FWD_REQ else 'No'}\n"
                         f"  Database: MongoDB\n"
                         f"  Active tasks: {sum(tasks.values())}\n"
                         f"  Schedule loops: {s}\n"
                         f"  Auth cache: {len(AUTH)} users")

    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("fix", fix_cmd))
    app.add_handler(CommandHandler("sysinfo", sys_cmd))
    app.add_handler(MessageHandler(
        filters.TEXT | filters.PHOTO | filters.VIDEO |
        filters.Document.ALL | filters.AUDIO | filters.ANIMATION,
        on_msg))

    print("Bot running...")
    app.run_polling()


if __name__ == "__main__":
    main()