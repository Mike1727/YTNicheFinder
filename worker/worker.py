#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
YouTube Niche Finder - воркер.

Конвейер:
  1. discover  - ищем свежие видео с большими просмотрами (search.list, order=viewCount),
                 берём каналы моложе MAX_CHANNEL_AGE_DAYS с малым числом подписчиков.
  2. deep dive - смотрим последние видео канала-кандидата.
  3. describe  - определяем нишу и поисковые запросы (LLM, если задан ключ; иначе эвристика).
  4. validate  - по запросам собираем видео за год, считаем каналы, подписчиков, просмотры.
  5. verdict   - pass / weak / fail + score; результат пишется в public/results.json.

Все счётчики квоты YouTube хранятся в data.db, воркер сам останавливается на бюджете.
"""
import argparse
import json
import math
import os
import random
import re
import sqlite3
import statistics
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

try:
    from zoneinfo import ZoneInfo
    PT = ZoneInfo("America/Los_Angeles")  # квота YouTube сбрасывается по тихоокеанскому времени
except Exception:  # на Windows без tzdata
    PT = timezone(timedelta(hours=-8))

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
OUT_PATH = ROOT / "public" / "results.json"
DB_PATH = HERE / "data.db"
LOG_PATH = HERE / "run.log"


# --------------------------------------------------------------------------- config
def load_env():
    p = HERE / ".env"
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        v = v.strip().strip('"').strip("'")
        if v:  # пустые значения не перекрывают значения по умолчанию
            os.environ.setdefault(k.strip(), v)


load_env()


def ei(name, default):
    try:
        return int(os.getenv(name, default))
    except ValueError:
        return default


API_KEY = os.getenv("YT_API_KEY", "")
LANG = os.getenv("RELEVANCE_LANGUAGE", "en")
REGION = os.getenv("REGION_CODE", "US")

# поиск кандидатов
MAX_CHANNEL_AGE_DAYS = ei("MAX_CHANNEL_AGE_DAYS", 60)
MAX_SUBS = ei("MAX_SUBS", 10_000)
MIN_OUTLIER_VIEWS = ei("MIN_OUTLIER_VIEWS", 50_000)
MIN_VIEWS_PER_SUB = ei("MIN_VIEWS_PER_SUB", 10)
MIN_DURATION = ei("MIN_DURATION_SEC", 181)  # отсекаем Shorts
DURATIONS = ["medium", "long"]  # 4-20 мин и 20+ мин (чередуются по запросам)

# проверка ниши
VALIDATION_QUERIES = ei("VALIDATION_QUERIES", 3)
MAX_CHANNELS = ei("MAX_CHANNELS", 30)
HARD_MAX_SUBS = ei("HARD_MAX_SUBS", 1_000_000)  # канал больше этого = ниша занята
BIG_SUBS = ei("BIG_SUBS", 100_000)
MAX_MEDIAN_SUBS = ei("MAX_MEDIAN_SUBS", 50_000)
MIN_TOP_VIEWS = ei("MIN_TOP_VIEWS", 100_000)
MIN_DEMAND_VIEWS = ei("MIN_DEMAND_VIEWS", 50_000)

# квота
DAILY_BUDGET = ei("DAILY_BUDGET", 9000)
DISCOVERY_SEARCHES = ei("DISCOVERY_SEARCHES", 20)
PAGES_PER_SEARCH = ei("PAGES_PER_SEARCH", 1)
MAX_VALIDATIONS = ei("MAX_VALIDATIONS", 15)
COST_SEARCH = 100
COST_VALIDATION = 4 + VALIDATION_QUERIES * (COST_SEARCH + 2)

# LLM (необязательно): любой OpenAI-совместимый endpoint
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "")
LLM_MODEL = os.getenv("LLM_MODEL", "")
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_ENABLED = bool(LLM_BASE_URL and LLM_MODEL and LLM_API_KEY)


# --------------------------------------------------------------------------- utils
def log(*a):
    line = datetime.now().strftime("%H:%M:%S ") + " ".join(str(x) for x in a)
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


def now():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_dt(s):
    s = re.sub(r"\.\d+", "", s or "").replace("Z", "+00:00")
    return datetime.fromisoformat(s)


def age_days(created):
    try:
        return max(0, (now() - parse_dt(created)).days)
    except Exception:
        return 99999


_DUR = re.compile(r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$")


def parse_duration(s):
    m = _DUR.match(s or "")
    if not m:
        return 0
    d, h, mi, sec = (int(x or 0) for x in m.groups())
    return d * 86400 + h * 3600 + mi * 60 + sec


STOP = set(
    "a an the of and or to in on for with from by at is are was were be been this that these those "
    "how why what who when where which into about as it its your you my our their his her they we i "
    "not no vs versus full documentary explained story official video new best top part episode "
    "will can all just more most than then them there here very".split()
)


def tokens(text):
    return [w for w in re.findall(r"\w+", (text or "").lower())
            if len(w) >= 3 and w not in STOP and not w.isdigit()]


def title_query(title, n=4):
    return " ".join(tokens(title)[:n])


def on_topic(video, query):
    """Видео относится к нише, если в заголовке/тегах есть минимум половина слов запроса."""
    q = tokens(query)
    if not q:
        return False
    vs = {w[:5] for w in tokens(video["title"] + " " + " ".join(video["tags"]))}
    hit = sum(1 for w in q if w[:5] in vs)
    return hit >= max(1, math.ceil(len(q) / 2))


# --------------------------------------------------------------------------- db + quota
SCHEMA = """
CREATE TABLE IF NOT EXISTS quota(day TEXT PRIMARY KEY, used INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS seed_log(key TEXT PRIMARY KEY, last_used TEXT);
CREATE TABLE IF NOT EXISTS extra_seeds(query TEXT PRIMARY KEY, source TEXT, added_at TEXT);
CREATE TABLE IF NOT EXISTS candidates(
    channel_id TEXT PRIMARY KEY, found_at TEXT, data TEXT, validated_at TEXT, result TEXT);
"""


def db_connect():
    db = sqlite3.connect(DB_PATH)
    db.executescript(SCHEMA)
    return db


class BudgetReached(Exception):
    pass


class Quota:
    def __init__(self, db, budget):
        self.db, self.budget = db, budget
        self.day = datetime.now(PT).strftime("%Y-%m-%d")
        row = db.execute("SELECT used FROM quota WHERE day=?", (self.day,)).fetchone()
        self.used = row[0] if row else 0

    def can(self, cost):
        return self.used + cost <= self.budget

    def add(self, cost):
        self.used += cost
        self.db.execute(
            "INSERT INTO quota(day, used) VALUES(?, ?) ON CONFLICT(day) DO UPDATE SET used=excluded.used",
            (self.day, self.used))
        self.db.commit()

    def exhaust(self):
        self.add(max(0, self.budget - self.used))


# --------------------------------------------------------------------------- youtube api
def norm_video(it):
    sn, st, cd = it.get("snippet", {}), it.get("statistics", {}), it.get("contentDetails", {})
    return {
        "id": it["id"],
        "title": sn.get("title", ""),
        "channel_id": sn.get("channelId", ""),
        "channel_title": sn.get("channelTitle", ""),
        "published": sn.get("publishedAt", ""),
        "views": int(st.get("viewCount", 0) or 0),
        "duration": parse_duration(cd.get("duration", "")),
        "tags": sn.get("tags", []) or [],
        "desc": (sn.get("description", "") or "")[:300],
        "live": sn.get("liveBroadcastContent", "none"),
    }


def norm_channel(it):
    sn, st = it.get("snippet", {}), it.get("statistics", {})
    hidden = bool(st.get("hiddenSubscriberCount"))
    return {
        "id": it["id"],
        "title": sn.get("title", ""),
        "created": sn.get("publishedAt", ""),
        "subs": None if hidden else int(st.get("subscriberCount", 0) or 0),
        "video_count": int(st.get("videoCount", 0) or 0),
        "uploads": (it.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
                    or "UU" + it["id"][2:]),
    }


class YT:
    BASE = "https://www.googleapis.com/youtube/v3/"

    def __init__(self, key, quota):
        self.key, self.quota = key, quota

    def _get(self, endpoint, params, cost):
        if not self.quota.can(cost):
            raise BudgetReached(f"дневной бюджет {self.quota.budget} units")
        params = {**params, "key": self.key}
        last = ""
        for attempt in range(3):
            try:
                r = requests.get(self.BASE + endpoint, params=params, timeout=30)
            except requests.RequestException as e:
                last = str(e)
                time.sleep(2 * (attempt + 1))
                continue
            if r.status_code == 200:
                self.quota.add(cost)
                return r.json()
            if r.status_code in (500, 502, 503, 504):
                last = f"HTTP {r.status_code}"
                time.sleep(2 * (attempt + 1))
                continue
            try:
                reason = r.json()["error"]["errors"][0]["reason"]
            except Exception:
                reason = ""
            if r.status_code == 403 and reason in ("quotaExceeded", "dailyLimitExceeded", "rateLimitExceeded"):
                self.quota.exhaust()
                raise BudgetReached("YouTube сообщил, что квота исчерпана")
            raise RuntimeError(f"YouTube API {r.status_code} {reason}: {r.text[:300]}")
        raise RuntimeError(f"YouTube API недоступен: {last}")

    def search(self, q, order, published_after, duration="any", page_token=None):
        p = {"part": "snippet", "type": "video", "q": q, "order": order, "maxResults": 50,
             "publishedAfter": published_after, "relevanceLanguage": LANG, "regionCode": REGION}
        if duration != "any":
            p["videoDuration"] = duration
        if page_token:
            p["pageToken"] = page_token
        return self._get("search", p, COST_SEARCH)

    def videos(self, ids):
        ids = list(dict.fromkeys(ids))
        out = []
        for i in range(0, len(ids), 50):
            res = self._get("videos", {"part": "snippet,statistics,contentDetails",
                                       "id": ",".join(ids[i:i + 50])}, 1)
            out += [norm_video(x) for x in res.get("items", [])]
        return out

    def channels(self, ids):
        ids = list(dict.fromkeys(ids))
        out = []
        for i in range(0, len(ids), 50):
            res = self._get("channels", {"part": "snippet,statistics,contentDetails",
                                         "id": ",".join(ids[i:i + 50])}, 1)
            out += [norm_channel(x) for x in res.get("items", [])]
        return out

    def playlist_items(self, playlist_id):
        return self._get("playlistItems", {"part": "contentDetails", "playlistId": playlist_id,
                                           "maxResults": 50}, 1)


def video_ids(search_result):
    return [it["id"]["videoId"] for it in search_result.get("items", [])
            if it.get("id", {}).get("videoId")]


# --------------------------------------------------------------------------- llm
def llm_json(prompt):
    if not LLM_ENABLED:
        return None
    try:
        r = requests.post(
            LLM_BASE_URL.rstrip("/") + "/chat/completions",
            headers={"Authorization": f"Bearer {LLM_API_KEY}"},
            json={"model": LLM_MODEL, "temperature": 0.2,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=90)
        r.raise_for_status()
        text = r.json()["choices"][0]["message"]["content"]
        return json.loads(text[text.find("{"): text.rfind("}") + 1])
    except Exception as e:
        log("   LLM не ответил, использую эвристику:", str(e)[:160])
        return None


def clean_list(x, n):
    if not isinstance(x, list):
        return []
    out = []
    for s in x:
        s = str(s).strip()
        if s and s not in out:
            out.append(s)
    return out[:n]


# --------------------------------------------------------------------------- stage 1: discovery
def read_seeds(db):
    seeds = []
    p = HERE / "seeds.txt"
    if p.exists():
        for line in p.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                seeds.append(line)
    seeds += [r[0] for r in db.execute("SELECT query FROM extra_seeds")]
    return list(dict.fromkeys(seeds))


def pick_seed_keys(db, n):
    keys = [f"{q}|{d}" for q in read_seeds(db) for d in DURATIONS]
    last = dict(db.execute("SELECT key, last_used FROM seed_log").fetchall())
    random.shuffle(keys)
    keys.sort(key=lambda k: last.get(k) or "")  # сначала то, что ещё не запускали
    return keys[:n]


def register_candidates(yt, db, ids, seed):
    if not ids:
        return 0
    vids = [v for v in yt.videos(ids) if v["duration"] >= MIN_DURATION and v["live"] == "none"]
    by_ch = {}
    for v in vids:
        by_ch.setdefault(v["channel_id"], []).append(v)
    known = {r[0] for r in db.execute("SELECT channel_id FROM candidates")}
    todo = [c for c in by_ch if c not in known]
    new = 0
    for ch in yt.channels(todo):
        if ch["subs"] is None:  # скрытые подписчики
            continue
        age = age_days(ch["created"])
        if age > MAX_CHANNEL_AGE_DAYS or ch["subs"] > MAX_SUBS:
            continue
        best = max(by_ch[ch["id"]], key=lambda v: v["views"])
        ratio = best["views"] / max(ch["subs"], 100)
        if best["views"] < MIN_OUTLIER_VIEWS or ratio < MIN_VIEWS_PER_SUB:
            continue
        data = {"channel": ch, "seed": seed, "best": best, "ratio": round(ratio, 1), "age_days": age}
        db.execute("INSERT OR IGNORE INTO candidates(channel_id, found_at, data) VALUES(?,?,?)",
                   (ch["id"], iso(now()), json.dumps(data, ensure_ascii=False)))
        new += 1
        log(f"   + кандидат: {ch['title']} | {ch['subs']} подп., {age} дн., "
            f"{best['views']} просм. (x{data['ratio']})")
    db.commit()
    return new


def discover(yt, db, n_searches):
    after = iso(now() - timedelta(days=MAX_CHANNEL_AGE_DAYS))
    total = 0
    for key in pick_seed_keys(db, n_searches):
        q, dur = key.rsplit("|", 1)
        log(f"поиск: «{q}» [{dur}]")
        ids, token = [], None
        for _ in range(PAGES_PER_SEARCH):
            res = yt.search(q, "viewCount", after, dur, token)
            ids += video_ids(res)
            token = res.get("nextPageToken")
            if not token:
                break
        new = register_candidates(yt, db, ids, q)
        db.execute("INSERT OR REPLACE INTO seed_log(key, last_used) VALUES(?,?)", (key, iso(now())))
        db.commit()
        log(f"   видео: {len(ids)}, новых кандидатов: {new}")
        total += new
    return total


# --------------------------------------------------------------------------- stage 2-3: niche
def deep_dive(yt, ch):
    res = yt.playlist_items(ch["uploads"])
    ids = [i["contentDetails"]["videoId"] for i in res.get("items", [])]
    vids = [v for v in yt.videos(ids) if v["duration"] >= MIN_DURATION]
    vids.sort(key=lambda v: -v["views"])
    views = [v["views"] for v in vids]
    return {
        "videos": vids,
        "n_videos": len(vids),
        "median_views": int(statistics.median(views)) if views else 0,
        "hits": sum(1 for x in views if x >= MIN_OUTLIER_VIEWS),
    }


def heuristic_queries(vids, k=3):
    tags, words = Counter(), Counter()
    for v in vids:
        for t in {x.lower().strip() for x in v["tags"]}:
            if 3 <= len(t) <= 40 and 2 <= len(t.split()) <= 4:
                tags[t] += 1
        for w in set(tokens(v["title"])):
            words[w] += 1
    qs = [t for t, c in tags.most_common(4) if c >= 2][:2]
    top_words = [w for w, _ in words.most_common(3)]
    if len(top_words) >= 2:
        qs.append(" ".join(top_words))
    for v in vids[:3]:
        if len(qs) >= k:
            break
        q = title_query(v["title"])
        if len(q.split()) >= 2 and q not in qs:
            qs.append(q)
    return qs[:k]


def describe_niche(ch, dive):
    vids = dive["videos"]
    top = vids[:5]
    llm = None
    if top:
        lines = [f"{i + 1}. {v['title']} | tags: {', '.join(v['tags'][:8])} | {v['desc'][:160]}"
                 for i, v in enumerate(top)]
        llm = llm_json(
            "You analyse YouTube channels to define their micro-niche.\n"
            f"Channel: {ch['title']}\nTop videos:\n" + "\n".join(lines) + "\n\n"
            "Return ONLY a JSON object, no other text:\n"
            '{"niche": "niche name, 3-8 words",\n'
            ' "path": ["broad topic", "niche", "sub-niche", "..."],\n'
            ' "queries": ["3 YouTube search queries of 2-5 words that a viewer of exactly this niche '
            'would type, in the language of the video titles"],\n'
            ' "sub_queries": ["5 narrower search queries for deeper sub-niches"]}')
    queries, niche, path, subq = [], "", [], []
    used_llm = False
    if llm:
        queries = clean_list(llm.get("queries"), 5)
        niche = str(llm.get("niche") or "").strip()[:120]
        path = clean_list(llm.get("path"), 8)
        subq = clean_list(llm.get("sub_queries"), 8)
        used_llm = len(queries) >= 2
    if len(queries) < 2:
        queries = heuristic_queries(vids)
        used_llm = False
    if not niche:
        niche = top[0]["title"] if top else ch["title"]
    return {"niche": niche, "path": path, "queries": queries, "sub_queries": subq, "llm": used_llm}


# --------------------------------------------------------------------------- stage 4-5: validation
def verdict(m):
    reasons, notes = [], []
    if m["n_channels"] == 0:
        return "fail", ["По запросам не нашлось подходящих видео за год: запросы не описывают нишу"], []
    if m["n_channels"] > MAX_CHANNELS:
        reasons.append(f"Каналов в нише {m['n_channels']}, лимит {MAX_CHANNELS}")
    if m["max_subs"] >= HARD_MAX_SUBS:
        reasons.append(f"Есть канал с {m['max_subs']:,} подписчиков".replace(",", " "))
    if m["median_subs"] > MAX_MEDIAN_SUBS:
        reasons.append(f"Медиана подписчиков {m['median_subs']:,} выше лимита".replace(",", " "))
    if m["top_views"] < MIN_TOP_VIEWS:
        reasons.append(f"Лучшее видео ниши набрало {m['top_views']:,}, нужно от {MIN_TOP_VIEWS:,}".replace(",", " "))
    if reasons:
        return "fail", reasons, notes
    if m["big_channels"]:
        notes.append(f"Есть каналов от {BIG_SUBS:,} подписчиков: {m['big_channels']}".replace(",", " "))
    if m["demand_channels"] >= 2 or m["demand_videos"] >= 3:
        return "pass", reasons, notes
    notes.append("Спрос подтверждён одним каналом и одним видео: это может быть разовый хит")
    return "weak", reasons, notes


def opportunity_score(m, top3):
    if not top3:
        return 0
    demand = sum(min(x, 1_000_000) for x in top3) / len(top3)
    comp = max(m["n_channels"], 1) * (1 + m["median_subs"] / 5000) * (1 + 2 * m["big_channels"])
    return int(demand / comp)


def validate(yt, queries):
    after = iso(now() - timedelta(days=365))
    topic = {}
    for q in queries[:VALIDATION_QUERIES]:
        res = yt.search(q, "relevance", after, "any")
        for v in yt.videos(video_ids(res)):
            if v["duration"] < MIN_DURATION or v["live"] != "none":
                continue
            if on_topic(v, q):
                topic[v["id"]] = v
    by_ch = {}
    for v in topic.values():
        by_ch.setdefault(v["channel_id"], []).append(v)
    chmap = {c["id"]: c for c in yt.channels(list(by_ch))} if by_ch else {}

    rows = []
    for cid, vs in by_ch.items():
        c = chmap.get(cid)
        best = max(vs, key=lambda v: v["views"])
        rows.append({
            "id": cid,
            "title": c["title"] if c else vs[0]["channel_title"],
            "subs": c["subs"] if c else None,
            "age_days": age_days(c["created"]) if c else None,
            "videos": len(vs),
            "best_views": best["views"],
            "best_title": best["title"],
            "best_id": best["id"],
        })
    rows.sort(key=lambda r: -r["best_views"])

    subs = [r["subs"] for r in rows if r["subs"] is not None]
    views = sorted((v["views"] for v in topic.values()), reverse=True)
    m = {
        "n_videos": len(topic),
        "n_channels": len(rows),
        "max_subs": max(subs, default=0),
        "median_subs": int(statistics.median(subs)) if subs else 0,
        "big_channels": sum(1 for s in subs if s >= BIG_SUBS),
        "top_views": views[0] if views else 0,
        "demand_videos": sum(1 for x in views if x >= MIN_DEMAND_VIEWS),
        "demand_channels": sum(1 for r in rows if r["best_views"] >= MIN_DEMAND_VIEWS),
        "young_hits": sum(1 for r in rows if r["age_days"] is not None and r["age_days"] <= 120
                          and r["best_views"] >= MIN_DEMAND_VIEWS),
    }
    status, reasons, notes = verdict(m)
    return {"market": m, "status": status, "reasons": reasons, "notes": notes,
            "score": opportunity_score(m, views[:3]), "competitors": rows[:8],
            "niche_channel_ids": list(by_ch)}


def load_covered(db):
    cov = {}
    for cid, res in db.execute("SELECT channel_id, result FROM candidates WHERE result IS NOT NULL"):
        r = json.loads(res)
        if r.get("status") in ("pass", "weak", "fail"):
            for c in r.get("niche_channel_ids", []):
                cov.setdefault(c, cid)
    return cov


def validate_pending(yt, db, quota, limit):
    rows = db.execute("SELECT channel_id, data, found_at FROM candidates WHERE result IS NULL").fetchall()
    cands = []
    for cid, d, found_at in rows:
        data = json.loads(d)
        data["found_at"] = found_at
        cands.append((cid, data))
    cands.sort(key=lambda x: -x[1]["ratio"])
    covered = load_covered(db)
    done = 0
    for cid, data in cands:
        if done >= limit:
            break
        if cid in covered:
            db.execute("UPDATE candidates SET validated_at=?, result=? WHERE channel_id=?",
                       (iso(now()), json.dumps({"status": "dup", "merged_into": covered[cid]}), cid))
            db.commit()
            log(f"{data['channel']['title']}: уже входит в проверенную нишу, пропускаю")
            continue
        if not quota.can(COST_VALIDATION):
            log(f"на проверку нужно ~{COST_VALIDATION} units, а бюджет почти исчерпан: остановка")
            break
        ch = data["channel"]
        log(f"проверка ниши канала: {ch['title']}")
        try:
            dive = deep_dive(yt, ch)
            desc = describe_niche(ch, dive)
            log(f"   ниша: {desc['niche']} | запросы: {desc['queries']} | LLM: {desc['llm']}")
            val = validate(yt, desc["queries"])
        except BudgetReached:
            raise
        except Exception as e:
            log("   ошибка, пропускаю канал:", str(e)[:200])
            continue
        best = data["best"]
        result = {
            "channel_id": ch["id"],
            "channel_title": ch["title"],
            "channel_created": (ch["created"] or "")[:10],
            "channel_age_days": data["age_days"],
            "subs": ch["subs"],
            "found_at": data.get("found_at") or iso(now()),
            "validated_at": iso(now()),
            "niche": desc["niche"],
            "path": desc["path"],
            "queries": desc["queries"][:VALIDATION_QUERIES],
            "sub_queries": desc["sub_queries"],
            "llm": desc["llm"],
            "best_video": {"id": best["id"], "title": best["title"], "views": best["views"]},
            "ratio": data["ratio"],
            "channel_stats": {"videos": dive["n_videos"], "median_views": dive["median_views"],
                              "hits": dive["hits"]},
            **val,
        }
        db.execute("UPDATE candidates SET validated_at=?, result=? WHERE channel_id=?",
                   (result["validated_at"], json.dumps(result, ensure_ascii=False), cid))
        if result["status"] in ("pass", "weak"):
            for q in desc["sub_queries"][:5]:
                db.execute("INSERT OR IGNORE INTO extra_seeds(query, source, added_at) VALUES(?,?,?)",
                           (q, cid, iso(now())))
        db.commit()
        for c in val["niche_channel_ids"]:
            covered.setdefault(c, cid)
        done += 1
        log(f"   итог: {result['status']} | score {result['score']} | "
            f"каналов {val['market']['n_channels']} | топ {val['market']['top_views']}")
    return done


# --------------------------------------------------------------------------- export
ORDER = {"pass": 0, "weak": 1, "fail": 2}


def export(db, quota):
    niches, pending = [], 0
    for _cid, res in db.execute("SELECT channel_id, result FROM candidates"):
        if res is None:
            pending += 1
            continue
        r = json.loads(res)
        if r.get("status") not in ORDER:
            continue
        r.pop("niche_channel_ids", None)
        niches.append(r)
    niches.sort(key=lambda r: (ORDER[r["status"]], -r.get("score", 0)))
    payload = {
        "generated_at": iso(now()),
        "quota": {"day": quota.day, "used": quota.used, "budget": quota.budget},
        "pending": pending,
        "thresholds": {
            "max_channel_age_days": MAX_CHANNEL_AGE_DAYS, "max_subs": MAX_SUBS,
            "min_outlier_views": MIN_OUTLIER_VIEWS, "max_channels": MAX_CHANNELS,
            "hard_max_subs": HARD_MAX_SUBS, "min_top_views": MIN_TOP_VIEWS,
        },
        "niches": niches,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, OUT_PATH)
    log(f"results.json: ниш {len(niches)}, в очереди на проверку {pending}")


# --------------------------------------------------------------------------- main
def main():
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            pass
    ap = argparse.ArgumentParser(description="YouTube Niche Finder")
    ap.add_argument("--searches", type=int, default=DISCOVERY_SEARCHES, help="сколько поисков на этапе discovery")
    ap.add_argument("--validate", type=int, default=MAX_VALIDATIONS, help="сколько кандидатов проверить")
    ap.add_argument("--budget", type=int, default=DAILY_BUDGET, help="бюджет квоты на сутки (units)")
    ap.add_argument("--no-discovery", action="store_true", help="только проверка уже найденных кандидатов")
    ap.add_argument("--export-only", action="store_true", help="только пересобрать results.json")
    args = ap.parse_args()

    if not API_KEY and not args.export_only:
        sys.exit("Не задан YT_API_KEY: впиши ключ в worker/.env")

    db = db_connect()
    quota = Quota(db, args.budget)
    yt = YT(API_KEY, quota)
    log(f"=== запуск | квота за сутки: {quota.used}/{quota.budget} | LLM: {'да' if LLM_ENABLED else 'нет'}")
    try:
        if not args.export_only:
            if not args.no_discovery:
                discover(yt, db, args.searches)
            validate_pending(yt, db, quota, args.validate)
    except BudgetReached as e:
        log("остановка по квоте:", e)
    export(db, quota)
    log(f"=== готово | квота за сутки: {quota.used}/{quota.budget}")


if __name__ == "__main__":
    main()
