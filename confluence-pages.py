#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import time
import requests
from urllib.parse import urljoin, urlparse, urlunparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================== CONFIG ==================
BASE_URL  = os.getenv("CONFLUENCE_BASE_URL", "https://digitalfemsa.atlassian.net/wiki/")
EMAIL     = os.getenv("ATLASSIAN_EMAIL", "enrique.galicia@spin.co")
API_TOKEN = os.getenv("ATLASSIAN_API_TOKEN", "xxxxxxxx")

CSV_PATH       = os.getenv("CSV_PATH", "confluence_restricted_pages.csv")
HTTP_TIMEOUT   = int(os.getenv("HTTP_TIMEOUT", "60"))
PAGE_LIMIT     = int(os.getenv("PAGE_LIMIT", "50"))          # v1/v2: 25–200 recomendado (v2 soporta 200)
SLEEP_SECONDS  = float(os.getenv("SLEEP_SECONDS", "0.15"))
MAX_RETRIES    = int(os.getenv("MAX_RETRIES", "4"))
BACKOFF_FACTOR = float(os.getenv("BACKOFF_FACTOR", "0.8"))
MAX_WORKERS    = int(os.getenv("MAX_WORKERS", "6"))          # <=10 para no golpear la API

# Tope duro para evitar “trabajar de más”.
# 0 = sin tope manual; por defecto tomamos el total v2 calculado.
MAX_PAGES      = int(os.getenv("MAX_PAGES", "0"))

# Filtrado opcional por espacios (ej: "HR,OPS,ENG"); si vacío, todos.
SPACE_KEYS     = [s.strip() for s in os.getenv("SPACE_KEYS", "").split(",") if s.strip()]

# Ignorar archivados y/o personales en el listado v2 de espacios
IGNORE_ARCHIVED_SPACES = os.getenv("IGNORE_ARCHIVED_SPACES", "true").lower() in ("1","true","yes","y")
SKIP_PERSONAL_SPACES   = os.getenv("SKIP_PERSONAL_SPACES", "true").lower() in ("1","true","yes","y")

# Salir en cuanto encuentre la primera página con restricción (para pruebas rápidas)
FAST_EXIT_ON_FIND = os.getenv("FAST_EXIT_ON_FIND", "false").lower() in ("1","true","yes","y")

# ================== HELPERS (URLs) ==================
def v1_url(path: str) -> str:
    base = BASE_URL if BASE_URL.endswith("/") else BASE_URL + "/"
    return urljoin(base, f"rest/api/{path.lstrip('/')}")

def v2_url(path: str) -> str:
    base = BASE_URL if BASE_URL.endswith("/") else BASE_URL + "/"
    return urljoin(base, f"api/v2/{path.lstrip('/')}")

def _origin(url: str) -> str:
    p = urlparse(url if url.endswith("/") else url + "/")
    return urlunparse((p.scheme, p.netloc, "/", "", "", ""))

_ORIGIN = _origin(BASE_URL)

def _follow_next(next_url: str | None) -> str | None:
    if not next_url:
        return None
    if next_url.startswith("http"):
        return next_url
    if next_url.startswith("/"):
        return urljoin(_ORIGIN, next_url.lstrip("/"))
    return urljoin(BASE_URL, next_url)

def page_ui_url(page_id: str) -> str:
    base = BASE_URL if BASE_URL.endswith("/") else BASE_URL + "/"
    return urljoin(base, f"pages/{page_id}")

# ================== SESSION & REQUESTS ==================
def build_session() -> requests.Session:
    s = requests.Session()
    s.auth = (EMAIL, API_TOKEN)
    s.headers.update({"Accept": "application/json"})
    retry = Retry(
        total=MAX_RETRIES,
        read=MAX_RETRIES,
        connect=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def safe_get(sess: requests.Session, url: str, **kwargs) -> requests.Response:
    attempts = MAX_RETRIES
    last_exc = None
    for i in range(1, attempts + 1):
        try:
            r = sess.get(url, timeout=HTTP_TIMEOUT, **kwargs)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", "2"))
                time.sleep(max(wait, 2))
                continue
            if r.status_code >= 500:
                time.sleep(BACKOFF_FACTOR * i)
                continue
            r.raise_for_status()
            return r
        except (requests.ReadTimeout, requests.ConnectTimeout, requests.ConnectionError) as e:
            last_exc = e
            time.sleep(BACKOFF_FACTOR * i)
            continue
    if last_exc:
        raise last_exc
    raise requests.HTTPError(f"GET failed after {attempts} attempts: {url}")

# ================== NAMES & RESTRICTIONS ==================
def join_names(users: list, groups: list) -> str:
    u = [u.get("publicName") or u.get("displayName") or u.get("username") or u.get("accountId","")
         for u in (users or [])]
    g = [g.get("name","") for g in (groups or [])]
    u = sorted({x for x in u if x})
    g = sorted({x for x in g if x})
    out = []
    if u: out.append("Users: " + ", ".join(u))
    if g: out.append("Groups: " + ", ".join(g))
    return " | ".join(out)

def get_page_restrictions(sess: requests.Session, page_id: str):
    """
    Lee restricciones intentando primero:
      GET /rest/api/content/{id}?expand=restrictions.read.restrictions.user,restrictions.read.restrictions.group,...
    Si viene vacío, hace fallback a:
      GET /rest/api/content/{id}/restriction/byOperation?expand=restrictions.user,restrictions.group
    Devuelve {'read':{'users':[],'groups':[]}, 'update':{...}} (nunca None).
    """
    # --- intento A: content?expand=restrictions... ---
    urlA = v1_url(f"content/{page_id}")
    paramsA = {
        "expand": ",".join([
            "restrictions.read.restrictions.user",
            "restrictions.read.restrictions.group",
            "restrictions.update.restrictions.user",
            "restrictions.update.restrictions.group",
        ])
    }
    try:
        r = safe_get(sess, urlA, params=paramsA)
        data = r.json()
        restrs = (data.get("restrictions") or {})
        def _blk(op):
            blk = (restrs.get(op) or {}).get("restrictions") or {}
            users = (blk.get("user") or {}).get("results", []) if blk.get("user") else []
            groups = (blk.get("group") or {}).get("results", []) if blk.get("group") else []
            return users, groups
        ru, rg = _blk("read")
        uu, ug = _blk("update")
        if ru or rg or uu or ug:
            return {"read": {"users": ru, "groups": rg},
                    "update": {"users": uu, "groups": ug}}
    except Exception as e:
        print(f"   ! Err leyendo content+expand page {page_id}: {e}")

    # --- intento B (fallback): byOperation ---
    urlB = v1_url(f"content/{page_id}/restriction/byOperation")
    paramsB = {"expand": "restrictions.user,restrictions.group"}
    out = {"read": {"users": [], "groups": []},
           "update": {"users": [], "groups": []}}
    try:
        r = safe_get(sess, urlB, params=paramsB)
        data = r.json()
        for op in data.get("results", []):
            key = (op.get("operation") or {}).get("operation")
            restr = op.get("restrictions") or {}
            users = (restr.get("user") or {}).get("results", []) if restr.get("user") else []
            groups = (restr.get("group") or {}).get("results", []) if restr.get("group") else []
            if key in out:
                out[key]["users"] = users
                out[key]["groups"] = groups
    except Exception as e:
        print(f"   ! Err leyendo byOperation page {page_id}: {e}")
    return out

def get_page_history(sess: requests.Session, page_id: str):
    """Devuelve (created_by, created_at) usando /rest/api/content/{id}?expand=history."""
    url = v1_url(f"content/{page_id}")
    try:
        r = safe_get(sess, url, params={"expand":"history"})
        data = r.json()
        hist = data.get("history") or {}
        created_by = ((hist.get("createdBy") or {}).get("publicName")
                      or (hist.get("createdBy") or {}).get("displayName")
                      or "")
        created_at = hist.get("createdDate","")
        return created_by, created_at
    except Exception:
        return "", ""

# ================== V2 LISTING (ANTI-LOOP) ==================
def get_spaces_map(sess: requests.Session) -> dict:
    """
    { space_key: {"id":..., "status":"current"/"archived", "type":"global"/"personal", "name":...}, ... }
    """
    url = v2_url("spaces")
    params = {"limit": 200}
    out = {}
    while url:
        r = safe_get(sess, url, params=params if "?" not in url else None)
        data = r.json()
        for sp in data.get("results", []):
            key = sp.get("key") or ""
            out[key] = {
                "id": sp.get("id"),
                "status": sp.get("status"),
                "type": sp.get("type"),
                "name": sp.get("name"),
            }
        url = _follow_next((data.get("_links") or {}).get("next"))
        params = None
        time.sleep(SLEEP_SECONDS)
    return out

def iter_pages_v2(sess: requests.Session, space_id: str):
    """Itera páginas de un espacio por API v2 (sin loops)."""
    url = v2_url("pages")
    params = {"limit": 200, "space-id": space_id}
    while url:
        r = safe_get(sess, url, params=params if "?" not in url else None)
        data = r.json()
        for pg in data.get("results", []):
            yield {
                "id": pg.get("id",""),
                "title": pg.get("title",""),
                "created_at": pg.get("createdAt",""),
                "space_id": pg.get("spaceId"),
            }
        url = _follow_next((data.get("_links") or {}).get("next"))
        params = None
        time.sleep(SLEEP_SECONDS)

def build_page_index(sess: requests.Session) -> list:
    """
    Devuelve lista de páginas a escanear (sin duplicados), usando v2 para evitar bucles.
    Respeta IGNORE_ARCHIVED_SPACES / SKIP_PERSONAL_SPACES y SPACE_KEYS.
    """
    smap = get_spaces_map(sess)

    # Filtra espacios permitidos
    allowed_keys = []
    for k, meta in smap.items():
        if not k:
            continue
        if IGNORE_ARCHIVED_SPACES and meta.get("status") == "archived":
            continue
        if SKIP_PERSONAL_SPACES and (meta.get("type") == "personal" or k.startswith("~")):
            continue
        allowed_keys.append(k)

    # Si el usuario indicó SPACE_KEYS, intersecta
    if SPACE_KEYS:
        space_keys_to_scan = [k for k in SPACE_KEYS if k in allowed_keys]
    else:
        space_keys_to_scan = allowed_keys

    print(f"Espacios a escanear (v2): {len(space_keys_to_scan)}")

    # Enumerar páginas por espacio
    pages = []
    seen_ids = set()
    total_counter = 0
    for sk in space_keys_to_scan:
        meta = smap.get(sk) or {}
        sid = meta.get("id")
        sname = meta.get("name","")
        if not sid:
            continue
        count_this_space = 0
        for pg in iter_pages_v2(sess, sid):
            pid = pg["id"]
            if not pid or pid in seen_ids:
                continue
            seen_ids.add(pid)
            pages.append({
                "id": pid,
                "title": pg.get("title",""),
                "space_key": sk,
                "space_name": sname,
                "created_at": pg.get("created_at",""),
            })
            count_this_space += 1
            total_counter += 1
        print(f"  · {sk} — {sname}: {count_this_space} páginas")
    print(f"TOTAL páginas (v2 enumeradas): {total_counter}")
    return pages

# ================== BATCH PROCESS ==================
def process_batch(sess: requests.Session, pages: list, rows_out: list) -> int:
    """
    Lanza en paralelo la consulta de restricciones y agrega al buffer rows_out las que sí tienen.
    Devuelve cuántas páginas quedaron (restringidas).
    """
    kept_local = 0

    def worker(p):
        pid   = p["id"]
        title = p.get("title","")
        skey  = p.get("space_key","")
        sname = p.get("space_name","")
        cat   = p.get("created_at","")

        restr = get_page_restrictions(sess, pid)
        has_read   = bool(restr["read"]["users"] or restr["read"]["groups"])
        has_update = bool(restr["update"]["users"] or restr["update"]["groups"])
        if not (has_read or has_update):
            return None

        # Sólo si hay restricciones, pedimos history para creator (minimizamos llamadas)
        created_by, created_at2 = get_page_history(sess, pid)
        created_at = created_at2 or cat

        read_str = join_names(restr["read"]["users"], restr["read"]["groups"])
        upd_str  = join_names(restr["update"]["users"], restr["update"]["groups"])

        return {
            "page_id": pid,
            "title": title,
            "url": page_ui_url(pid),
            "space_key": skey,
            "space_name": sname,
            "created_by": created_by,
            "created_at": created_at,
            "read_restricted_to": read_str,
            "update_restricted_to": upd_str,
        }

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(worker, p) for p in pages]
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                rows_out.append(res)
                kept_local += 1
    return kept_local

# ================== MAIN ==================
def main():
    if not (BASE_URL.startswith("http") and EMAIL and API_TOKEN and API_TOKEN != "xxxxxxxx"):
        print("⚠️ Configura CONFLUENCE_BASE_URL, ATLASSIAN_EMAIL y ATLASSIAN_API_TOKEN.")
        return

    t0 = time.time()
    sess = build_session()

    # 1) Enumerar páginas por v2 (anti-loop)
    page_index = build_page_index(sess)
    total_v2 = len(page_index)

    # 2) Aplicar tope duro
    cap = MAX_PAGES if MAX_PAGES > 0 else total_v2
    if cap < total_v2:
        print(f"⚠️ Aplicando tope MAX_PAGES={cap} (v2={total_v2}).")

    # 3) Procesar en lotes + paralelo
    rows = []
    processed, kept = 0, 0
    fast_exit_triggered = False

    batch = []
    for p in page_index[:cap]:
        batch.append(p)
        if len(batch) >= PAGE_LIMIT:
            kept += process_batch(sess, batch, rows)
            processed += len(batch)
            print(f"   · Páginas procesadas: {processed}/{cap} | con restricción: {kept}")
            batch = []
            if FAST_EXIT_ON_FIND and kept > 0:
                print("⚑ Encontré al menos 1 página restringida. Saliendo por FAST_EXIT_ON_FIND.")
                fast_exit_triggered = True
                break
            time.sleep(SLEEP_SECONDS)

    if not fast_exit_triggered and batch:
        kept += process_batch(sess, batch, rows)
        processed += len(batch)
        print(f"   · Páginas procesadas: {processed}/{cap} | con restricción: {kept}")
        if FAST_EXIT_ON_FIND and kept > 0:
            print("⚑ Encontré al menos 1 página restringida. Saliendo por FAST_EXIT_ON_FIND.")

    # 4) CSV
    fieldnames = [
        "page_id","title","url",
        "space_key","space_name",
        "created_by","created_at",
        "read_restricted_to","update_restricted_to",
    ]
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    dt = time.time() - t0
    print(f"\n✅ Listo. CSV: {CSV_PATH} | páginas v2: {total_v2} | escaneadas: {processed} | con restricción: {kept} | tiempo: {dt:0.1f}s")

if __name__ == "__main__":
    main()
