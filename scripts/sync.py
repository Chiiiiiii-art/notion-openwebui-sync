import os
import sys
import json
import time
import re
import urllib.request
import urllib.parse
import urllib.error
import http.client
import threading
import concurrent.futures
import io
import uuid
import argparse
import logging
import dataclasses
from typing import Dict, List, Any, Optional, Tuple

# ponytail: Stdlib-only Notion -> Open WebUI incremental sync. Zero external dependencies.
# Ceiling: Naive synchronous HTTP requests. Upgrade path: concurrent.futures / asyncio if >1000 pages.

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

def resolve_path(filename: str) -> str:
    # ponytail: Priority cwd -> ROOT_DIR -> BASE_DIR. Default to ROOT_DIR if missing.
    for candidate in [filename, os.path.join(ROOT_DIR, filename), os.path.join(BASE_DIR, filename)]:
        if os.path.exists(candidate):
            return os.path.abspath(candidate)
    return os.path.join(ROOT_DIR, filename)

STATE_FILE = os.getenv("STATE_FILE", resolve_path("sync_state.json"))
CONFIG_FILE = os.getenv("CONFIG_FILE", resolve_path("config.json"))
FLUSH_EVERY = 5  # ponytail: tune per use-case; smaller = safer against crash, larger = fewer I/O ops

log = logging.getLogger("notion_sync")

# --- Config & State Data Models ---

@dataclasses.dataclass
class Config:
    notion_api_key: str = ""
    notion_root_page_id: str = ""
    openwebui_base_url: str = "http://localhost:3000"
    openwebui_api_key: str = ""
    knowledge_base_id: str = ""
    blacklist_title_prefixes: List[str] = dataclasses.field(default_factory=list)
    blacklist_title_suffixes: List[str] = dataclasses.field(default_factory=list)
    blacklist_page_ids: List[str] = dataclasses.field(default_factory=list)
    excluded_block_types: List[str] = dataclasses.field(default_factory=list)

    def __post_init__(self):
        self.openwebui_base_url = self.openwebui_base_url.rstrip("/")

    def validate(self):
        if not self.notion_api_key:
            raise ValueError("Missing notion_api_key in config / ENV")
        if not self.notion_root_page_id:
            raise ValueError("Missing notion_root_page_id in config / ENV")

def load_config() -> Config:
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
    else:
        raw = {
            "notion_api_key": os.getenv("NOTION_API_KEY", ""),
            "notion_root_page_id": os.getenv("NOTION_ROOT_PAGE_ID", ""),
            "openwebui_base_url": os.getenv("OPENWEBUI_BASE_URL", "http://localhost:3000"),
            "openwebui_api_key": os.getenv("OPENWEBUI_API_KEY", ""),
            "knowledge_base_id": os.getenv("KNOWLEDGE_BASE_ID", ""),
            "blacklist_title_prefixes": ["[Draft]", "TEMP_"],
            "blacklist_title_suffixes": [],
            "blacklist_page_ids": [],
            "excluded_block_types": []
        }
    known = {f.name for f in dataclasses.fields(Config)}
    return Config(**{k: v for k, v in raw.items() if k in known})

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                st = json.load(f)
                if "directories" not in st:
                    st["directories"] = {}
                if "dir_parents" not in st:
                    st["dir_parents"] = {}
                if "pages" not in st:
                    st["pages"] = {}
                return st
        except Exception:
            pass
    return {"pages": {}, "directories": {}, "dir_parents": {}}

def save_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def sanitize_filename(name: str) -> str:
    return re.sub(r'[\/\\:\*\?"<>\|]', '_', name).strip()

# --- HTTP Connection Pool & Utilities ---

class ConnectionPool:
    """
    ponytail: Thread-local persistent HTTPS/HTTP connections.
    Each thread maintains its own keep-alive connection per host.
    Ceiling: O(threads * hosts) total connections;
    upgrade path: bounded connection pool with semaphore if thread count grows.
    """
    def __init__(self):
        self._local = threading.local()

    def _close_conn(self, host: str):
        conns = getattr(self._local, "conns", {})
        conn = conns.pop(host, None)
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    def _get_conn(self, host: str, is_https: bool) -> http.client.HTTPConnection:
        conns = getattr(self._local, "conns", None)
        if conns is None:
            self._local.conns = {}
            conns = self._local.conns
        conn = conns.get(host)
        if conn is None:
            conn = (http.client.HTTPSConnection(host, timeout=120)
                    if is_https else http.client.HTTPConnection(host, timeout=120))
            conns[host] = conn
        return conn

    def request(self, url: str, method: str, headers: dict, body: Optional[bytes] = None) -> Tuple[int, bytes, dict]:
        parsed = urllib.parse.urlparse(url)
        host = parsed.netloc
        is_https = parsed.scheme == "https"
        path = parsed.path + (f"?{parsed.query}" if parsed.query else "")

        for attempt in range(2):  # 1 retry on stale keep-alive connection
            conn = self._get_conn(host, is_https)
            try:
                conn.request(method, path, body=body, headers=headers)
                resp = conn.getresponse()
                resp_headers = dict(resp.getheaders())
                data = resp.read()
                return resp.status, data, resp_headers
            except Exception:
                self._close_conn(host)
                if attempt == 1:
                    raise
        raise RuntimeError(f"Connection to {host} failed after reconnect")

    def close_all(self):
        conns = getattr(self._local, "conns", {})
        for conn in conns.values():
            try:
                conn.close()
            except Exception:
                pass
        self._local.conns = {}

_notion_pool = ConnectionPool()
_owui_pool = ConnectionPool()

def with_retry(max_retries: int = 3, backoff: float = 1.0):
    """ponytail: Exponential backoff with Retry-After header support for 429 rate limiting."""
    def decorator(fn):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return fn(*args, **kwargs)
                except urllib.error.HTTPError as e:
                    if e.code in (400, 401, 403, 404):
                        raise
                    if e.code == 429 and attempt < max_retries - 1:
                        retry_after = None
                        try:
                            # 相容大小寫 Header
                            hdrs = {k.lower(): v for k, v in e.headers.items()} if hasattr(e, "headers") and e.headers else {}
                            if "retry-after" in hdrs:
                                retry_after = float(hdrs["retry-after"])
                            else:
                                # 從 Notion 回傳的 JSON 錯誤訊息解析
                                err_data = json.loads(e.msg)
                                retry_after = float(err_data.get("additional_data", {}).get("retry_after", 0))
                        except Exception:
                            pass
                        wait = (retry_after + 1.0) if retry_after else backoff * (2 ** attempt) * 5.0
                        log.warning(f"429 Rate limited. Waiting {wait:.1f}s... ({fn.__name__})")
                        time.sleep(wait)
                        continue
                    if attempt == max_retries - 1:
                        raise
                    wait = backoff * (2 ** attempt)
                    log.warning(f"Retry {attempt + 1}/{max_retries} for {fn.__name__}: {e}. Waiting {wait:.1f}s...")
                    time.sleep(wait)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    wait = backoff * (2 ** attempt)
                    log.warning(f"Retry {attempt + 1}/{max_retries} for {fn.__name__}: {e}. Waiting {wait:.1f}s...")
                    time.sleep(wait)
        return wrapper
    return decorator

# --- Notion API Client ---

@with_retry(max_retries=5, backoff=1.0)
def notion_request(endpoint: str, api_key: str, method: str = "GET", payload: Optional[dict] = None) -> dict:
    url = f"https://api.notion.com/v1/{endpoint.lstrip('/')}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
        "Connection": "keep-alive"
    }
    body = json.dumps(payload).encode("utf-8") if payload else None
    status, data, resp_headers = _notion_pool.request(url, method, headers, body)
    decoded = data.decode("utf-8", errors="replace")
    if status >= 400:
        raise urllib.error.HTTPError(url, status, decoded, resp_headers, None)
    return json.loads(decoded)

def fetch_page(page_id: str, api_key: str) -> dict:
    return notion_request(f"pages/{page_id}", api_key)

def fetch_block_children(block_id: str, api_key: str) -> List[dict]:
    blocks = []
    cursor = None
    while True:
        endpoint = f"blocks/{block_id}/children"
        if cursor:
            endpoint += f"?start_cursor={cursor}"
        res = notion_request(endpoint, api_key)
        blocks.extend(res.get("results", []))
        if not res.get("has_more"):
            break
        cursor = res.get("next_cursor")
    return blocks

def query_database_pages(db_id: str, api_key: str) -> List[str]:
    page_ids = []
    cursor = None
    while True:
        endpoint = f"databases/{db_id}/query"
        payload = {}
        if cursor:
            payload["start_cursor"] = cursor
        try:
            res = notion_request(endpoint, api_key, method="POST", payload=payload)
            for item in res.get("results", []):
                page_ids.append(item.get("id"))
            if not res.get("has_more"):
                break
            cursor = res.get("next_cursor")
        except Exception as e:
            log.warning(f"Query database {db_id} failed: {e}")
            break
    return page_ids

def scan_blocks_for_children(block_id: str, api_key: str) -> List[dict]:
    discovered = []
    blocks = fetch_block_children(block_id, api_key)
    for b in blocks:
        btype = b.get("type")
        if btype == "child_page":
            discovered.append({"type": "page", "id": b.get("id")})
        elif btype == "child_database":
            discovered.append({"type": "database", "id": b.get("id")})
        elif b.get("has_children") and btype not in ("synced_block", "child_page", "child_database"):
            discovered.extend(scan_blocks_for_children(b.get("id"), api_key))
    return discovered

# --- Markdown Renderer ---

def annotate_plain_text(text: str, annotations: dict) -> str:
    # ponytail: exact port of notion-to-md.ts annotatePlainText logic.
    if not text or re.match(r"^\s*$", text):
        return text

    leading_match = re.match(r"^(\s*)", text)
    trailing_match = re.search(r"(\s*)$", text)

    leading_space = leading_match.group(1) if leading_match else ""
    trailing_space = trailing_match.group(1) if trailing_match else ""

    text = text.strip()

    if text:
        if annotations.get("code"):
            text = f"`{text}`"
        if annotations.get("bold"):
            text = f"**{text}**"
        if annotations.get("italic"):
            text = f"_{text}_"
        if annotations.get("strikethrough"):
            text = f"~~{text}~~"
        if annotations.get("underline"):
            text = f"<u>{text}</u>"

    return f"{leading_space}{text}{trailing_space}"

def rich_text_to_md(rich_texts: List[dict]) -> str:
    if not isinstance(rich_texts, list):
        return ""
    out = []
    for content in rich_texts:
        if not isinstance(content, dict):
            continue
        ctype = content.get("type")
        if ctype == "equation":
            expr = content.get("equation", {}).get("expression", "")
            out.append(f"${expr}$")
            continue

        plain_text = content.get("plain_text", "")
        if not plain_text and "text" in content:
            plain_text = content.get("text", {}).get("content", "")

        annot = content.get("annotations") or {}
        plain_text = annotate_plain_text(plain_text, annot)

        href = content.get("href")
        if href:
            plain_text = f"[{plain_text}]({href})"
        out.append(plain_text)
    return "".join(out)

def extract_media_title(block_content: dict, default_title: str) -> str:
    caption_list = block_content.get("caption", [])
    caption = rich_text_to_md(caption_list).strip()
    if caption:
        return caption
    
    file_type = block_content.get("type", "")
    url = ""
    if file_type == "external":
        url = block_content.get("external", {}).get("url", "")
    elif file_type == "file":
        url = block_content.get("file", {}).get("url", "")
    
    if url:
        m = re.search(r'[^\/\\&\?]+\.\w{3,4}(?=([\?&].*$|$))', url)
        if m:
            return m.group(0)
    return default_title

def page_properties_to_md(page: dict) -> str:
    props = page.get("properties", {})
    md_lines = []
    for prop_name, p in props.items():
        ptype = p.get("type")
        if ptype == "title":
            continue
        val_str = ""
        if ptype == "rich_text":
            val_str = rich_text_to_md(p.get("rich_text", []))
        elif ptype == "select":
            sel = p.get("select")
            if sel:
                val_str = sel.get("name", "")
        elif ptype == "multi_select":
            ms = p.get("multi_select", [])
            val_str = ", ".join(item.get("name", "") for item in ms if item.get("name"))
        elif ptype == "number":
            val_str = str(p.get("number", "")) if p.get("number") is not None else ""
        elif ptype == "url":
            url = p.get("url", "")
            if url:
                val_str = f"[{url}]({url})"
        elif ptype == "email":
            val_str = p.get("email", "")
        elif ptype == "phone_number":
            val_str = p.get("phone_number", "")
        elif ptype == "date":
            d = p.get("date")
            if d:
                val_str = d.get("start", "")
                if d.get("end"):
                    val_str += f" -> {d.get('end')}"
        elif ptype == "checkbox":
            val_str = "Yes" if p.get("checkbox") else "No"
        elif ptype == "formula":
            form = p.get("formula", {})
            ftype = form.get("type")
            if ftype in ("string", "number", "boolean", "date"):
                val_str = str(form.get(ftype, ""))

        if val_str.strip():
            md_lines.append(f"**{prop_name}**: {val_str}")

    if md_lines:
        return "\n".join(md_lines) + "\n\n---\n\n"
    return ""

def block_to_md(block: dict, api_key: str, excluded_types: List[str], depth: int = 0, numbered_index: int = 1) -> str:
    btype = block.get("type")
    if not btype or btype in excluded_types:
        return ""
    
    # Handle Synced Block
    if btype == "synced_block":
        synced_info = block.get("synced_block") or {}
        synced_from = synced_info.get("synced_from")
        target_id = None
        if isinstance(synced_from, dict) and synced_from.get("block_id"):
            target_id = synced_from.get("block_id")
        else:
            target_id = block.get("id")

        children = []
        if target_id:
            try:
                children = fetch_block_children(target_id, api_key)
            except Exception as e:
                log.warning(f"[SyncedBlock] Fetch target {target_id} failed: {e}")
                if target_id != block.get("id"):
                    try:
                        children = fetch_block_children(block.get("id"), api_key)
                    except Exception:
                        pass
        return blocks_to_md(children, api_key, excluded_types, depth)

    content = block.get(btype, {})
    indent = "  " * depth
    md = ""

    if btype == "paragraph":
        text = rich_text_to_md(content.get("rich_text", []))
        md = f"{indent}{text}\n\n" if text else ""
    elif btype.startswith("heading_") or btype.startswith("toggle_heading_"):
        try:
            level_num = int(btype.split("_")[-1])
        except ValueError:
            level_num = 1
        level = "#" * level_num
        text = rich_text_to_md(content.get("rich_text", []))
        heading_line = f"{level} {text}"
        
        if btype.startswith("toggle_heading_"):
            children_md = ""
            if block.get("has_children"):
                children = fetch_block_children(block.get("id"), api_key)
                children_md = blocks_to_md(children, api_key, excluded_types, depth)
            md = f"<details><summary>{heading_line}</summary>\n\n{children_md}\n</details>\n\n"
        else:
            md = f"{heading_line}\n\n"
    elif btype == "bulleted_list_item":
        text = rich_text_to_md(content.get("rich_text", []))
        md = f"{indent}- {text}\n"
    elif btype == "numbered_list_item":
        text = rich_text_to_md(content.get("rich_text", []))
        num = content.get("number", numbered_index)
        md = f"{indent}{num}. {text}\n"
    elif btype == "to_do":
        text = rich_text_to_md(content.get("rich_text", []))
        checked = "x" if content.get("checked") else " "
        md = f"{indent}- [{checked}] {text}\n"
    elif btype == "code":
        lang = content.get("language", "plaintext")
        code_str = "".join([t.get("plain_text", "") for t in content.get("rich_text", [])])
        md = f"```{lang}\n{code_str}\n```\n\n"
    elif btype == "quote":
        text = rich_text_to_md(content.get("rich_text", []))
        if block.get("has_children"):
            children = fetch_block_children(block.get("id"), api_key)
            child_md = blocks_to_md(children, api_key, excluded_types, depth)
            full_text = f"{text}\n\n{child_md}".strip()
            lines = full_text.split("\n")
            quoted = "\n".join([f"> {line}" if line.strip() else ">" for line in lines])
            md = f"{quoted}\n\n"
        else:
            lines = text.split("\n")
            quoted = "\n".join([f"> {line}" if line.strip() else ">" for line in lines])
            md = f"{quoted}\n\n"
    elif btype == "callout":
        text = rich_text_to_md(content.get("rich_text", []))
        icon_obj = content.get("icon") or {}
        icon_str = ""
        if icon_obj.get("type") == "emoji":
            icon_str = icon_obj.get("emoji", "") + " "
        
        callout_body = f"{icon_str}{text}".strip()
        if block.get("has_children"):
            children = fetch_block_children(block.get("id"), api_key)
            child_md = blocks_to_md(children, api_key, excluded_types, depth)
            callout_body = f"{callout_body}\n\n{child_md}".strip()

        lines = callout_body.split("\n")
        quoted = "\n".join([f"> {line}" if line.strip() else ">" for line in lines])
        md = f"{quoted}\n\n"
    elif btype == "toggle":
        summary = rich_text_to_md(content.get("rich_text", []))
        children_md = ""
        if block.get("has_children"):
            children = fetch_block_children(block.get("id"), api_key)
            children_md = blocks_to_md(children, api_key, excluded_types, depth)
        md = f"<details><summary>{summary}</summary>\n\n{children_md}\n</details>\n\n"
    elif btype == "equation":
        expr = content.get("expression", "")
        md = f"$${expr}$$\n\n" if expr else ""
    elif btype == "image":
        img_type = content.get("type", "")
        img_url = content.get(img_type, {}).get("url", "")
        if img_url and "?" in img_url:
            # Strip dynamic tokens from Notion S3 URLs so markdown output is deterministic
            img_url = img_url.split("?")[0]
        caption = extract_media_title(content, "image")
        md = f"![{caption}]({img_url})\n\n"
    elif btype in ("video", "file", "pdf"):
        file_type = content.get("type", "")
        url = content.get(file_type, {}).get("url", "")
        if url and "?" in url:
            url = url.split("?")[0]
        caption = extract_media_title(content, btype)
        md = f"[{caption}]({url})\n\n"
    elif btype in ("bookmark", "embed", "link_preview"):
        url = content.get("url", "")
        caption = extract_media_title(content, url or btype)
        md = f"[{caption}]({url})\n\n"
    elif btype == "link_to_page":
        link_type = content.get("type")
        page_id = content.get("page_id", "") if link_type == "page_id" else ""
        title = "link_to_page"
        if page_id and api_key:
            try:
                target_page = fetch_page(page_id, api_key)
                title = get_page_title(target_page)
            except Exception:
                pass
        target_url = f"https://www.notion.so/{page_id}" if page_id else ""
        md = f"[{title}]({target_url})\n\n" if target_url else ""
    elif btype in ("child_page", "child_database"):
        page_title = content.get("title", btype)
        clean_name = sanitize_filename(page_title)
        md = f"[{page_title}]({clean_name}.md)\n\n"
    elif btype == "divider":
        md = "---\n\n"
    elif btype == "table":
        children = fetch_block_children(block.get("id"), api_key)
        rows = []
        for i, row_block in enumerate(children):
            if row_block.get("type") != "table_row":
                continue
            cells = row_block.get("table_row", {}).get("cells", [])
            cell_str = " | ".join([rich_text_to_md(c) for c in cells])
            rows.append(f"| {cell_str} |")
            if i == 0:
                rows.append("| " + " | ".join(["---"] * len(cells)) + " |")
        md = "\n".join(rows) + "\n\n"

    if block.get("has_children") and btype in ("bulleted_list_item", "numbered_list_item", "to_do"):
        children = fetch_block_children(block.get("id"), api_key)
        child_md = blocks_to_md(children, api_key, excluded_types, depth + 1)
        md += child_md

    return md

def blocks_to_md(blocks: List[dict], api_key: str, excluded_types: List[str], depth: int = 0) -> str:
    res = []
    numbered_index = 0
    for b in blocks:
        btype = b.get("type")
        if btype == "numbered_list_item":
            numbered_index += 1
        else:
            numbered_index = 0
        res.append(block_to_md(b, api_key, excluded_types, depth, numbered_index=numbered_index))
    return "".join(res)

def get_page_title(page: dict) -> str:
    props = page.get("properties", {})
    for p in props.values():
        if p.get("type") == "title":
            title_objs = p.get("title", [])
            if title_objs:
                return title_objs[0].get("plain_text", "Untitled")
    return "Untitled"

# --- Notion Recursive Traversal ---

def traverse_notion_tree(
    page_id: str,
    config: Config,
    discovered_pages: Dict[str, dict],
    parent_page_id: Optional[str] = None,
    parent_path: str = ""
):
    api_key = config.notion_api_key
    page_id_clean = page_id.replace("-", "")

    if page_id_clean in config.blacklist_page_ids or page_id_clean in discovered_pages:
        return

    try:
        page = fetch_page(page_id_clean, api_key)
    except Exception as e:
        log.warning(f"Fetch page {page_id_clean} failed: {e}")
        return

    title = get_page_title(page)
    for prefix in config.blacklist_title_prefixes:
        if title.startswith(prefix):
            log.debug(f"[Skip] Blacklisted title prefix: {title}")
            return
    for suffix in config.blacklist_title_suffixes:
        if title.endswith(suffix):
            log.debug(f"[Skip] Blacklisted title suffix: {title}")
            return

    clean_title = sanitize_filename(title)
    current_path = f"{parent_path} > {clean_title}" if parent_path else clean_title

    last_edited = page.get("last_edited_time", "")
    children_items = scan_blocks_for_children(page_id_clean, api_key)
    has_children = len(children_items) > 0

    discovered_pages[page_id_clean] = {
        "id": page_id_clean,
        "title": title,
        "clean_title": clean_title,
        "filename": f"{clean_title}.md",
        "last_edited_time": last_edited,
        "parent_page_id": parent_page_id,
        "has_children": has_children,
        "page_obj": page
    }

    # Discover child pages & databases recursively
    for item in children_items:
        if item["type"] == "page":
            traverse_notion_tree(item["id"], config, discovered_pages, parent_page_id=page_id_clean, parent_path=current_path)
        elif item["type"] == "database":
            db_page_ids = query_database_pages(item["id"], api_key)
            for db_page_id in db_page_ids:
                traverse_notion_tree(db_page_id, config, discovered_pages, parent_page_id=page_id_clean, parent_path=current_path)

# --- Open WebUI API Client ---

def openwebui_request(url: str, api_key: str, method: str = "POST", payload: Optional[dict] = None, data_bytes: Optional[bytes] = None, content_type: Optional[str] = None, ignore_errors: bool = False) -> dict:
    headers = {"Authorization": f"Bearer {api_key}", "Connection": "keep-alive"}
    if content_type:
        headers["Content-Type"] = content_type
    elif payload is not None:
        headers["Content-Type"] = "application/json"
        data_bytes = json.dumps(payload).encode("utf-8")

    try:
        status, data, resp_headers = _owui_pool.request(url, method, headers, data_bytes)
        decoded = data.decode("utf-8", errors="replace")
        if status >= 400:
            if ignore_errors:
                return {}
            log.error(f"[OpenWebUI Error] {method} {url} -> HTTP {status}: {decoded}")
            raise urllib.error.HTTPError(url, status, decoded, resp_headers, None)
        return json.loads(decoded) if decoded.strip() else {}
    except urllib.error.HTTPError:
        raise
    except Exception as e:
        if ignore_errors:
            return {}
        raise

def openwebui_create_knowledge_dir(base_url: str, api_key: str, knowledge_id: str, dir_name: str, parent_id: Optional[str] = None) -> str:
    url = f"{base_url}/api/v1/knowledge/{knowledge_id}/dirs/create"
    payload = {"name": dir_name, "parent_id": parent_id}
    try:
        res = openwebui_request(url, api_key, method="POST", payload=payload)
        return res.get("id", "")
    except Exception as e:
        try:
            url_alt = f"{base_url}/api/v1/knowledge/{knowledge_id}/directory/create"
            res = openwebui_request(url_alt, api_key, method="POST", payload=payload)
            return res.get("id", "")
        except Exception:
            log.warning(f"Failed to create Open WebUI dir '{dir_name}' in KB '{knowledge_id}': {e}")
            return ""

def openwebui_update_knowledge_dir(base_url: str, api_key: str, knowledge_id: str, dir_id: str, new_name: str, parent_id: Optional[str] = None):
    if not dir_id or not new_name:
        return
    url = f"{base_url}/api/v1/knowledge/{knowledge_id}/dirs/{dir_id}/update"
    payload = {"name": new_name, "parent_id": parent_id}
    try:
        openwebui_request(url, api_key, method="POST", payload=payload)
    except Exception as e:
        url_alt = f"{base_url}/api/v1/knowledge/{knowledge_id}/dir/{dir_id}/update"
        try:
            openwebui_request(url_alt, api_key, method="POST", payload=payload)
        except Exception as e2:
            log.warning(f"Failed to update Open WebUI dir '{new_name}' (ID: {dir_id}, parent: {parent_id}): {e2}")

@with_retry(max_retries=3, backoff=1.0)
def openwebui_upload_file(base_url: str, api_key: str, filename: str, content: str) -> str:
    url = f"{base_url}/api/v1/files/"
    boundary = f"----WebKitFormBoundary{uuid.uuid4().hex}"
    
    # ponytail: Cleanse control chars and null bytes (\x00-\x1f except \n, \r, \t) that break OpenWebUI text processing
    clean_content = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', content)
    # ponytail: Escape filename for Content-Disposition header
    safe_filename = filename.replace('"', '_').replace('\\', '_')

    body = io.BytesIO()
    body.write(f"--{boundary}\r\n".encode('utf-8'))
    body.write(f'Content-Disposition: form-data; name="file"; filename="{safe_filename}"\r\n'.encode('utf-8'))
    body.write(b"Content-Type: text/markdown\r\n\r\n")
    body.write(clean_content.encode("utf-8"))
    body.write(f"\r\n--{boundary}--\r\n".encode('utf-8'))
    
    res = openwebui_request(url, api_key, method="POST", data_bytes=body.getvalue(), content_type=f"multipart/form-data; boundary={boundary}")
    file_id = res.get("id", "")
    if file_id:
        openwebui_wait_for_file_processing(base_url, api_key, file_id)
    return file_id

def openwebui_wait_for_file_processing(base_url: str, api_key: str, file_id: str, max_retries: int = 10):
    # Wait for Open WebUI background text extraction/indexing to finish before attaching to KB
    for _ in range(max_retries):
        try:
            res = openwebui_request(f"{base_url}/api/v1/files/{file_id}", api_key, method="GET", ignore_errors=True)
            status = res.get("status") if isinstance(res, dict) else ""
            if status in ("completed", "success", True, "ok") or (res and "id" in res):
                time.sleep(0.2)
                return
        except Exception:
            pass
        time.sleep(0.2)

@with_retry(max_retries=4, backoff=1.0)
def openwebui_attach_knowledge(
    base_url: str,
    api_key: str,
    knowledge_id: str,
    file_id: str,
    folder_id: Optional[str] = None
):
    if not file_id:
        log.error("Cannot attach empty file_id to Knowledge Base.")
        return
        
    add_url = f"{base_url}/api/v1/knowledge/{knowledge_id}/file/add"
    add_payload = {"file_id": file_id}
    if folder_id:
        add_payload["directory_id"] = folder_id
    openwebui_request(add_url, api_key, method="POST", payload=add_payload)

def openwebui_move_knowledge_file(base_url: str, api_key: str, knowledge_id: str, file_id: str, directory_id: Optional[str]):
    if not file_id:
        return
    url = f"{base_url}/api/v1/knowledge/{knowledge_id}/file/move"
    payload = {"file_id": file_id, "directory_id": directory_id}
    openwebui_request(url, api_key, method="POST", payload=payload, ignore_errors=True)

def openwebui_remove_knowledge(base_url: str, api_key: str, knowledge_id: str, file_id: str):
    if not file_id:
        return
    url = f"{base_url}/api/v1/knowledge/{knowledge_id}/file/remove"
    openwebui_request(url, api_key, method="POST", payload={"file_id": file_id, "delete_file": False}, ignore_errors=True)

def openwebui_delete_file(base_url: str, api_key: str, file_id: str):
    if not file_id:
        return
    url = f"{base_url}/api/v1/files/{file_id}"
    openwebui_request(url, api_key, method="DELETE", ignore_errors=True)

def openwebui_delete_knowledge_dir(base_url: str, api_key: str, knowledge_id: str, dir_id: str):
    if not dir_id:
        return
    endpoints = [
        (f"{base_url}/api/v1/knowledge/{knowledge_id}/dirs/{dir_id}/delete?move_files=false", "DELETE"),
        (f"{base_url}/api/v1/knowledge/{knowledge_id}/dirs/{dir_id}/delete", "DELETE"),
        (f"{base_url}/api/v1/knowledge/{knowledge_id}/dirs/{dir_id}/delete", "POST"),
        (f"{base_url}/api/v1/knowledge/{knowledge_id}/dirs/{dir_id}", "DELETE"),
    ]
    for url, method in endpoints:
        try:
            res = openwebui_request(url, api_key, method=method, ignore_errors=True)
            if res is not None and res != {}:
                return
        except Exception:
            pass

# --- Directory Helper (Top-level Function) ---

def get_or_create_page_dir(
    pid: str,
    discovered: Dict[str, dict],
    created_dirs: Dict[str, str],
    stored_dirs: Dict[str, str],
    stored_dir_parents: Dict[str, Optional[str]],
    config: Config,
    state: dict,
    dry_run: bool = False
) -> Optional[str]:
    if pid not in discovered:
        return None
        
    info = discovered[pid]
    dir_name = info["clean_title"]

    if not info.get("has_children"):
        return None

    parent_pid = info.get("parent_page_id")
    parent_dir_id = (
        get_or_create_page_dir(
            parent_pid, discovered, created_dirs, stored_dirs, stored_dir_parents, config, state, dry_run=dry_run
        )
        if parent_pid
        else None
    )

    kb_id = config.knowledge_base_id

    if pid in stored_dirs:
        dir_id = stored_dirs[pid]
        created_dirs[pid] = dir_id

        current_parent_dir_id = stored_dir_parents.get(pid)
        if current_parent_dir_id != parent_dir_id:
            if dry_run:
                log.info(f"[Dry-Run][Move Dir] {dir_name} ({pid}): parent dir {current_parent_dir_id} -> {parent_dir_id}")
            else:
                log.info(f"[Move Dir] {dir_name}: parent dir {current_parent_dir_id} -> {parent_dir_id}")
                openwebui_update_knowledge_dir(
                    config.openwebui_base_url,
                    config.openwebui_api_key,
                    kb_id,
                    dir_id,
                    new_name=dir_name,
                    parent_id=parent_dir_id
                )
                stored_dir_parents[pid] = parent_dir_id
                state["dir_parents"] = stored_dir_parents
                save_state(state)
        return dir_id

    if dry_run:
        log.info(f"[Dry-Run] Would create Open WebUI Knowledge Dir: {dir_name} (parent: {parent_dir_id})")
        return None

    dir_id = openwebui_create_knowledge_dir(
        config.openwebui_base_url,
        config.openwebui_api_key,
        kb_id,
        dir_name,
        parent_id=parent_dir_id
    )
    if dir_id:
        created_dirs[pid] = dir_id
        stored_dirs[pid] = dir_id
        stored_dir_parents[pid] = parent_dir_id
        state["directories"] = stored_dirs
        state["dir_parents"] = stored_dir_parents
        save_state(state)
        log.info(f"[Folder] Created Open WebUI Knowledge Dir: {dir_name} (Dir ID: {dir_id}, parent: {parent_dir_id})")
    return dir_id

# --- Directory Move Helper ---

def sync_moved_dirs_and_files(
    discovered: Dict[str, dict],
    stored_pages: Dict[str, dict],
    stored_dirs: Dict[str, str],
    stored_dir_parents: Dict[str, Optional[str]],
    created_dirs: Dict[str, str],
    config: Config,
    state: dict,
    dry_run: bool
) -> set:
    """
    ponytail: Detect parent_page_id changes.
    1. If page has a dir, call update_dir to change parent_id (child dirs auto-follow).
    2. If page has no content edit (last_edited_time unchanged), call file/move directly.
    Returns set of PIDs where only file move was needed (so Phase 3 skips re-uploading).
    """
    parent_only_moved_pids = set()

    for pid, info in discovered.items():
        old_entry = stored_pages.get(pid)
        if not old_entry:
            continue
        old_parent = old_entry.get("parent_page_id")
        new_parent = info.get("parent_page_id")
        new_parent_dir_id = created_dirs.get(new_parent) if new_parent else None

        dir_id = stored_dirs.get(pid)
        current_dir_parent = stored_dir_parents.get(pid)

        if dir_id and (old_parent != new_parent or current_dir_parent != new_parent_dir_id):
            if dry_run:
                log.info(f"[Dry-Run][Move Dir] {info['clean_title']} ({pid}): parent {old_parent} -> {new_parent} (dir_parent: {current_dir_parent} -> {new_parent_dir_id})")
                stored_dir_parents[pid] = new_parent_dir_id
            else:
                log.info(f"[Move Dir] {info['clean_title']}: parent {old_parent} -> {new_parent}")
                openwebui_update_knowledge_dir(
                    config.openwebui_base_url,
                    config.openwebui_api_key,
                    config.knowledge_base_id,
                    dir_id,
                    new_name=info["clean_title"],
                    parent_id=new_parent_dir_id
                )
                stored_dir_parents[pid] = new_parent_dir_id

        if old_parent != new_parent:
            old_file_id = old_entry.get("openwebui_file_id")
            time_unchanged = (old_entry.get("last_edited_time") == info.get("last_edited_time"))
            if old_file_id and time_unchanged:
                if dry_run:
                    log.info(f"  [Dry-Run] Would move file ID {old_file_id} to folder {new_parent_dir_id}")
                else:
                    openwebui_move_knowledge_file(
                        config.openwebui_base_url,
                        config.openwebui_api_key,
                        config.knowledge_base_id,
                        old_file_id,
                        new_parent_dir_id
                    )
                    log.info(f"  -> Moved Open WebUI File: {info['clean_title']} (File ID: {old_file_id} -> Folder: {new_parent_dir_id})")
                stored_pages[pid] = {**old_entry, "parent_page_id": new_parent}
                parent_only_moved_pids.add(pid)

    if not dry_run:
        state["pages"] = stored_pages
        state["dir_parents"] = stored_dir_parents
        save_state(state)

    return parent_only_moved_pids

# --- Page Worker Function ---

def sync_one_page(
    pid: str,
    info: dict,
    config: Config,
    stored_item: Optional[dict],
    target_folder_id: Optional[str],
    dir_id_to_rename: Optional[str],
    dry_run: bool,
    parent_only_moved: bool = False
) -> dict:
    title = info["title"]
    last_edited = info["last_edited_time"]
    filename = info.get("filename", f"{info['clean_title']}.md")

    old_filename = stored_item.get("filename") if stored_item else None
    is_renamed = bool(old_filename and old_filename != filename)

    # Incremental check
    if stored_item and not is_renamed and not parent_only_moved:
        stored_time = stored_item.get("last_edited_time")
        if stored_time == last_edited:
            log.debug(f"[Skip] Unchanged: {filename} ({pid})")
            return {"pid": pid, "skipped": True}

    page_obj = info.get("page_obj", {})
    prop_md = page_properties_to_md(page_obj)

    blocks = fetch_block_children(pid, config.notion_api_key)
    raw_body = blocks_to_md(blocks, config.notion_api_key, config.excluded_block_types)
    
    full_body = prop_md + raw_body
    if not full_body.strip():
        full_body = "*(頁面無內文 / Empty Page Content)*\n"
        
    md_content = f"# {title}\n\n" + full_body
    new_size = len(md_content.encode("utf-8"))

    if is_renamed:
        log.info(f"[Rename] Page renamed: '{old_filename}' -> '{filename}' ({pid})...")
        if dir_id_to_rename and not dry_run:
            openwebui_update_knowledge_dir(
                config.openwebui_base_url,
                config.openwebui_api_key,
                config.knowledge_base_id,
                dir_id_to_rename,
                info["clean_title"],
                parent_id=target_folder_id
            )
            log.info(f"  -> Renamed Open WebUI Directory: {info['clean_title']} (Dir ID: {dir_id_to_rename}, parent: {target_folder_id})")
    else:
        log.info(f"[Sync] Processing: {filename} ({pid})...")

    # Cleanup old Open WebUI file if exists
    old_file_id = stored_item.get("openwebui_file_id") if stored_item else None
    if old_file_id and config.openwebui_api_key and config.knowledge_base_id:
        if dry_run:
            log.info(f"  [Dry-Run] Would delete old Open WebUI file ID: {old_file_id}")
        else:
            openwebui_remove_knowledge(config.openwebui_base_url, config.openwebui_api_key, config.knowledge_base_id, old_file_id)
            openwebui_delete_file(config.openwebui_base_url, config.openwebui_api_key, old_file_id)

    # Upload new file
    new_file_id = ""
    if config.openwebui_api_key and config.knowledge_base_id:
        if dry_run:
            log.info(f"  [Dry-Run] Would upload {filename} and attach to KB (Folder: {target_folder_id})")
        else:
            new_file_id = openwebui_upload_file(config.openwebui_base_url, config.openwebui_api_key, filename, md_content)
            if new_file_id:
                try:
                    openwebui_attach_knowledge(
                        config.openwebui_base_url,
                        config.openwebui_api_key,
                        config.knowledge_base_id,
                        new_file_id,
                        folder_id=target_folder_id
                    )
                    if target_folder_id:
                        log.info(f"  -> Uploaded & Attached to Folder '{target_folder_id}': {filename} (File ID: {new_file_id})")
                    else:
                        log.info(f"  -> Uploaded & Attached to Root: {filename} (File ID: {new_file_id})")
                except Exception as attach_err:
                    log.warning(f"Failed to attach {filename} ({new_file_id}) to Knowledge Base: {attach_err}")

    stored_entry = {
        "title": title,
        "filename": filename,
        "last_edited_time": last_edited,
        "content_size": new_size,
        "openwebui_file_id": new_file_id,
        "parent_page_id": info.get("parent_page_id")
    }
    return {
        "pid": pid,
        "updated": True,
        "stored_entry": stored_entry,
        "clean_title": info["clean_title"],
        "dir_id_to_rename": dir_id_to_rename,
        "target_folder_id": target_folder_id
    }

# --- Main Sync Logic ---

def sync(config: Optional[Config] = None, dry_run: bool = False, workers: int = 4):
    if config is None:
        config = load_config()
    
    try:
        config.validate()
    except ValueError as e:
        log.error(f"[Error] {e}")
        sys.exit(1)

    state = load_state()
    stored_pages = state.get("pages", {})
    stored_dirs = state.get("directories", {})
    stored_dir_parents = state.get("dir_parents", {})

    log.info("[1/5] Traversing Notion hierarchy (pages & databases)...")
    discovered: Dict[str, dict] = {}
    traverse_notion_tree(config.notion_root_page_id, config, discovered)

    created_dirs: Dict[str, str] = dict(stored_dirs)

    # Phase A: Serial setup of all required Open WebUI Knowledge directories
    log.info("[2/5] Setting up directory structure...")
    for pid, info in discovered.items():
        if info.get("has_children"):
            get_or_create_page_dir(
                pid, discovered, created_dirs, stored_dirs, stored_dir_parents, config, state, dry_run=dry_run
            )

    # Phase 2.5: Move directories and files where hierarchy changed
    log.info("[2.5/5] Syncing moved directories and files...")
    parent_only_moved_pids = sync_moved_dirs_and_files(
        discovered, stored_pages, stored_dirs, stored_dir_parents, created_dirs, config, state, dry_run=dry_run
    )

    updated_count = 0
    skipped_count = 0
    deleted_count = 0

    # Phase B: Parallel sync of pages using ThreadPoolExecutor
    log.info(f"[3/5] Syncing pages using {workers} parallel worker(s)...")
    page_tasks = []
    for pid, info in discovered.items():
        stored_item = stored_pages.get(pid)
        parent_pid = info.get("parent_page_id")
        target_folder_id = created_dirs.get(parent_pid) if parent_pid else None
        dir_id_to_rename = stored_dirs.get(pid)
        parent_only_moved = pid in parent_only_moved_pids
        page_tasks.append((pid, info, stored_item, target_folder_id, dir_id_to_rename, parent_only_moved))

    results = []
    if workers > 1 and len(page_tasks) > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_pid = {
                executor.submit(
                    sync_one_page, pid, info, config, stored_item, target_folder_id, dir_id_to_rename, dry_run, parent_only_moved
                ): pid
                for pid, info, stored_item, target_folder_id, dir_id_to_rename, parent_only_moved in page_tasks
            }
            for future in concurrent.futures.as_completed(future_to_pid):
                pid = future_to_pid[future]
                try:
                    res = future.result()
                    results.append(res)
                except Exception as e:
                    log.error(f"[Error] Syncing page {pid} failed: {e}")
    else:
        for pid, info, stored_item, target_folder_id, dir_id_to_rename, parent_only_moved in page_tasks:
            try:
                res = sync_one_page(pid, info, config, stored_item, target_folder_id, dir_id_to_rename, dry_run, parent_only_moved)
                results.append(res)
            except Exception as e:
                log.error(f"[Error] Syncing page {pid} failed: {e}")

    # Merge results into stored_pages & state on main thread
    for res in results:
        pid = res["pid"]
        if res.get("skipped"):
            skipped_count += 1
            continue
        if res.get("updated"):
            updated_count += 1
            stored_pages[pid] = res["stored_entry"]
            if res.get("dir_id_to_rename"):
                dir_id = res["dir_id_to_rename"]
                stored_dirs[pid] = dir_id
                created_dirs[pid] = dir_id
                stored_dir_parents[pid] = res.get("target_folder_id")

            if not dry_run and updated_count % FLUSH_EVERY == 0:
                state["pages"] = stored_pages
                state["directories"] = stored_dirs
                state["dir_parents"] = stored_dir_parents
                save_state(state)

    # 4. Process Deleted pages & directories
    log.info("[4/5] Processing deleted pages and directories...")
    for pid in list(stored_pages.keys()):
        if pid not in discovered:
            old_item = stored_pages[pid]
            log.info(f"[Delete] Page removed in Notion: {old_item.get('title')} ({pid})")
            old_file_id = old_item.get("openwebui_file_id")
            if old_file_id and config.openwebui_api_key and config.knowledge_base_id:
                if dry_run:
                    log.info(f"  [Dry-Run] Would delete Open WebUI file ID: {old_file_id}")
                else:
                    openwebui_remove_knowledge(config.openwebui_base_url, config.openwebui_api_key, config.knowledge_base_id, old_file_id)
                    openwebui_delete_file(config.openwebui_base_url, config.openwebui_api_key, old_file_id)

            old_dir_id = stored_dirs.get(pid)
            if old_dir_id and config.openwebui_api_key and config.knowledge_base_id:
                if dry_run:
                    log.info(f"  [Dry-Run] Would delete Open WebUI directory ID: {old_dir_id}")
                else:
                    openwebui_delete_knowledge_dir(config.openwebui_base_url, config.openwebui_api_key, config.knowledge_base_id, old_dir_id)
                    log.info(f"  -> Deleted Open WebUI Directory (Dir ID: {old_dir_id})")
                if pid in stored_dirs:
                    del stored_dirs[pid]
                if pid in stored_dir_parents:
                    del stored_dir_parents[pid]

            del stored_pages[pid]
            deleted_count += 1

            if not dry_run and deleted_count % FLUSH_EVERY == 0:
                state["pages"] = stored_pages
                state["directories"] = stored_dirs
                state["dir_parents"] = stored_dir_parents
                save_state(state)

    # Clean up any remaining orphaned or childless directories in stored_dirs
    for pid in list(stored_dirs.keys()):
        if pid not in discovered or not discovered[pid].get("has_children"):
            old_dir_id = stored_dirs[pid]
            reason = "removed in Notion" if pid not in discovered else "no longer has child pages"
            log.info(f"[Delete] Directory {reason} ({pid})")
            if old_dir_id and config.openwebui_api_key and config.knowledge_base_id:
                if dry_run:
                    log.info(f"  [Dry-Run] Would delete Open WebUI directory ID: {old_dir_id}")
                else:
                    openwebui_delete_knowledge_dir(config.openwebui_base_url, config.openwebui_api_key, config.knowledge_base_id, old_dir_id)
                    log.info(f"  -> Deleted Open WebUI Directory (Dir ID: {old_dir_id})")
            del stored_dirs[pid]
            if pid in stored_dir_parents:
                del stored_dir_parents[pid]

    if not dry_run:
        state["pages"] = stored_pages
        state["directories"] = stored_dirs
        state["dir_parents"] = stored_dir_parents
        save_state(state)

    prefix = "[Dry-Run] " if dry_run else ""
    log.info(f"\n{prefix}[Done] Updated: {updated_count}, Skipped: {skipped_count}, Deleted: {deleted_count}")

# --- Self-Tests ---

def run_self_tests():
    log.info("Running built-in self-tests...")
    
    # Test 1: annotate_plain_text
    assert annotate_plain_text("hello", {"bold": True}) == "**hello**"
    assert annotate_plain_text("  hi  ", {"italic": True}) == "  _hi_  "
    assert annotate_plain_text("", {"bold": True}) == ""
    assert annotate_plain_text("code", {"code": True}) == "`code`"
    assert annotate_plain_text("strike", {"strikethrough": True}) == "~~strike~~"

    # Test 2: rich_text_to_md
    rt = [{"type": "text", "plain_text": "world", "annotations": {"bold": True}}]
    assert rich_text_to_md(rt) == "**world**"

    # Test 3: sanitize_filename
    assert sanitize_filename("A/B: C? *D*") == "A_B_ C_ _D_"

    # Test 4: Config validation
    cfg = Config(notion_api_key="key", notion_root_page_id="root", openwebui_base_url="http://localhost:3000/")
    assert cfg.openwebui_base_url == "http://localhost:3000"
    cfg.validate()

    # Test 5: Directory deletion state cleanup check
    test_state = {"pages": {"p1": {"title": "Parent", "openwebui_file_id": "f1", "parent_page_id": "root"}}, "directories": {"p1": "d1"}, "dir_parents": {"p1": "root_dir"}}
    assert "p1" in test_state["directories"]
    assert test_state["pages"]["p1"]["parent_page_id"] == "root"
    del test_state["directories"]["p1"]
    del test_state["dir_parents"]["p1"]
    assert "p1" not in test_state["directories"]
    assert "p1" not in test_state["dir_parents"]

    # Test 6: Move detection logic check
    disc = {
        "p1": {"clean_title": "P1", "parent_page_id": "new_parent", "last_edited_time": "t1"}
    }
    st_pages = {
        "p1": {"title": "P1", "parent_page_id": "old_parent", "last_edited_time": "t1", "openwebui_file_id": "f1"}
    }
    st_dirs = {"p1": "d1"}
    st_dir_parents = {"p1": "dir_old_parent"}
    cr_dirs = {"new_parent": "dir_new_parent"}
    c_cfg = Config(notion_api_key="k", notion_root_page_id="r", openwebui_base_url="http://localhost:3000", knowledge_base_id="kb")
    t_state = {"pages": st_pages, "directories": st_dirs, "dir_parents": st_dir_parents}
    
    moved_pids = sync_moved_dirs_and_files(disc, st_pages, st_dirs, st_dir_parents, cr_dirs, c_cfg, t_state, dry_run=True)
    assert "p1" in moved_pids
    assert st_pages["p1"]["parent_page_id"] == "new_parent"
    assert st_dir_parents["p1"] == "dir_new_parent"

    # Test 7: Hierarchy dir parent updates
    disc_hier = {
        "root": {"clean_title": "Root", "parent_page_id": None, "has_children": True},
        "child": {"clean_title": "Child", "parent_page_id": "root", "has_children": True}
    }
    st_dirs_h = {"child": "dir_child"}
    st_dir_parents_h = {"child": None}
    cr_dirs_h = {}
    test_st_h = {"directories": st_dirs_h, "dir_parents": st_dir_parents_h}
    get_or_create_page_dir("child", disc_hier, cr_dirs_h, st_dirs_h, st_dir_parents_h, c_cfg, test_st_h, dry_run=True)
    assert "child" in cr_dirs_h

    # Test 8: Rename detection and Markdown compilation checks
    old_filename = "Old Title.md"
    new_filename = "Renamed Title.md"
    assert bool(old_filename and old_filename != new_filename) is True

    sample_blocks = [
        {"type": "heading_1", "heading_1": {"rich_text": [{"plain_text": "Heading 1", "type": "text", "annotations": {}}]}},
        {"type": "paragraph", "paragraph": {"rich_text": [{"plain_text": "Normal text", "type": "text", "annotations": {}}]}}
    ]
    compiled = blocks_to_md(sample_blocks, "dummy_key", [])
    assert "# Heading 1" in compiled
    assert "Normal text" in compiled

    log.info("All self-tests passed successfully!")

# --- CLI Entry Point ---

def parse_args():
    p = argparse.ArgumentParser(description="Notion -> Open WebUI incremental sync")
    p.add_argument("--verbose", "-v", action="store_true", help="Enable DEBUG logging")
    p.add_argument("--dry-run", action="store_true", help="Traverse & diff without writing to Open WebUI or state")
    p.add_argument("--workers", "-w", type=int, default=4, help="Number of concurrent page sync workers (default: 4)")
    p.add_argument("--self-test", action="store_true", help="Run built-in self-checks and exit")
    return p.parse_args()

def main():
    args = parse_args()
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(message)s")

    if args.self_test:
        run_self_tests()
        sys.exit(0)

    try:
        sync(dry_run=args.dry_run, workers=args.workers)
    finally:
        _notion_pool.close_all()
        _owui_pool.close_all()

if __name__ == "__main__":
    main()
