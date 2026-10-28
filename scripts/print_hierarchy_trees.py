import json
import os
import sys
import urllib.request

# Support UTF-8 output on Windows terminals
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

def resolve_path(filename: str) -> str:
    for candidate in [filename, os.path.join(ROOT_DIR, filename), os.path.join(BASE_DIR, filename)]:
        if os.path.exists(candidate):
            return os.path.abspath(candidate)
    return os.path.join(ROOT_DIR, filename)

def load_config():
    cfg_path = os.getenv("CONFIG_FILE", resolve_path("config.json"))
    if os.path.exists(cfg_path):
        with open(cfg_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "notion_api_key": os.getenv("NOTION_API_KEY", ""),
        "notion_root_page_id": os.getenv("NOTION_ROOT_PAGE_ID", ""),
        "openwebui_base_url": os.getenv("OPENWEBUI_BASE_URL", "http://localhost:3000"),
        "openwebui_api_key": os.getenv("OPENWEBUI_API_KEY", ""),
        "knowledge_base_id": os.getenv("KNOWLEDGE_BASE_ID", "")
    }

config = load_config()

# --- Part 1: Notion Hierarchy ---

notion_api_key = config.get("notion_api_key", "")
notion_root_id = config.get("notion_root_page_id", "").replace("-", "")

def notion_req(endpoint, method="GET", payload=None):
    url = "https://api.notion.com/v1/" + endpoint.lstrip("/")
    headers = {
        "Authorization": f"Bearer {notion_api_key}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    data = json.dumps(payload).encode("utf-8") if payload else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))

def get_page_title(page):
    for p in page.get("properties", {}).values():
        if p.get("type") == "title":
            title_objs = p.get("title", [])
            if title_objs:
                return title_objs[0].get("plain_text", "Untitled")
    return "Untitled"

def get_block_children(block_id):
    blocks = []
    cursor = None
    while True:
        ep = f"blocks/{block_id}/children" + (f"?start_cursor={cursor}" if cursor else "")
        res = notion_req(ep)
        blocks.extend(res.get("results", []))
        if not res.get("has_more"):
            break
        cursor = res.get("next_cursor")
    return blocks

def query_db_pages(db_id):
    pages = []
    cursor = None
    while True:
        payload = {"start_cursor": cursor} if cursor else {}
        res = notion_req(f"databases/{db_id}/query", method="POST", payload=payload)
        for item in res.get("results", []):
            pages.append(item.get("id"))
        if not res.get("has_more"):
            break
        cursor = res.get("next_cursor")
    return pages

def scan_blocks(block_id):
    items = []
    for b in get_block_children(block_id):
        btype = b.get("type")
        if btype == "child_page":
            items.append({"type": "page", "id": b.get("id")})
        elif btype == "child_database":
            items.append({"type": "database", "id": b.get("id")})
        elif b.get("has_children") and btype not in ("synced_block", "child_page", "child_database"):
            items.extend(scan_blocks(b.get("id")))
    return items

def print_notion_tree_node(page_id, prefix=""):
    pid_clean = page_id.replace("-", "")
    page = notion_req(f"pages/{pid_clean}")
    title = get_page_title(page)
    print(f"{prefix}📄 {title} ({pid_clean})")
    
    children = scan_blocks(pid_clean)
    for i, child in enumerate(children):
        is_last = (i == len(children) - 1)
        child_prefix = prefix + ("└── " if is_last else "├── ")
        next_prefix = prefix + ("    " if is_last else "│   ")
        if child["type"] == "page":
            print_notion_tree_node(child["id"], child_prefix)
        elif child["type"] == "database":
            db_pages = query_db_pages(child["id"])
            print(f"{child_prefix}🗄️ Database ({child['id']})")
            for j, db_pid in enumerate(db_pages):
                db_last = (j == len(db_pages) - 1)
                db_c_prefix = next_prefix + ("└── " if db_last else "├── ")
                print_notion_tree_node(db_pid, db_c_prefix)

def print_notion_hierarchy():
    print("=========================================")
    print("      Notion Hierarchy Tree              ")
    print("=========================================")
    if not notion_api_key or not notion_root_id:
        print("Error: Missing notion_api_key or notion_root_page_id in config.json / ENV.")
        return
    try:
        print_notion_tree_node(notion_root_id)
    except Exception as e:
        print(f"Error fetching Notion tree: {e}")

# --- Part 2: Open WebUI Hierarchy ---

owui_base_url = config.get("openwebui_base_url", "http://localhost:3000").rstrip("/")
owui_api_key = config.get("openwebui_api_key", "")
owui_kb_id = config.get("knowledge_base_id", "")

def owui_req(endpoint, method="GET", payload=None):
    url = owui_base_url + "/api/v1/" + endpoint.lstrip("/")
    headers = {"Authorization": "Bearer " + owui_api_key}
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode("utf-8")
    else:
        data = None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))

def fetch_files_for_dir(dir_id=""):
    res = owui_req(f"knowledge/{owui_kb_id}/files?directory_id={dir_id}")
    items = res.get("items", res.get("files", []))
    files = []
    for f in items:
        name = f.get("meta", {}).get("name") or f.get("filename", "")
        if name:
            files.append(name)
    return files

def build_owui_tree_from_api():
    diff = owui_req(f"knowledge/{owui_kb_id}/sync/diff", method="POST", payload={"manifest": []})
    dir_map = diff.get("directory_map", {})
    
    root_files = fetch_files_for_dir(dir_id="")
    tree = {"_files": root_files}
    
    for path, dir_id in dir_map.items():
        dir_files = fetch_files_for_dir(dir_id=dir_id)
        parts = path.split("/")
        curr = tree
        for p in parts:
            if p not in curr:
                curr[p] = {"_files": []}
            curr = curr[p]
        curr["_files"] = dir_files
        
    return tree

def build_owui_tree_from_state():
    state_file = os.getenv("STATE_FILE", resolve_path("sync_state.json"))
    with open(state_file, "r", encoding="utf-8") as f:
        st = json.load(f)
    pages = st.get("pages", {})
    dirs = st.get("directories", {})
    
    nodes = {}
    for pid, pdata in pages.items():
        title = pdata.get("title", pid)
        filename = pdata.get("filename", f"{title}.md")
        nodes[pid] = {
            "title": title,
            "filename": filename,
            "parent": pdata.get("parent_page_id"),
            "is_dir": pid in dirs,
            "children": []
        }
    
    for pid, node in nodes.items():
        parent_id = node["parent"]
        if parent_id and parent_id in nodes:
            nodes[parent_id]["children"].append(node)
            
    def convert(node):
        res = {"_files": []}
        for c in node["children"]:
            if c["is_dir"]:
                res[c["title"]] = convert(c)
            else:
                res["_files"].append(c["filename"])
        return res
        
    tree = {"_files": []}
    for pid, node in nodes.items():
        if not node["parent"] or node["parent"] not in nodes:
            if node["is_dir"]:
                tree[node["title"]] = convert(node)
            else:
                tree["_files"].append(node["filename"])
    return tree

def print_owui_tree_node(node, prefix=""):
    dirs = [k for k in node.keys() if k != "_files"]
    files = node.get("_files", [])
    items = [(d, True) for d in dirs] + [(f, False) for f in files]
    
    for i, (name, is_dir) in enumerate(items):
        is_last = (i == len(items) - 1)
        c_prefix = prefix + ("└── " if is_last else "├── ")
        n_prefix = prefix + ("    " if is_last else "│   ")
        
        if is_dir:
            print(f"{c_prefix}📁 {name}/")
            print_owui_tree_node(node[name], n_prefix)
        else:
            print(f"{c_prefix}📄 {name}")

def print_openwebui_hierarchy():
    print("=========================================")
    print(f"   Open WebUI Knowledge Base Tree ({owui_kb_id}) ")
    print("=========================================")
    if not owui_base_url or not owui_kb_id:
        print("Error: Missing openwebui_base_url or knowledge_base_id in config.json / ENV.")
        return
    try:
        tree = build_owui_tree_from_api()
    except Exception as e:
        print(f"API query failed ({e}). Reading local sync_state.json...")
        state_file = os.getenv("STATE_FILE", resolve_path("sync_state.json"))
        if os.path.exists(state_file):
            tree = build_owui_tree_from_state()
        else:
            print(f"Error: Could not fetch from API and {state_file} does not exist.")
            return

    print_owui_tree_node(tree)

# --- Main Entry Point ---

def main():
    print_notion_hierarchy()
    print()
    print_openwebui_hierarchy()

if __name__ == "__main__":
    main()
