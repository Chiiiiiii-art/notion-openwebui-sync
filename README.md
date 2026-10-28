# Notion → Open WebUI Incremental Sync Tool

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Dependencies](https://img.shields.io/badge/Dependencies-Zero%20(Stdlib)-success.svg)](#)

輕量化 Python 自動化工具，將 Notion 頁面階層增量同步至 Open WebUI Knowledge Base（知識庫）。  
**零第三方套件依賴**（純 Python 3.9+ 標準函式庫）。

---

## 🌟 核心特色

1. **增量狀態比對 (Incremental Sync)**
   - 透過 `sync_state.json` 追蹤頁面 ID 與 `last_edited_time`。
   - 僅對新增、修改或移動的頁面發動請求與編譯，大幅節省 API 額度與時間。
   - 自動追蹤刪除：Notion 刪除的頁面，自動自 Open WebUI 知識庫移除對應檔案。
2. **完整保留目錄樹結構 (Hierarchy Mirroring)**
   - 遞迴解析 Notion 父子頁面關係。
   - 在 Open WebUI Knowledge Base 中自動建立對應的多層目錄（Knowledge Directory），保持文件組織一致。
3. **高品質 Markdown 編譯**
   - 完整支援標題、段落、清單、待辦清單、引文、代碼區塊、表格、Callout 及 Synced Block（同步區塊）。
   - 支援黑名單過濾規則（標題前綴如 `[Draft]`、特定 Page ID 或 Block 類型）。
4. **穩健的網路請求與重試機制**
   - 內建指數退避重試（Exponential Backoff）與 HTTP 429 `Retry-After` 自動等待處理。
   - 支援 `--dry-run` 模擬演練，不更動遠端資料。
   - 內建連線池（Connection Pool）與多執行緒平行處理（`--workers`）。

---

## 🚀 快速開始

### 1. 環境需求
- Python 3.9 或以上版本
- 無需 `pip install` 任何套件（純標準庫）

### 2. 設定檔配置

複製範例設定檔：
```bash
cp config.example.json config.json
```

編輯 `config.json`，填入連線金鑰與 ID：
```json
{
  "notion_api_key": "your-notion-api-key",
  "notion_root_page_id": "your-notion-root-page-id",
  "openwebui_base_url": "http://localhost:3000",
  "openwebui_api_key": "your-openwebui-api-key",
  "knowledge_base_id": "your-knowledge-base-id",
  "blacklist_title_prefixes": ["[Draft]"],
  "blacklist_title_suffixes": [],
  "blacklist_page_ids": [],
  "excluded_block_types": []
}
```

> **提示**：亦支援以環境變數帶入（優先順序：`config.json` > 環境變數）：
> - `NOTION_API_KEY`
> - `NOTION_ROOT_PAGE_ID`
> - `OPENWEBUI_BASE_URL`
> - `OPENWEBUI_API_KEY`
> - `KNOWLEDGE_BASE_ID`

---

## 💻 執行同步

### 基本同步
```bash
python scripts/sync.py
```

### 命令列選項 (CLI Arguments)
```bash
# 乾跑模擬 (Dry-run)：檢查樹狀結構與異動，不發送寫入請求、不變更 state
python scripts/sync.py --dry-run

# 開啟詳細 Debug Log
python scripts/sync.py --verbose

# 指定並行 Worker 數量 (預設 4)
python scripts/sync.py --workers 8

# 執行內建自我檢測 (離線 unit test)
python scripts/sync.py --self-test
```

### 檢視結構樹工具
```bash
# 輸出 Notion 與 Open WebUI 目前的樹狀階層結構
python scripts/print_hierarchy_trees.py
```

---

## ⏰ 定時排程範例

### Linux / macOS (crontab)
每小時執行一次：
```cron
0 * * * * cd /path/to/notion-openwebui-sync && python3 scripts/sync.py >> sync.log 2>&1
```

### Windows 工作排程器 (Task Scheduler)
- **動作**：啟動程式
- **程式/指令碼**：`python`
- **引數**：`scripts/sync.py`
- **開始於**：專案根目錄路徑

---

## 📁 專案結構

```
notion-openwebui-sync/
├── config.example.json         # 設定檔範本 (不含機密資訊)
├── LICENSE                     # MIT 授權條款
├── README.md                   # 專案說明文件
├── .gitignore                  # Git 忽略設定 (已包含 config.json, sync_state.json, *.key 等)
└── scripts/
    ├── sync.py                 # 主要增量同步程式
    └── print_hierarchy_trees.py# 階層樹檢視輔助工具
```

---

## 📄 授權條款

本專案採用 [MIT License](LICENSE) 授權。
