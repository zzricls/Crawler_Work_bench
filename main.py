"""Crawler Workbench · 爬虫工作台（单文件版）
本代码为原先多文件融合版
原本分散在多个目录里的模块，全部融合进了这一个 main.py：

    原 core/logger.py            →  ① 基础设施 · 日志系统
    原 core/database.py          →  ② 虚拟磁盘 · SQLite 底座
    原 core/file_system.py       →  ③ 虚拟磁盘 · 文件操作封装
    原 core/anti_crawl.py        →  ④ 反爬工具 · 请求头与智能重试
    原 core/guides.py            →  ⑤ 爬虫说明文档 · 内置攻略
    原 core/scrapy_generator.py  →  ⑥ Scrapy 代码生成器
    原 ui/apps/image_viewer.py   →  ⑦ 图片查看器
    原 ui/apps/music_player.py   →  ⑧ 音乐播放器
    原 ui/apps/video_player.py   →  ⑨ 视频播放
    原 ui/apps/file_manager.py   →  ⑩ 图形化文件管理器
    原 ui/apps/zip_tool.py       →  ⑪ 压缩 / 解压工具
    原 ui/apps/crawler_requests.py → ⑫ requests + bs4 经典爬虫
    原 ui/apps/crawler_regex.py  →  ⑬ 正则专用爬虫
    原 ui/apps/crawler_async.py  →  ⑭ aiohttp 异步高并发爬虫
    原 ui/apps/crawler_browser.py → ⑮ Playwright 浏览器爬虫
    原 ui/apps/crawler_scrapy.py →  ⑯ Scrapy 高级爬虫（沙盒运行）
    原 ui/terminal.py            →  ⑰ 主终端

合并后所有模块共享同一个命名空间，原先的 `from core import file_system as fs`
之类的内部导入全部去掉，`fs.list_dir(...)` 直接写成 `list_dir(...)`；同名的模块级
变量（logger、UA_OPTIONS、PROXIES 等）按章节加了前缀，避免互相覆盖。

依赖：requests / beautifulsoup4 / pillow / pygame / aiohttp / scrapy / playwright
"""


# ---------------------------------------------------------------------------
# 标准库
# ---------------------------------------------------------------------------
import os
import io
import re
import sys
import json
import time
import queue
import random
import shutil
import sqlite3
import asyncio
import logging
import zipfile
import tempfile
import threading
import subprocess
from datetime import datetime
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# 特殊模式：作为 Scrapy 运行器（子进程分支）
#
# 这一段必须放在 tkinter / pygame 等界面依赖之前：Scrapy 子进程只需要跑爬虫，
# 不该为了一个命令行任务把整套 GUI 依赖都加载一遍（缺依赖时会直接崩掉）。
# ---------------------------------------------------------------------------
if "--scrapy-runner" in sys.argv:
    # 去掉自定义参数，还原成 scrapy 能识别的 argv
    args = [a for a in sys.argv[1:] if a != "--scrapy-runner"]
    sys.argv = ["scrapy"] + args

    # 强制 stdout/stderr 无缓冲 + UTF-8，保证日志实时传回主进程
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", line_buffering=True)
    except Exception:
        pass

    try:
        import scrapy.cmdline
        scrapy.cmdline.execute()
        sys.exit(0)
    except Exception as e:
        print(f"[scrapy-runner] ERROR: {e}", flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)


# ---------------------------------------------------------------------------
# 第三方库
# ---------------------------------------------------------------------------
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog, scrolledtext

import requests
import aiohttp
import pygame
from bs4 import BeautifulSoup
from PIL import (Image, ImageTk)


# ---------------------------------------------------------------------------
# ① 基础设施 · 日志系统    ← 原 core/logger.py
# ---------------------------------------------------------------------------

def get_base_dir():
    """项目根目录：打包后取 exe 所在目录，源码运行时就是本文件所在目录。"""
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


LOG_DIR = os.path.join(get_base_dir(), "logs")
os.makedirs(LOG_DIR, exist_ok=True)


def get_logger(name="app"):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    log_file = os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


# ---------------------------------------------------------------------------
# ② 虚拟磁盘 · SQLite 底座    ← 原 core/database.py
# ---------------------------------------------------------------------------

DB_PATH = os.path.join(get_base_dir(), "data", "disk.db")


def get_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            content TEXT,
            blob_data BLOB,
            parent_id INTEGER DEFAULT 0,
            is_dir INTEGER DEFAULT 0,
            created TEXT,
            modified TEXT
        )
    """)
    conn.commit()
    conn.close()


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ==================== 创建 ====================

def create_folder(name, parent_id=0):
    conn = get_conn()
    cursor = conn.cursor()
    now = _now()
    cursor.execute("""
        INSERT INTO files (name, content, parent_id, is_dir, created, modified)
        VALUES (?, NULL, ?, 1, ?, ?)
    """, (name, parent_id, now, now))
    new_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return new_id


def create_file(name, content="", parent_id=0):
    conn = get_conn()
    cursor = conn.cursor()
    now = _now()
    cursor.execute("""
        INSERT INTO files (name, content, blob_data, parent_id, is_dir, created, modified)
        VALUES (?, ?, NULL, ?, 0, ?, ?)
    """, (name, content, parent_id, now, now))
    new_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return new_id


def create_binary_file(name, blob_data, parent_id=0):
    """创建二进制文件（图片、视频等）"""
    conn = get_conn()
    cursor = conn.cursor()
    now = _now()
    cursor.execute("""
        INSERT INTO files (name, content, blob_data, parent_id, is_dir, created, modified)
        VALUES (?, NULL, ?, ?, 0, ?, ?)
    """, (name, blob_data, parent_id, now, now))
    new_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return new_id


# ==================== 查询 ====================

def get_item(item_id):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM files WHERE id = ?", (item_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def list_items(parent_id=0):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM files WHERE parent_id = ?
        ORDER BY is_dir DESC, name ASC
    """, (parent_id,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def find_by_name(name, parent_id=0):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM files WHERE parent_id = ? AND name = ?
    """, (parent_id, name))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


# ==================== 修改 ====================

def rename_item(item_id, new_name):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("UPDATE files SET name = ?, modified = ? WHERE id = ?",
                   (new_name, _now(), item_id))
    conn.commit()
    conn.close()


def write_file_content(item_id, content):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("UPDATE files SET content = ?, modified = ? WHERE id = ?",
                   (content, _now(), item_id))
    conn.commit()
    conn.close()


# ==================== 删除 ====================

def _collect_ids(item_id):
    ids = [item_id]
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM files WHERE parent_id = ?", (item_id,))
    for row in cursor.fetchall():
        ids.extend(_collect_ids(row["id"]))
    conn.close()
    return ids


def delete_item(item_id):
    ids = _collect_ids(item_id)
    conn = get_conn()
    cursor = conn.cursor()
    for i in ids:
        cursor.execute("DELETE FROM files WHERE id = ?", (i,))
    conn.commit()
    conn.close()


# 初始化
init_db()


# ---------------------------------------------------------------------------
# ③ 虚拟磁盘 · 文件操作封装    ← 原 core/file_system.py
# ---------------------------------------------------------------------------

ROOT_ID = 0

# 文本类扩展名（存进 content 字段）
TEXT_EXTS = {".txt", ".md", ".py", ".json", ".csv", ".log",
             ".html", ".css", ".js", ".xml", ".ini", ".yaml", ".yml"}


def list_dir(parent_id=ROOT_ID):
    return list_items(parent_id)


def new_folder(name, parent_id=ROOT_ID):
    return create_folder(name, parent_id)


def new_file(name, content="", parent_id=ROOT_ID):
    return create_file(name, content, parent_id)


def get_info(item_id):
    return get_item(item_id)


def find(name, parent_id=ROOT_ID):
    return find_by_name(name, parent_id)


def rename(item_id, new_name):
    rename_item(item_id, new_name)


def remove(item_id):
    delete_item(item_id)


def read_file(item_id):
    item = get_item(item_id)
    if item and not item["is_dir"]:
        return item["content"] or ""
    return ""


def write_file(item_id, content):
    write_file_content(item_id, content)


# ==================== 导入/导出（真实磁盘 <-> 虚拟磁盘） ====================

def import_file(real_path, parent_id=ROOT_ID):
    """把真实电脑上的文件导入虚拟磁盘"""
    if not os.path.isfile(real_path):
        return None
    name = os.path.basename(real_path)

    if find(name, parent_id):
        base, ext = os.path.splitext(name)
        name = f"{base}_导入{ext}"

    ext = os.path.splitext(name)[1].lower()

    try:
        if ext in TEXT_EXTS:
            with open(real_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
            return create_file(name, content, parent_id)
        else:
            with open(real_path, "rb") as f:
                blob = f.read()
            return create_binary_file(name, blob, parent_id)
    except Exception as e:
        print(f"导入失败: {real_path}, {e}")
        return None


def import_folder(real_path, parent_id=ROOT_ID):
    """把真实电脑上的文件夹（递归）导入虚拟磁盘"""
    if not os.path.isdir(real_path):
        return None
    folder_name = os.path.basename(real_path) or "导入文件夹"

    if find(folder_name, parent_id):
        folder_name = folder_name + "_导入"

    new_folder_id = new_folder(folder_name, parent_id)

    for item in os.listdir(real_path):
        item_path = os.path.join(real_path, item)
        if os.path.isdir(item_path):
            import_folder(item_path, new_folder_id)
        else:
            import_file(item_path, new_folder_id)

    return new_folder_id


def export_file(item_id, real_path):
    """把虚拟磁盘文件导出到真实磁盘"""
    item = get_item(item_id)
    if not item or item["is_dir"]:
        return False
    try:
        if item["blob_data"]:
            with open(real_path, "wb") as f:
                f.write(item["blob_data"])
        else:
            with open(real_path, "w", encoding="utf-8") as f:
                f.write(item["content"] or "")
        return True
    except Exception as e:
        print(f"导出失败: {real_path}, {e}")
        return False


def is_text_file(item):
    """判断虚拟磁盘文件是否为文本文件"""
    if not item or item["is_dir"]:
        return False
    return item["blob_data"] is None


# ---------------------------------------------------------------------------
# ④ 反爬工具 · 请求头与智能重试    ← 原 core/anti_crawl.py
# ---------------------------------------------------------------------------

# ===== UA 池 =====
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
]


def get_headers(url=None, user_agent=None, cookie=None, referer=None):
    """
    生成一份完整的浏览器请求头
    - url: 目标 URL（用于自动生成 Referer）
    - user_agent: 指定 UA；None 或 "RANDOM" 表示随机
    - cookie: 手动 Cookie 字符串
    - referer: 手动 Referer；None 表示从 url 自动生成
    """
    if not user_agent or user_agent == "RANDOM":
        user_agent = random.choice(USER_AGENTS)

    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    }

    # Referer
    if referer:
        headers["Referer"] = referer
    elif url:
        try:
            p = urlparse(url)
            headers["Referer"] = f"{p.scheme}://{p.netloc}/"
        except Exception:
            pass

    # Cookie
    if cookie:
        headers["Cookie"] = cookie

    return headers


def request_with_retry(session, method, url, headers, proxies=None, timeout=15,
                       max_retries=3, log_fn=None):
    """
    带智能重试的请求
    - 200: 成功
    - 429: 等待后重试
    - 403/401: 换 UA 后重试
    - 超时/连接错误: 等待后重试
    返回 (success: bool, response_or_error_str)
    """
    import requests as req_lib

    last_error = ""
    for attempt in range(max_retries):
        try:
            if log_fn and attempt > 0:
                log_fn(f"[重试 {attempt}/{max_retries - 1}] {url}")

            requester = session if session else req_lib
            if method.upper() == "GET":
                resp = requester.get(url, headers=headers, proxies=proxies, timeout=timeout)
            else:
                resp = requester.post(url, headers=headers, proxies=proxies, timeout=timeout)

            if resp.status_code == 200:
                return True, resp

            if resp.status_code == 429:
                wait = 5 * (attempt + 1)
                if log_fn:
                    log_fn(f"⚠️ HTTP 429（请求过多），等待 {wait}s 后重试")
                time.sleep(wait)
                last_error = "HTTP 429"
                continue

            if resp.status_code in (403, 401):
                if log_fn:
                    log_fn(f"⚠️ HTTP {resp.status_code}，更换 UA 重试")
                headers["User-Agent"] = random.choice(USER_AGENTS)
                time.sleep(2)
                last_error = f"HTTP {resp.status_code}"
                continue

            # 其他状态码直接返回（由调用方判断）
            return True, resp

        except req_lib.exceptions.Timeout:
            if log_fn:
                log_fn("⏱️ 请求超时，重试中...")
            last_error = "请求超时"
            time.sleep(2)

        except req_lib.exceptions.ConnectionError:
            if log_fn:
                log_fn("❌ 连接错误，重试中...")
            last_error = "连接错误"
            time.sleep(2)

        except Exception as e:
            if log_fn:
                log_fn(f"❌ 异常：{str(e)[:60]}")
            last_error = str(e)[:100]
            time.sleep(1)

    return False, f"重试 {max_retries} 次后仍失败：{last_error}"


# ---------------------------------------------------------------------------
# ⑤ 爬虫说明文档 · 内置攻略    ← 原 core/guides.py
# ---------------------------------------------------------------------------

# 每个爬虫的详细说明
GUIDES = {
    "crawler": {
        "title": "🕷️ requests + BeautifulSoup 经典爬虫",
        "scenario": "适用于【静态页面】，HTML 源码里直接包含数据。",
        "params": [
            ("目标 URL", "要抓取的页面地址"),
            ("请求方法", "GET（默认）或 POST"),
            ("User-Agent", "可选 6 种内置 UA，或自定义"),
            ("Referer", "留空自动生成，也可手动填"),
            ("Cookie", "可选，需要登录态时手动粘贴"),
            ("代理", "内置 Clash/V2Ray 常用端口，或自定义"),
            ("启用 Session", "保持 Cookies，登录态用"),
            ("自动重试", "429 等待、403 换 UA 重试"),
            ("提取器", "CSS 选择器 / XPath / 正则"),
            ("提取模式", "文本 / HTML / 属性值"),
            ("属性名", "当提取模式选属性值时使用"),
            ("请求间隔", "0~5 秒，随机延迟"),
        ],
        "examples": [
            ("抓名言文本", "URL: quotes.toscrape.com | CSS: .quote .text | 文本"),
            ("抓所有链接", "URL: 任意页面 | CSS: a | 属性值 | href"),
            ("抓所有 div", "URL: 任意页面 | 正则: div | HTML"),
            ("抓邮箱", "URL: 任意页面 | 正则: [\\w.-]+@[\\w.-]+\\.\\w+ | 文本"),
        ],
        "pitfalls": [
            "· 正则模式匹配的是【标签名/属性/文本】三合一，不是纯文本",
            "· CSS 选择器里用 .class 或 #id，不是直接写 class 名",
            "· 遇到 Cloudflare 等强反爬，这个窗口搞不定，换 browser",
        ],
    },

    "regex": {
        "title": "🔤 正则专用爬虫",
        "scenario": "适用于【快速抓取散落文本】，不做 DOM 解析，纯正则匹配。",
        "params": [
            ("目标 URL", "要抓取的页面地址"),
            ("User-Agent", "同上，6 种内置 + 自定义"),
            ("Referer", "留空自动生成"),
            ("Cookie", "可选"),
            ("代理", "同上"),
            ("自动重试", "同上"),
            ("正则模板", "内置 10 种常用模板（邮箱/手机/URL等）"),
            ("正则表达式", "你的匹配模式"),
            ("匹配模式", "全匹配（findall）/ 带上下文（前后 60 字符）"),
            ("结果去重", "相同结果只保留一次"),
            ("请求间隔", "0~5 秒"),
        ],
        "examples": [
            ("抓邮箱", "模板选'邮箱'，匹配模式'全匹配'"),
            ("抓手机号", "模板选'手机号'"),
            ("抓所有链接", "模板选'所有 a 标签'"),
            ("带上下文匹配", "表达式: 关键词 | 匹配模式: 带上下文"),
        ],
        "pitfalls": [
            "· 正则匹配的是【源码字符串】，不是 DOM 节点",
            "· 输入 div 会匹配源码里的 'div' 四个字母，不是 div 节点",
            "· 要提取节点，请用 crawler 窗口（三合一正则）",
            "· 带上下文模式会截取前后各 60 字符，适合人工核对",
        ],
    },

    "async": {
        "title": "⚡ aiohttp 异步高并发爬虫",
        "scenario": "适用于【批量 URL 并发抓取】，速度极快，适合 I/O 密集型任务。",
        "params": [
            ("URL 列表", "每行一个 URL，支持几十上百个"),
            ("并发数", "1~50，同时请求的协程数（推荐 10~20）"),
            ("提取器", "CSS / XPath / 正则 / 不提取"),
            ("提取模式", "文本 / HTML / 属性值"),
            ("User-Agent", "同上"),
            ("代理", "同上"),
            ("请求间隔", "每个请求前的随机延迟"),
        ],
        "examples": [
            ("批量抓名言", "URL 列表填 3~5 个 quotes 页面 | CSS: .quote .text | 文本"),
            ("批量测速", "URL 列表填 20 个不同站点 | 不提取 | 看响应时间"),
            ("批量抓标题", "URL 列表填多个页面 | CSS: title | 文本"),
        ],
        "pitfalls": [
            "· 并发数别超 50，否则容易被封 IP",
            "· 结果区先显示每个 URL 的状态，全部完成后清屏显示提取结果",
            "· 单条提取结果超 300 字符会截断显示（保存时是完整的）",
        ],
    },

    "scrapy": {
        "title": "🕸️ Scrapy 高级爬虫（沙盒运行）",
        "scenario": "适用于【大规模、多页面、自动翻页】的爬取。填表单 → 生成代码 → 沙盒运行。",
        "params": [
            ("项目名", "英文名，用于生成 spider 类名"),
            ("起始 URL", "爬虫的起点"),
            ("允许域名", "限制在哪个站内爬（可留空）"),
            ("列表容器选择器", "每个条目的 CSS 选择器，如 .quote"),
            ("字段配置", "可增删行：字段名 / 选择器 / 模式 / 属性名"),
            ("翻页选择器", "下一页链接的 CSS，如 .next a::attr(href)"),
        ],
        "examples": [
            ("抓名言（默认配置）", "URL: quotes.toscrape.com | 容器: .quote | 字段: text/author/tags"),
            ("抓文章列表", "容器: article | 字段: title/链接/时间"),
            ("不翻页", "翻页选择器留空即可"),
        ],
        "pitfalls": [
            "· 字段选择器是【相对选择器】，相对于列表容器",
            "· 提取文本用 ::text，提取属性用 ::attr(属性名)",
            "· 生成的代码在 temp_scrapy/ 里，运行完会自动清理",
            "· 首次运行可能慢（Scrapy 编译内部组件），第二次就快了",
        ],
    },

    "browser": {
        "title": "🌐 Playwright 浏览器爬虫",
        "scenario": "适用于【JS 动态渲染页面】，requests 和 scrapy 都抓不到的，它都能抓到。",
        "params": [
            ("目标 URL", "要抓取的页面地址"),
            ("无头模式", "勾选=后台跑（快）；不勾=弹窗显示（调试用）"),
            ("加载策略", "DOM 加载完 / 网络空闲 / 页面完全加载"),
            ("等待选择器", "等某个元素出现后才继续，推荐填数据区 class"),
            ("额外等待", "0~10 秒，让 JS 跑完"),
            ("滚动到底部", "0~20 次，触发懒加载"),
            ("点击某元素", "进入页面后先点一下的 CSS 选择器"),
            ("截图", "勾选后保存整页截图到虚拟磁盘"),
            ("提取器", "CSS / XPath / 正则 / 不提取"),
            ("提取模式", "文本 / HTML / 属性值"),
        ],
        "examples": [
            ("抓 JS 页面", "URL: quotes.toscrape.com/js/ | 等 .quote | CSS: .quote .text | 文本"),
            ("抓无限滚动图库", "滚动 8 次 | CSS: img | 属性值 | src"),
            ("抓需要点击的页面", "点击: button.load-more | 额外等待 2s"),
            ("保存页面截图", "勾选截图 | 起名 page.png"),
        ],
        "pitfalls": [
            "· 速度比 requests 慢，每页 3~10 秒，这是渲染的代价",
            "· 首次使用会自动启动浏览器，可能需要几秒",
            "· 遇到 Cloudflare 等强反爬可能弹验证码，也搞不定",
            "· 打包时会带上 Chromium 内核（约 150MB），注意体积",
        ],
    },
}


def get_overview():
    """获取所有爬虫的概览"""
    lines = []
    for cmd, data in GUIDES.items():
        lines.append(f"  {cmd:<10} {data['title']}")
        lines.append(f"             {data['scenario']}")
        lines.append("")
    return "\n".join(lines)


def get_guide(cmd):
    """获取某个爬虫的详细说明"""
    if cmd not in GUIDES:
        return None
    data = GUIDES[cmd]

    lines = []
    lines.append("=" * 70)
    lines.append(data["title"])
    lines.append("=" * 70)
    lines.append("")
    lines.append("📌 适用场景")
    lines.append(f"  {data['scenario']}")
    lines.append("")

    lines.append("⚙️ 参数说明")
    for name, desc in data["params"]:
        lines.append(f"  · {name:<12} {desc}")
    lines.append("")

    lines.append("💡 使用示例")
    for title, example in data["examples"]:
        lines.append(f"  ▸ {title}")
        lines.append(f"    {example}")
    lines.append("")

    lines.append("⚠️ 常见坑")
    for pitfall in data["pitfalls"]:
        lines.append(f"  {pitfall}")
    lines.append("")
    lines.append("=" * 70)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# ⑥ Scrapy 代码生成器    ← 原 core/scrapy_generator.py
# ---------------------------------------------------------------------------

def _sanitize_name(name):
    name = re.sub(r"[^\w]", "_", name)
    if not name or name[0].isdigit():
        name = "spider_" + name
    return name.lower()


def _build_field_line(field):
    var_name = _sanitize_name(field["name"])
    selector = field["selector"].strip()
    mode = field["mode"]
    attr = field.get("attr", "href").strip()

    if mode == "text":
        full = selector if "::" in selector else selector + "::text"
        expr = f'item.css("{full}").get()'
    elif mode == "attr":
        full = selector if "::attr(" in selector else f'{selector}::attr({attr})'
        expr = f'item.css("{full}").get()'
    else:
        expr = f'item.css("{selector}").get()'
    return f'                "{var_name}": {expr},'


def generate_spider(config):
    spider_name = _sanitize_name(config["project_name"])
    start_url = config["start_url"].strip()
    allowed = config.get("allowed_domain", "").strip()
    list_sel = config["list_selector"].strip()
    fields = config["fields"]
    next_sel = config.get("next_selector", "").strip()

    field_lines = []
    for f in fields:
        if f["name"] and f["selector"]:
            field_lines.append(_build_field_line(f))
    fields_code = "\n".join(field_lines) if field_lines else '                "raw": str(item),'

    if next_sel:
        pagination = f'''
        next_page = response.css("{next_sel}").get()
        if next_page:
            yield response.follow(next_page, self.parse)
'''
    else:
        pagination = ""

    allowed_line = f'    allowed_domains = ["{allowed}"]' if allowed else '    allowed_domains = []'

    code = f'''"""自动生成的 Scrapy 爬虫 - {config["project_name"]}"""
import random
import scrapy
from urllib.parse import urlparse

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


class {spider_name.title().replace("_", "")}Spider(scrapy.Spider):
    name = "{spider_name}"
    start_urls = ["{start_url}"]
{allowed_line}

    custom_settings = {{
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 1.0,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "LOG_LEVEL": "INFO",
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504, 522, 524, 408, 403],
        "DOWNLOAD_TIMEOUT": 20,
        "COOKIES_ENABLED": True,
    }}

    def parse(self, response):
        items = response.css("{list_sel}")
        self.logger.info("本页找到 %d 个条目", len(items))

        for item in items:
            yield {{
{fields_code}
            }}
{pagination}
    def start_requests(self):
        for url in self.start_urls:
            headers = {{
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Referer": "{{}}://{{}}/".format(*urlparse(url)[:2]),
            }}
            yield scrapy.Request(url, headers=headers, callback=self.parse)
'''

    return code


def write_spider(sandbox_dir, config):
    os.makedirs(sandbox_dir, exist_ok=True)
    code = generate_spider(config)
    path = os.path.join(sandbox_dir, "spider.py")
    with open(path, "w", encoding="utf-8") as f:
        f.write(code)
    return path, code


# ---------------------------------------------------------------------------
# ⑦ 图片查看器    ← 原 ui/apps/image_viewer.py
# ---------------------------------------------------------------------------

logger_image = get_logger("image_viewer")


class ImageViewerWindow:
    def __init__(self, parent, file_id):
        self.parent = parent
        item = get_info(file_id)
        if not item:
            messagebox.showerror("错误", "文件不存在", parent=parent)
            return

        self.item = item
        self.win = tk.Toplevel(parent)
        self.win.title(f"图片查看 - {item['name']}")
        self.win.geometry("800x620")
        self.win.configure(bg="#1e1e1e")
        self.win.attributes("-topmost", True)

        self.original_image = None
        self.current_photo = None

        self.build_ui()
        self.load_image()
        logger_image.info(f"图片查看器打开: {item['name']}")

    def build_ui(self):
        # 顶部信息
        top = tk.Frame(self.win, bg="#1e1e1e")
        top.pack(fill="x", padx=10, pady=10)

        tk.Label(top, text=f"🖼️ {self.item['name']}", font=("微软雅黑", 12, "bold"),
                 fg="white", bg="#1e1e1e").pack(side="left")

        tk.Button(top, text="🔍 适应窗口", command=self.fit_to_window,
                  bg="#3498db", fg="white", relief="flat",
                  font=("微软雅黑", 10)).pack(side="right", padx=5)

        # 画布
        self.canvas = tk.Canvas(self.win, bg="#0c0c0c", highlightthickness=0)
        self.canvas.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.canvas.bind("<Configure>", lambda e: self.fit_to_window())

        # 状态栏
        self.status_label = tk.Label(self.win, text="", bg="#111111", fg="#aaaaaa",
                                     font=("微软雅黑", 9), anchor="w", padx=10)
        self.status_label.pack(side="bottom", fill="x")

    def load_image(self):
        try:
            blob = self.item.get("blob_data")
            if not blob:
                messagebox.showerror("错误", "该文件没有图片数据", parent=self.win)
                return
            self.original_image = Image.open(io.BytesIO(blob))
            w, h = self.original_image.size
            self.status_label.config(text=f"原始尺寸: {w} × {h}")
            self.fit_to_window()
        except Exception as e:
            messagebox.showerror("错误", f"图片加载失败：{e}", parent=self.win)
            logger_image.error(f"图片加载失败: {e}")

    def fit_to_window(self):
        if not self.original_image:
            return
        cw = self.canvas.winfo_width()
        ch = self.canvas.winfo_height()
        if cw <= 1 or ch <= 1:
            return

        iw, ih = self.original_image.size
        ratio = min(cw / iw, ch / ih, 1.0)
        new_w = max(1, int(iw * ratio))
        new_h = max(1, int(ih * ratio))

        resized = self.original_image.resize((new_w, new_h), Image.LANCZOS)
        self.current_photo = ImageTk.PhotoImage(resized)

        self.canvas.delete("all")
        self.canvas.create_image(cw // 2, ch // 2, image=self.current_photo, anchor="center")


# ---------------------------------------------------------------------------
# ⑧ 音乐播放器    ← 原 ui/apps/music_player.py
# ---------------------------------------------------------------------------

logger_music = get_logger("music_player")


class MusicPlayerWindow:
    def __init__(self, parent, file_id):
        self.parent = parent
        item = get_info(file_id)
        if not item:
            messagebox.showerror("错误", "文件不存在", parent=parent)
            return

        self.item = item
        self.temp_path = None
        self.is_paused = False

        self.win = tk.Toplevel(parent)
        self.win.title(f"音乐播放 - {item['name']}")
        self.win.geometry("480x320")
        self.win.configure(bg="#1e1e1e")
        self.win.attributes("-topmost", True)
        self.win.protocol("WM_DELETE_WINDOW", self.on_close)

        # 初始化 pygame 音频
        try:
            pygame.mixer.init()
        except Exception as e:
            messagebox.showerror("错误", f"音频系统初始化失败：{e}", parent=self.win)
            return

        self.build_ui()
        self.load_music()
        logger_music.info(f"音乐播放器打开: {item['name']}")

    def build_ui(self):
        # 顶部歌曲信息
        tk.Label(self.win, text="🎵", font=("Segoe UI Emoji", 40),
                 bg="#1e1e1e").pack(pady=(20, 5))
        tk.Label(self.win, text=self.item["name"], font=("微软雅黑", 13, "bold"),
                 fg="white", bg="#1e1e1e", wraplength=420).pack()

        # 控制按钮
        btn_frame = tk.Frame(self.win, bg="#1e1e1e")
        btn_frame.pack(pady=20)

        self.play_btn = tk.Button(btn_frame, text="▶️ 播放", command=self.play_music,
                                  bg="#4CAF50", fg="white", relief="flat",
                                  font=("微软雅黑", 11), width=10, height=1)
        self.play_btn.pack(side="left", padx=5)

        tk.Button(btn_frame, text="⏸️ 暂停", command=self.pause_music,
                  bg="#e67e22", fg="white", relief="flat",
                  font=("微软雅黑", 11), width=10, height=1).pack(side="left", padx=5)

        tk.Button(btn_frame, text="⏹️ 停止", command=self.stop_music,
                  bg="#e74c3c", fg="white", relief="flat",
                  font=("微软雅黑", 11), width=10, height=1).pack(side="left", padx=5)

        # 音量
        vol_frame = tk.Frame(self.win, bg="#1e1e1e")
        vol_frame.pack(pady=10)
        tk.Label(vol_frame, text="🔊 音量", fg="white", bg="#1e1e1e",
                 font=("微软雅黑", 10)).pack(side="left", padx=5)
        self.volume_scale = tk.Scale(vol_frame, from_=0.0, to=1.0, resolution=0.05,
                                     orient="horizontal", command=self.set_volume,
                                     bg="#1e1e1e", fg="white", highlightthickness=0,
                                     troughcolor="#2a2a2a", activebackground="#4CAF50",
                                     length=250)
        self.volume_scale.set(0.5)
        self.volume_scale.pack(side="left")

        self.status_label = tk.Label(self.win, text="准备就绪", bg="#111111",
                                     fg="#aaaaaa", font=("微软雅黑", 9), anchor="w", padx=10)
        self.status_label.pack(side="bottom", fill="x")

    def load_music(self):
        try:
            blob = self.item.get("blob_data")
            if not blob:
                messagebox.showerror("错误", "该文件没有音频数据", parent=self.win)
                return

            # 写入临时文件（pygame 不能直接读内存）
            ext = os.path.splitext(self.item["name"])[1] or ".mp3"
            fd, self.temp_path = tempfile.mkstemp(suffix=ext)
            with os.fdopen(fd, "wb") as f:
                f.write(blob)

            pygame.mixer.music.load(self.temp_path)
            pygame.mixer.music.set_volume(0.5)
            self.status_label.config(text="已就绪，点击播放")
        except Exception as e:
            messagebox.showerror("错误", f"音频加载失败：{e}", parent=self.win)
            logger_music.error(f"音频加载失败: {e}")

    def play_music(self):
        try:
            if self.is_paused:
                pygame.mixer.music.unpause()
                self.is_paused = False
                self.play_btn.config(text="▶️ 播放")
                self.status_label.config(text="播放中")
            else:
                pygame.mixer.music.play()
                self.status_label.config(text="播放中")
        except Exception as e:
            self.status_label.config(text=f"播放失败: {e}")

    def pause_music(self):
        try:
            pygame.mixer.music.pause()
            self.is_paused = True
            self.play_btn.config(text="▶️ 继续")
            self.status_label.config(text="已暂停")
        except Exception as e:
            self.status_label.config(text=f"暂停失败: {e}")

    def stop_music(self):
        try:
            pygame.mixer.music.stop()
            self.is_paused = False
            self.play_btn.config(text="▶️ 播放")
            self.status_label.config(text="已停止")
        except Exception as e:
            self.status_label.config(text=f"停止失败: {e}")

    def set_volume(self, val):
        try:
            pygame.mixer.music.set_volume(float(val))
        except Exception:
            pass

    def on_close(self):
        try:
            pygame.mixer.music.stop()
        except Exception:
            pass
        if self.temp_path and os.path.exists(self.temp_path):
            try:
                os.remove(self.temp_path)
            except Exception:
                pass
        self.win.destroy()


# ---------------------------------------------------------------------------
# ⑨ 视频播放（调用系统播放器）    ← 原 ui/apps/video_player.py
# ---------------------------------------------------------------------------

logger_video = get_logger("video_player")


def open_video(parent, file_id):
    """把虚拟磁盘里的视频导出到临时文件，用系统默认播放器打开"""
    item = get_info(file_id)
    if not item:
        messagebox.showerror("错误", "文件不存在", parent=parent)
        return

    blob = item.get("blob_data")
    if not blob:
        messagebox.showerror("错误", "该文件没有视频数据", parent=parent)
        return

    try:
        ext = os.path.splitext(item["name"])[1] or ".mp4"
        fd, temp_path = tempfile.mkstemp(suffix=ext, prefix="cw_video_")
        with os.fdopen(fd, "wb") as f:
            f.write(blob)

        # 用系统默认播放器打开
        os.startfile(temp_path)
        logger_video.info(f"视频已调用系统播放器: {item['name']} -> {temp_path}")

    except Exception as e:
        messagebox.showerror("错误", f"视频打开失败：{e}", parent=parent)
        logger_video.error(f"视频打开失败: {e}")


# ---------------------------------------------------------------------------
# ⑩ 图形化文件管理器    ← 原 ui/apps/file_manager.py
# ---------------------------------------------------------------------------

logger_files = get_logger("file_manager")


class FileManagerWindow:
    def __init__(self, parent):
        self.parent = parent
        self.win = tk.Toplevel(parent)
        self.win.title("文件管理器 - Crawler Workbench")
        self.win.geometry("820x560")
        self.win.configure(bg="#1e1e1e")
        self.win.attributes("-topmost", True)

        self.current_parent_id = 0
        self.path_stack = [0]
        self.selected_item_id = None

        self.build_ui()
        self.load_dir()
        logger_files.info("文件管理器启动")

    def build_ui(self):
        top = tk.Frame(self.win, bg="#1e1e1e")
        top.pack(fill="x", padx=10, pady=10)

        tk.Button(top, text="⬅ 返回上一级", command=self.go_back,
                  bg="#3498db", fg="white", relief="flat").pack(side="left", padx=5)
        tk.Button(top, text="🏠 根目录", command=self.go_root,
                  bg="#3498db", fg="white", relief="flat").pack(side="left", padx=5)

        self.path_label = tk.Label(top, text="/", font=("微软雅黑", 11),
                                   fg="#4CAF50", bg="#1e1e1e")
        self.path_label.pack(side="left", padx=15)

        table_frame = tk.Frame(self.win, bg="#1e1e1e")
        table_frame.pack(fill="both", expand=True, padx=10)

        columns = ("name", "type", "modified")
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", selectmode="browse")
        self.tree.heading("name", text="名称")
        self.tree.heading("type", text="类型")
        self.tree.heading("modified", text="修改时间")

        self.tree.column("name", width=360, anchor="w")
        self.tree.column("type", width=100, anchor="center")
        self.tree.column("modified", width=180, anchor="center")

        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        self.tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        self.tree.bind("<Double-Button-1>", self.on_double_click)
        self.tree.bind("<<TreeviewSelect>>", self.on_select)

        btn_frame = tk.Frame(self.win, bg="#1e1e1e")
        btn_frame.pack(fill="x", padx=10, pady=10)

        btn_style = {"bg": "#4CAF50", "fg": "white", "relief": "flat", "padx": 8, "pady": 3}
        tk.Button(btn_frame, text="新建文件夹", command=self.create_folder, **btn_style).pack(side="left", padx=4)
        tk.Button(btn_frame, text="新建文本", command=self.create_text, **btn_style).pack(side="left", padx=4)

        btn_style2 = {"bg": "#3498db", "fg": "white", "relief": "flat", "padx": 8, "pady": 3}
        tk.Button(btn_frame, text="导入文件", command=self.import_file, **btn_style2).pack(side="left", padx=4)
        tk.Button(btn_frame, text="导入文件夹", command=self.import_folder, **btn_style2).pack(side="left", padx=4)
        tk.Button(btn_frame, text="导出选中", command=self.export_selected, **btn_style2).pack(side="left", padx=4)

        tk.Button(btn_frame, text="重命名", command=self.rename_selected,
                  bg="#e67e22", fg="white", relief="flat", padx=8, pady=3).pack(side="right", padx=4)
        tk.Button(btn_frame, text="删除", command=self.delete_selected,
                  bg="#e74c3c", fg="white", relief="flat", padx=8, pady=3).pack(side="right", padx=4)

        self.status_label = tk.Label(self.win, text="", bg="#111111", fg="#aaaaaa",
                                     font=("微软雅黑", 9), anchor="w", padx=10)
        self.status_label.pack(side="bottom", fill="x")

    def load_dir(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        items = list_dir(self.current_parent_id)
        for item in items:
            icon = "📁" if item["is_dir"] else "📄"
            type_str = "文件夹" if item["is_dir"] else "文件"
            mod_time = item["modified"][:16] if item["modified"] else ""
            self.tree.insert("", "end", iid=str(item["id"]),
                             values=(f"{icon} {item['name']}", type_str, mod_time))

        self.path_label.config(text=self._get_path_string(self.current_parent_id))
        self.status_label.config(text=f"当前共 {len(items)} 个项目")

    def _get_path_string(self, item_id):
        parts = []
        current = item_id
        while current and current != 0:
            item = get_info(current)
            if not item:
                break
            parts.insert(0, item["name"])
            current = item["parent_id"]
        return "/" + "/".join(parts) if parts else "/"

    def on_select(self, event):
        sel = self.tree.selection()
        if sel:
            self.selected_item_id = int(sel[0])

    def on_double_click(self, event):
        if not self.selected_item_id:
            return
        item = get_info(self.selected_item_id)
        if not item:
            return

        # 文件夹：进入
        if item["is_dir"]:
            self.current_parent_id = item["id"]
            self.path_stack.append(item["id"])
            self.load_dir()
            return

        # 根据扩展名分发
        name = item["name"].lower()
        ext = os.path.splitext(name)[1]

        # 图片
        if ext in (".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp"):
            ImageViewerWindow(self.win, item["id"])

        # 音频
        elif ext in (".mp3", ".wav", ".ogg"):
            MusicPlayerWindow(self.win, item["id"])

        # 视频
        elif ext in (".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv"):
            open_video(self.win, item["id"])

        # 文本
        elif is_text_file(item):
            self.open_text_editor(item)

        # 其他：提示导出
        else:
            messagebox.showinfo("提示",
                                f"'{item['name']}' 是未知类型文件。\n"
                                "请使用【导出选中】把它导出到真实磁盘后再查看。",
                                parent=self.win)

    def go_back(self):
        if len(self.path_stack) > 1:
            self.path_stack.pop()
            self.current_parent_id = self.path_stack[-1]
            self.load_dir()
        else:
            self.go_root()

    def go_root(self):
        self.current_parent_id = 0
        self.path_stack = [0]
        self.load_dir()

    def create_folder(self):
        name = simpledialog.askstring("新建文件夹", "请输入文件夹名称：", parent=self.win)
        if name:
            name = name.strip()
            if not name:
                return
            if find(name, self.current_parent_id):
                messagebox.showwarning("提示", "已存在同名文件或文件夹", parent=self.win)
                return
            new_folder(name, self.current_parent_id)
            self.load_dir()

    def create_text(self):
        """创建文件，支持任意扩展名（若用户没写扩展名，才默认 .txt）"""
        name = simpledialog.askstring(
            "新建文件",
            "请输入文件名（可带任意扩展名，如 .py / .json / .md）：\n不写扩展名则默认 .txt",
            parent=self.win
        )
        if not name:
            return
        name = name.strip()
        if not name:
            return

        # 关键修复：只有当文件名完全没有扩展名时，才补 .txt
        root, ext = os.path.splitext(name)
        if not ext:
            name += ".txt"

        if find(name, self.current_parent_id):
            messagebox.showwarning("提示", "已存在同名文件或文件夹", parent=self.win)
            return
        new_file(name, "", self.current_parent_id)
        self.load_dir()

    def rename_selected(self):
        if not self.selected_item_id:
            messagebox.showwarning("提示", "请先选中一个项目", parent=self.win)
            return
        item = get_info(self.selected_item_id)
        if not item:
            return
        new_name = simpledialog.askstring("重命名", "请输入新名称：",
                                          initialvalue=item["name"], parent=self.win)
        if new_name and new_name.strip() and new_name.strip() != item["name"]:
            new_name = new_name.strip()
            if find(new_name, self.current_parent_id):
                messagebox.showwarning("提示", "该名称已存在", parent=self.win)
                return
            rename(self.selected_item_id, new_name)
            self.load_dir()

    def delete_selected(self):
        if not self.selected_item_id:
            messagebox.showwarning("提示", "请先选中一个项目", parent=self.win)
            return
        item = get_info(self.selected_item_id)
        if not item:
            return
        if messagebox.askyesno("确认", f"确定要删除 '{item['name']}' 吗？", parent=self.win):
            remove(self.selected_item_id)
            self.load_dir()

    def import_file(self):
        path = filedialog.askopenfilename(parent=self.win, title="选择要导入的文件")
        if not path:
            return
        new_id = import_file(path, self.current_parent_id)
        if new_id:
            self.load_dir()
            self.status_label.config(text="✅ 文件导入成功")
        else:
            messagebox.showerror("错误", "导入失败", parent=self.win)

    def import_folder(self):
        path = filedialog.askdirectory(parent=self.win, title="选择要导入的文件夹")
        if not path:
            return
        new_id = import_folder(path, self.current_parent_id)
        if new_id:
            self.load_dir()
            self.status_label.config(text="✅ 文件夹导入成功")
        else:
            messagebox.showerror("错误", "导入失败", parent=self.win)

    def export_selected(self):
        if not self.selected_item_id:
            messagebox.showwarning("提示", "请先选中一个文件", parent=self.win)
            return
        item = get_info(self.selected_item_id)
        if not item or item["is_dir"]:
            messagebox.showwarning("提示", "只能导出文件，不支持导出文件夹", parent=self.win)
            return

        path = filedialog.asksaveasfilename(parent=self.win, initialfile=item["name"],
                                            title="导出到真实磁盘")
        if not path:
            return
        if export_file(self.selected_item_id, path):
            self.status_label.config(text=f"✅ 已导出到: {path}")
            messagebox.showinfo("成功", "导出成功", parent=self.win)
        else:
            messagebox.showerror("错误", "导出失败", parent=self.win)

    def open_text_editor(self, item):
        edit_win = tk.Toplevel(self.win)
        edit_win.title(f"编辑 - {item['name']}")
        edit_win.geometry("700x520")
        edit_win.configure(bg="#1e1e1e")
        edit_win.attributes("-topmost", True)

        # ===== 底部状态栏 =====
        status = tk.Label(edit_win, text="按 Ctrl+S 保存", bg="#111111",
                          fg="#aaaaaa", font=("微软雅黑", 9), anchor="w", padx=10)
        status.pack(side="bottom", fill="x")

        # ===== 文本区 + 滚动条 =====
        text_frame = tk.Frame(edit_win, bg="#1e1e1e")
        text_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # 垂直滚动条
        scrollbar_y = tk.Scrollbar(text_frame, orient="vertical")
        scrollbar_y.pack(side="right", fill="y")

        # 水平滚动条（长行不折行时使用）
        scrollbar_x = tk.Scrollbar(text_frame, orient="horizontal")
        scrollbar_x.pack(side="bottom", fill="x")

        text = tk.Text(text_frame, font=("Consolas", 11),
                       bg="#0c0c0c", fg="#00ff00",
                       insertbackground="#00ff00", relief="flat",
                       wrap="none",
                       yscrollcommand=scrollbar_y.set,
                       xscrollcommand=scrollbar_x.set)
        text.pack(side="left", fill="both", expand=True)

        scrollbar_y.config(command=text.yview)
        scrollbar_x.config(command=text.xview)

        content = read_file(item["id"])
        text.insert("1.0", content)
        text.focus()

        def save(event=None):
            new_content = text.get("1.0", tk.END)
            write_file(item["id"], new_content)
            self.load_dir()
            status.config(text=f"✅ 已保存（{len(new_content)} 字符）")
            logger_files.info(f"保存文件: {item['name']}")

        # ===== 底部按钮 =====
        btn_frame = tk.Frame(edit_win, bg="#1e1e1e")
        btn_frame.pack(fill="x", padx=10, pady=(0, 10))
        tk.Button(btn_frame, text="💾 保存", command=save,
                  bg="#4CAF50", fg="white", relief="flat",
                  font=("微软雅黑", 11, "bold"), width=12).pack(side="left")
        tk.Button(btn_frame, text="关闭", command=edit_win.destroy,
                  bg="#888888", fg="white", relief="flat",
                  font=("微软雅黑", 11), width=12).pack(side="right")

        # 快捷键
        edit_win.bind("<Control-s>", save)

        def save(event=None):
            new_content = text.get("1.0", tk.END)
            write_file(item["id"], new_content)
            self.load_dir()
            status.config(text=f"✅ 已保存（{len(new_content)} 字符）")
            logger_files.info(f"保存文件: {item['name']}")

        btn_frame = tk.Frame(edit_win, bg="#1e1e1e")
        btn_frame.pack(fill="x", padx=10, pady=(0, 10))
        tk.Button(btn_frame, text="💾 保存", command=save,
                  bg="#4CAF50", fg="white", relief="flat",
                  font=("微软雅黑", 11, "bold"), width=12).pack(side="left")
        tk.Button(btn_frame, text="关闭", command=edit_win.destroy,
                  bg="#888888", fg="white", relief="flat",
                  font=("微软雅黑", 11), width=12).pack(side="right")

        edit_win.bind("<Control-s>", save)


# ---------------------------------------------------------------------------
# ⑪ 压缩 / 解压工具    ← 原 ui/apps/zip_tool.py
# ---------------------------------------------------------------------------

logger_zip = get_logger("zip_tool")


class ZipToolWindow:
    def __init__(self, parent):
        self.parent = parent
        self.win = tk.Toplevel(parent)
        self.win.title("压缩 / 解压 - Crawler Workbench")
        self.win.geometry("620x560")
        self.win.configure(bg="#1e1e1e")
        self.win.attributes("-topmost", True)

        self.cwd_id = 0
        self.selected_ids = []  # 支持多选

        self.build_ui()
        self.load_items()
        logger_zip.info("压缩/解压工具启动")

    def build_ui(self):
        tk.Label(self.win, text="📦 压缩 / 解压工具",
                 font=("微软雅黑", 16, "bold"),
                 fg="#4CAF50", bg="#1e1e1e").pack(pady=(15, 5))
        tk.Label(self.win, text="所有操作均在虚拟磁盘中进行 · 列表支持 Ctrl / Shift 多选",
                 font=("微软雅黑", 9), fg="#888888", bg="#1e1e1e").pack(pady=(0, 10))

        notebook = ttk.Notebook(self.win)
        notebook.pack(fill="x", padx=20, pady=5)

        tab_zip = tk.Frame(notebook, bg="#1e1e1e")
        notebook.add(tab_zip, text=" 压缩文件 ")
        self.build_zip_tab(tab_zip)

        tab_unzip = tk.Frame(notebook, bg="#1e1e1e")
        notebook.add(tab_unzip, text=" 解压文件 ")
        self.build_unzip_tab(tab_unzip)

        tk.Label(self.win, text="📁 当前目录内容（双击文件夹进入）",
                 font=("微软雅黑", 10), fg="#aaaaaa", bg="#1e1e1e").pack(anchor="w", padx=20, pady=(15, 5))

        nav_frame = tk.Frame(self.win, bg="#1e1e1e")
        nav_frame.pack(fill="x", padx=20)
        tk.Button(nav_frame, text="⬅ 返回上级", command=self.go_back,
                  bg="#3498db", fg="white", relief="flat",
                  font=("微软雅黑", 9)).pack(side="left")
        tk.Button(nav_frame, text="🏠 根目录", command=self.go_root,
                  bg="#3498db", fg="white", relief="flat",
                  font=("微软雅黑", 9)).pack(side="left", padx=5)
        self.path_label = tk.Label(nav_frame, text="/", fg="#4CAF50", bg="#1e1e1e",
                                   font=("微软雅黑", 10))
        self.path_label.pack(side="left", padx=10)

        list_frame = tk.Frame(self.win, bg="#1e1e1e")
        list_frame.pack(fill="both", expand=True, padx=20, pady=(5, 15))

        # ===== 关键：支持多选 =====
        self.listbox = tk.Listbox(list_frame, bg="#0c0c0c", fg="#00ff00",
                                  font=("Consolas", 10), selectbackground="#4CAF50",
                                  relief="flat", highlightthickness=0,
                                  selectmode="extended")
        self.listbox.pack(side="left", fill="both", expand=True)
        self.listbox.bind("<Double-Button-1>", self.on_double_click)
        self.listbox.bind("<<ListboxSelect>>", self.on_select)

        sb = tk.Scrollbar(list_frame, orient="vertical", command=self.listbox.yview)
        sb.pack(side="right", fill="y")
        self.listbox.config(yscrollcommand=sb.set)

        self.status_label = tk.Label(self.win, text="准备就绪", bg="#111111", fg="#aaaaaa",
                                     font=("微软雅黑", 9), anchor="w", padx=10)
        self.status_label.pack(side="bottom", fill="x")

    def build_zip_tab(self, parent):
        frame = tk.Frame(parent, bg="#1e1e1e")
        frame.pack(fill="x", padx=20, pady=15)

        tk.Label(frame, text="将选中的多个文件/文件夹压缩为一个 .zip",
                 font=("微软雅黑", 10), fg="#dddddd", bg="#1e1e1e").pack(anchor="w")
        tk.Label(frame, text="输入压缩包名称（含 .zip）：",
                 font=("微软雅黑", 10), fg="#dddddd", bg="#1e1e1e").pack(anchor="w", pady=(10, 3))

        self.zip_name_entry = tk.Entry(frame, font=("Consolas", 11),
                                       bg="#0c0c0c", fg="#00ff00",
                                       insertbackground="#00ff00", relief="flat")
        self.zip_name_entry.pack(fill="x", ipady=4)
        self.zip_name_entry.insert(0, "archive.zip")

        tk.Button(frame, text="⚡ 开始压缩", command=self.do_zip,
                  bg="#4CAF50", fg="white", relief="flat",
                  font=("微软雅黑", 11, "bold"), width=15).pack(pady=10)

    def build_unzip_tab(self, parent):
        frame = tk.Frame(parent, bg="#1e1e1e")
        frame.pack(fill="x", padx=20, pady=15)

        tk.Label(frame, text="将选中的 .zip 文件解压到当前目录",
                 font=("微软雅黑", 10), fg="#dddddd", bg="#1e1e1e").pack(anchor="w")
        tk.Label(frame, text="（解压时只取第一个选中的 zip 文件）",
                 font=("微软雅黑", 9), fg="#888888", bg="#1e1e1e").pack(anchor="w", pady=(10, 3))

        tk.Button(frame, text="⚡ 开始解压", command=self.do_unzip,
                  bg="#4CAF50", fg="white", relief="flat",
                  font=("微软雅黑", 11, "bold"), width=15).pack(pady=10)

    # ==================== 列表加载 ====================
    def load_items(self):
        self.listbox.delete(0, tk.END)
        self.items = list_dir(self.cwd_id)
        for item in self.items:
            icon = "📁" if item["is_dir"] else "📄"
            self.listbox.insert(tk.END, f"{icon}  {item['name']}")

        if self.cwd_id == 0:
            self.path_label.config(text="/")
        else:
            parts = []
            current = self.cwd_id
            while current and current != 0:
                it = get_info(current)
                if not it:
                    break
                parts.insert(0, it["name"])
                current = it["parent_id"]
            self.path_label.config(text="/" + "/".join(parts))

        self.selected_ids = []

    def on_select(self, event):
        """支持多选：把所有选中项 id 存进列表"""
        sel = self.listbox.curselection()
        self.selected_ids = []
        for idx in sel:
            if idx < len(self.items):
                self.selected_ids.append(self.items[idx]["id"])

    def on_double_click(self, event):
        """双击：如果是文件夹就进入"""
        sel = self.listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        if idx >= len(self.items):
            return
        item = self.items[idx]
        if item["is_dir"]:
            self.cwd_id = item["id"]
            self.load_items()

    def go_back(self):
        if self.cwd_id == 0:
            return
        item = get_info(self.cwd_id)
        if item:
            self.cwd_id = item["parent_id"]
            self.load_items()

    def go_root(self):
        self.cwd_id = 0
        self.load_items()

    # ==================== 压缩 ====================
    def do_zip(self):
        if not self.selected_ids:
            messagebox.showwarning("提示", "请先在下方列表中选择至少一个文件或文件夹", parent=self.win)
            return

        zip_name = self.zip_name_entry.get().strip()
        if not zip_name:
            messagebox.showwarning("提示", "请输入压缩包名称", parent=self.win)
            return
        if not zip_name.endswith(".zip"):
            zip_name += ".zip"

        if find(zip_name, self.cwd_id):
            messagebox.showwarning("提示", "当前目录已存在同名文件", parent=self.win)
            return

        try:
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                for item_id in self.selected_ids:
                    self._add_to_zip(zf, item_id, "")

            zip_bytes = buf.getvalue()
            create_binary_file(zip_name, zip_bytes, self.cwd_id)
            self.load_items()
            self.status_label.config(
                text=f"✅ 已压缩 {len(self.selected_ids)} 个项目到 {zip_name}"
                     f"（{len(zip_bytes)/1024:.1f} KB）")
            logger_zip.info(f"压缩成功: {zip_name}，共 {len(self.selected_ids)} 项")
        except Exception as e:
            messagebox.showerror("错误", f"压缩失败：{e}", parent=self.win)
            logger_zip.error(f"压缩失败: {e}")

    def _add_to_zip(self, zf, item_id, prefix):
        item = get_info(item_id)
        if not item:
            return
        arcname = os.path.join(prefix, item["name"]) if prefix else item["name"]

        if item["is_dir"]:
            if not list_dir(item_id):
                zf.writestr(arcname + "/", "")
            else:
                for child in list_dir(item_id):
                    self._add_to_zip(zf, child["id"], arcname)
        else:
            if item["blob_data"]:
                zf.writestr(arcname, item["blob_data"])
            else:
                zf.writestr(arcname, item["content"] or "")

    # ==================== 解压 ====================
    def do_unzip(self):
        if not self.selected_ids:
            messagebox.showwarning("提示", "请先在下方列表中选择一个 .zip 文件", parent=self.win)
            return

        # 只取第一个
        item = get_info(self.selected_ids[0])
        if not item:
            return

        if item["is_dir"]:
            messagebox.showwarning("提示", "选中的是文件夹，不是 .zip 文件", parent=self.win)
            return

        if not item["name"].lower().endswith(".zip"):
            messagebox.showwarning("提示", "选中的文件不是 .zip 格式", parent=self.win)
            return

        if not item["blob_data"]:
            messagebox.showwarning("提示", "该文件没有二进制数据（不是有效的 zip）", parent=self.win)
            return

        try:
            target_parent = self.cwd_id
            folder_name = item["name"][:-4]
            if find(folder_name, target_parent):
                folder_name += "_解压"

            extract_folder_id = new_folder(folder_name, target_parent)

            with zipfile.ZipFile(io.BytesIO(item["blob_data"]), "r") as zf:
                count = 0
                for name in zf.namelist():
                    if name.endswith("/"):
                        dir_name = name.rstrip("/").split("/")[-1]
                        if dir_name:
                            new_folder(dir_name, extract_folder_id)
                        continue
                    data = zf.read(name)
                    file_name = os.path.basename(name)
                    try:
                        text = data.decode("utf-8")
                        create_file(file_name, text, extract_folder_id)
                    except (UnicodeDecodeError, AttributeError):
                        create_binary_file(file_name, data, extract_folder_id)
                    count += 1

            self.load_items()
            self.status_label.config(text=f"✅ 已解压 {count} 个文件到文件夹 '{folder_name}'")
            logger_zip.info(f"解压成功: {item['name']} -> {folder_name}")
        except Exception as e:
            messagebox.showerror("错误", f"解压失败：{e}", parent=self.win)
            logger_zip.error(f"解压失败: {e}")


# ---------------------------------------------------------------------------
# ⑫ 经典爬虫窗口 · requests + bs4    ← 原 ui/apps/crawler_requests.py
# ---------------------------------------------------------------------------

logger_requests = get_logger("crawler_requests")

# 下拉用的 UA 名单
REQUESTS_UA_OPTIONS = {
    "随机": "RANDOM",
    "Chrome / Win10": USER_AGENTS[0],
    "Chrome / Win11": USER_AGENTS[1],
    "Chrome / Win (旧)": USER_AGENTS[2],
    "Safari / Mac": USER_AGENTS[3],
    "Firefox / Win10": USER_AGENTS[4],
    "Edge / Win10": USER_AGENTS[5],
    "自定义": None,
}

REQUESTS_PROXIES = {
    "不使用代理": "",
    "Clash (7890)": "http://127.0.0.1:7890",
    "V2Ray (10809)": "http://127.0.0.1:10809",
    "Shadowsocks (1080)": "socks5://127.0.0.1:1080",
    "自定义": None,
}


class CrawlerRequestsWindow:
    def __init__(self, parent):
        self.parent = parent
        self.win = tk.Toplevel(parent)
        self.win.title("requests 经典爬虫 - Crawler Workbench")
        self.win.geometry("1100x860")
        self.win.configure(bg="#1e1e1e")
        self.win.attributes("-topmost", True)

        self.is_running = False
        self.msg_queue = queue.Queue()
        self.current_result = ""
        self.session = requests.Session()

        self.build_ui()
        self.check_queue()
        logger_requests.info("requests 爬虫窗口启动")

    def build_ui(self):
        tk.Label(self.win, text="🕷️ requests + BeautifulSoup 经典爬虫",
                 font=("微软雅黑", 16, "bold"),
                 fg="#4CAF50", bg="#1e1e1e").pack(pady=(10, 5))
        tk.Label(self.win, text="静态页面 · Session · 代理 · Cookie · Referer · 自动重试",
                 font=("微软雅黑", 9), fg="#888888", bg="#1e1e1e").pack(pady=(0, 10))

        main = tk.Frame(self.win, bg="#1e1e1e")
        main.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        left = tk.Frame(main, bg="#1e1e1e", width=460)
        left.pack(side="left", fill="y", padx=(0, 5))
        left.pack_propagate(False)
        self._build_left(left)

        right = tk.Frame(main, bg="#1e1e1e")
        right.pack(side="right", fill="both", expand=True, padx=(5, 0))
        self._build_right(right)

        self.status_label = tk.Label(self.win, text="准备就绪", bg="#111111", fg="#aaaaaa",
                                     font=("微软雅黑", 9), anchor="w", padx=10)
        self.status_label.pack(side="bottom", fill="x")

    def _build_left(self, parent):
        # URL
        tk.Label(parent, text="🌐 目标 URL", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(5, 3))
        self.url_entry = tk.Entry(parent, font=("Consolas", 10),
                                  bg="#0c0c0c", fg="#00ff00",
                                  insertbackground="#00ff00", relief="flat")
        self.url_entry.pack(fill="x", ipady=4)
        self.url_entry.insert(0, "https://quotes.toscrape.com/")

        # 方法
        method_frame = tk.Frame(parent, bg="#1e1e1e")
        method_frame.pack(fill="x", pady=(8, 3))
        tk.Label(method_frame, text="请求方法：", fg="#aaaaaa", bg="#1e1e1e",
                 font=("微软雅黑", 10)).pack(side="left")
        self.method_var = tk.StringVar(value="GET")
        tk.Radiobutton(method_frame, text="GET", variable=self.method_var, value="GET",
                       bg="#1e1e1e", fg="white", selectcolor="#1e1e1e",
                       activebackground="#1e1e1e", activeforeground="white").pack(side="left")
        tk.Radiobutton(method_frame, text="POST", variable=self.method_var, value="POST",
                       bg="#1e1e1e", fg="white", selectcolor="#1e1e1e",
                       activebackground="#1e1e1e", activeforeground="white").pack(side="left")

        # UA 下拉
        tk.Label(parent, text="🎭 User-Agent", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.ua_var = tk.StringVar(value="随机")
        ua_combo = ttk.Combobox(parent, textvariable=self.ua_var,
                                values=list(REQUESTS_UA_OPTIONS.keys()),
                                state="readonly", width=40)
        ua_combo.pack(fill="x")
        ua_combo.bind("<<ComboboxSelected>>", self._on_ua_change)

        self.custom_ua_entry = tk.Entry(parent, font=("Consolas", 9),
                                        bg="#0c0c0c", fg="#00ff00",
                                        insertbackground="#00ff00", relief="flat")
        self.custom_ua_entry.insert(0, "自定义 UA")
        self.custom_ua_entry.pack_forget()

        # Referer
        tk.Label(parent, text="🔗 Referer（留空自动生成）", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.referer_entry = tk.Entry(parent, font=("Consolas", 10),
                                      bg="#0c0c0c", fg="#00ff00",
                                      insertbackground="#00ff00", relief="flat")
        self.referer_entry.pack(fill="x", ipady=4)
        self.referer_entry.insert(0, "")

        # Cookie
        tk.Label(parent, text="🍪 Cookie（可选，登录态用）", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.cookie_entry = tk.Entry(parent, font=("Consolas", 9),
                                     bg="#0c0c0c", fg="#00ff00",
                                     insertbackground="#00ff00", relief="flat")
        self.cookie_entry.pack(fill="x", ipady=4)

        # 代理
        tk.Label(parent, text="🌐 代理", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.proxy_var = tk.StringVar(value="不使用代理")
        ttk.Combobox(parent, textvariable=self.proxy_var,
                     values=list(REQUESTS_PROXIES.keys()),
                     state="readonly", width=40).pack(fill="x")

        # Session 开关
        self.use_session = tk.BooleanVar(value=True)
        tk.Checkbutton(parent, text="启用 Session（保持 Cookies）",
                       variable=self.use_session,
                       bg="#1e1e1e", fg="white", selectcolor="#1e1e1e",
                       activebackground="#1e1e1e", activeforeground="white").pack(anchor="w", pady=3)

        # 自动重试
        self.auto_retry = tk.BooleanVar(value=True)
        tk.Checkbutton(parent, text="自动重试（429/403 智能处理）",
                       variable=self.auto_retry,
                       bg="#1e1e1e", fg="white", selectcolor="#1e1e1e",
                       activebackground="#1e1e1e", activeforeground="white").pack(anchor="w", pady=3)

        # 提取器
        tk.Label(parent, text="🎯 提取器", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.extractor_var = tk.StringVar(value="css")
        ext_frame = tk.Frame(parent, bg="#1e1e1e")
        ext_frame.pack(fill="x")
        for text, val in [("CSS 选择器", "css"), ("XPath", "xpath"), ("正则", "regex")]:
            tk.Radiobutton(ext_frame, text=text, variable=self.extractor_var, value=val,
                           bg="#1e1e1e", fg="white", selectcolor="#1e1e1e",
                           activebackground="#1e1e1e", activeforeground="white",
                           command=self._on_extractor_change).pack(side="left", padx=3)

        self.pattern_entry = tk.Entry(parent, font=("Consolas", 10),
                                      bg="#0c0c0c", fg="#00ff00",
                                      insertbackground="#00ff00", relief="flat")
        self.pattern_entry.pack(fill="x", ipady=4, pady=(5, 0))
        self.pattern_entry.insert(0, ".quote .text")

        self.tip_label = tk.Label(parent, text="", font=("微软雅黑", 9),
                                  fg="#ffaa00", bg="#1e1e1e", justify="left", wraplength=440)
        self.tip_label.pack(anchor="w", pady=(3, 0))
        self._on_extractor_change()

        # 提取模式
        mode_frame = tk.Frame(parent, bg="#1e1e1e")
        mode_frame.pack(fill="x", pady=(5, 0))
        tk.Label(mode_frame, text="提取：", fg="#aaaaaa", bg="#1e1e1e",
                 font=("微软雅黑", 10)).pack(side="left")
        self.mode_var = tk.StringVar(value="text")
        for text, val in [("文本", "text"), ("HTML", "html"), ("属性值", "attr")]:
            tk.Radiobutton(mode_frame, text=text, variable=self.mode_var, value=val,
                           bg="#1e1e1e", fg="white", selectcolor="#1e1e1e",
                           activebackground="#1e1e1e", activeforeground="white").pack(side="left", padx=3)

        attr_frame = tk.Frame(parent, bg="#1e1e1e")
        attr_frame.pack(fill="x", pady=(3, 0))
        tk.Label(attr_frame, text="属性名：", fg="#aaaaaa", bg="#1e1e1e",
                 font=("微软雅黑", 10)).pack(side="left")
        self.attr_entry = tk.Entry(attr_frame, font=("Consolas", 10), width=15,
                                   bg="#0c0c0c", fg="#00ff00",
                                   insertbackground="#00ff00", relief="flat")
        self.attr_entry.pack(side="left", padx=5)
        self.attr_entry.insert(0, "href")

        # 延迟
        tk.Label(parent, text="⏱️ 请求间隔(秒)", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.delay_var = tk.DoubleVar(value=1.0)
        tk.Scale(parent, from_=0.0, to=5.0, resolution=0.5, orient="horizontal",
                 variable=self.delay_var, bg="#1e1e1e", fg="white",
                 highlightthickness=0, troughcolor="#2a2a2a",
                 activebackground="#4CAF50").pack(fill="x")

        # 按钮
        btn_frame = tk.Frame(parent, bg="#1e1e1e")
        btn_frame.pack(fill="x", pady=10)
        self.run_btn = tk.Button(btn_frame, text="🚀 开始爬取", command=self.start_crawl,
                                 bg="#4CAF50", fg="white", relief="flat",
                                 font=("微软雅黑", 12, "bold"), width=14)
        self.run_btn.pack(side="left", padx=3)
        tk.Button(btn_frame, text="💾 保存结果", command=self.save_result,
                  bg="#3498db", fg="white", relief="flat",
                  font=("微软雅黑", 11), width=12).pack(side="left", padx=3)

    def _build_right(self, parent):
        tk.Label(parent, text="📄 抓取结果", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(5, 3))
        text_frame = tk.Frame(parent, bg="#1e1e1e")
        text_frame.pack(fill="both", expand=True)
        self.result_text = tk.Text(text_frame, font=("Consolas", 10),
                                   bg="#0c0c0c", fg="#00ff00",
                                   insertbackground="#00ff00", relief="flat", wrap="word")
        self.result_text.pack(side="left", fill="both", expand=True)
        sb = tk.Scrollbar(text_frame, orient="vertical", command=self.result_text.yview)
        sb.pack(side="right", fill="y")
        self.result_text.config(yscrollcommand=sb.set)

    # ==================== 联动 ====================
    def _on_ua_change(self, event=None):
        if self.ua_var.get() == "自定义":
            self.custom_ua_entry.pack(fill="x", pady=(3, 0), ipady=3)
        else:
            self.custom_ua_entry.pack_forget()

    def _on_extractor_change(self):
        ext = self.extractor_var.get()
        if ext == "css":
            self.tip_label.config(text="💡 CSS：'div' 提取所有 div；'.quote .text' 提取嵌套元素；'a[href]' 提取带属性元素")
        elif ext == "xpath":
            self.tip_label.config(text="💡 XPath：'//div[@class=\"quote\"]' 提取节点；'//a/@href' 提取属性")
        else:
            self.tip_label.config(text="⚠️ 正则：匹配标签名/属性/文本三合一。输入 'div' 返回所有 div 节点。")

    # ==================== 主流程 ====================
    def start_crawl(self):
        url = self.url_entry.get().strip()
        pattern = self.pattern_entry.get().strip()
        if not url:
            messagebox.showwarning("提示", "请输入目标 URL", parent=self.win)
            return
        if not pattern:
            messagebox.showwarning("提示", "请输入提取表达式", parent=self.win)
            return
        if self.is_running:
            return

        self.is_running = True
        self.run_btn.config(state="disabled", text="爬取中...")
        self.status_label.config(text="正在爬取...")
        self.result_text.config(state="normal")
        self.result_text.delete("1.0", tk.END)
        self.result_text.config(state="disabled")

        # UA
        ua_key = self.ua_var.get()
        if ua_key == "自定义":
            ua = self.custom_ua_entry.get().strip()
        else:
            ua = REQUESTS_UA_OPTIONS[ua_key]

        config = {
            "url": url,
            "method": self.method_var.get(),
            "user_agent": ua,
            "referer": self.referer_entry.get().strip(),
            "cookie": self.cookie_entry.get().strip(),
            "proxy": REQUESTS_PROXIES[self.proxy_var.get()],
            "use_session": self.use_session.get(),
            "auto_retry": self.auto_retry.get(),
            "extractor": self.extractor_var.get(),
            "pattern": pattern,
            "mode": self.mode_var.get(),
            "attr": self.attr_entry.get().strip(),
            "delay": self.delay_var.get(),
        }

        threading.Thread(target=self._do_crawl, args=(config,), daemon=True).start()

    def _do_crawl(self, cfg):
        try:
            if cfg["delay"] > 0:
                time.sleep(random.uniform(cfg["delay"] * 0.8, cfg["delay"] * 1.2))

            # 完整请求头
            headers = get_headers(url=cfg["url"], user_agent=cfg["user_agent"],
                                  cookie=cfg["cookie"], referer=cfg["referer"])

            proxies = None
            if cfg["proxy"]:
                proxies = {"http": cfg["proxy"], "https": cfg["proxy"]}

            requester = self.session if cfg["use_session"] else None
            log_fn = lambda msg: self.msg_queue.put(("log", msg))

            if cfg["auto_retry"]:
                success, resp = request_with_retry(requester, cfg["method"], cfg["url"],
                                                   headers, proxies, timeout=15,
                                                   max_retries=3, log_fn=log_fn)
                if not success:
                    self.msg_queue.put(("log", f"❌ {resp}"))
                    self.msg_queue.put(("done", None))
                    return
            else:
                req = requester if requester else requests
                resp = req.get(cfg["url"], headers=headers, proxies=proxies, timeout=15)

            resp.encoding = resp.apparent_encoding or "utf-8"

            if resp.status_code != 200:
                self.msg_queue.put(("log", f"❌ HTTP {resp.status_code}"))
                self.msg_queue.put(("done", None))
                return

            self.msg_queue.put(("log", f"✅ 请求成功（{len(resp.text)} 字符），正在解析...\n"))

            results = self._extract(resp.text, cfg)
            if not results:
                self.msg_queue.put(("log", f"⚠️ 未匹配到内容：{cfg['pattern']}"))
                self.msg_queue.put(("done", None))
                return

            MAX_DISPLAY = 300
            PREVIEW_LEN = 150
            total = len(results)

            full_output = f"共匹配到 {total} 条结果：\n\n"
            for i, item in enumerate(results, 1):
                full_output += f"[{i}] {item}\n\n"

            preview = f"共匹配到 {total} 条结果"
            if total > MAX_DISPLAY:
                preview += f"（仅显示前 {MAX_DISPLAY} 条）"
            preview += "：\n\n"
            for i, item in enumerate(results[:MAX_DISPLAY], 1):
                s = str(item)
                if len(s) > PREVIEW_LEN:
                    s = s[:PREVIEW_LEN] + " ...[截断]"
                preview += f"[{i}] {s}\n\n"

            self.msg_queue.put(("log", preview))
            self.msg_queue.put(("done", full_output))
            logger_requests.info(f"爬取成功: {cfg['url']} 共{total}条")

        except Exception as e:
            self.msg_queue.put(("log", f"❌ 异常：{e}"))
            self.msg_queue.put(("done", None))
            logger_requests.error(f"爬取失败: {e}")

    def _extract(self, html, cfg):
        results = []
        mode = cfg["mode"]
        try:
            if cfg["extractor"] == "css":
                soup = BeautifulSoup(html, "lxml")
                for el in soup.select(cfg["pattern"]):
                    if mode == "text":
                        t = el.get_text(strip=True)
                        if t: results.append(t)
                    elif mode == "html":
                        results.append(str(el))
                    elif mode == "attr":
                        v = el.get(cfg["attr"], "")
                        if v: results.append(v)

            elif cfg["extractor"] == "xpath":
                from lxml import etree
                dom = etree.HTML(html)
                for el in dom.xpath(cfg["pattern"]):
                    if isinstance(el, str):
                        results.append(el)
                    else:
                        if mode == "text":
                            t = el.text_content().strip() if hasattr(el, "text_content") else str(el)
                            if t: results.append(t)
                        elif mode == "html":
                            results.append(etree.tostring(el, encoding="unicode"))
                        elif mode == "attr":
                            v = el.get(cfg["attr"], "")
                            if v: results.append(v)

            elif cfg["extractor"] == "regex":
                soup = BeautifulSoup(html, "lxml")
                try:
                    regex = re.compile(cfg["pattern"], re.DOTALL | re.IGNORECASE)
                except re.error:
                    regex = re.compile(re.escape(cfg["pattern"]), re.DOTALL | re.IGNORECASE)
                seen = set()
                MAX_COLLECT = 500

                for tag in soup.find_all(True):
                    if len(results) >= MAX_COLLECT:
                        break
                    if (tag.name and regex.search(tag.name)) or \
                       (tag.attrs and regex.search(str(tag.attrs))):
                        h = str(tag)
                        if h in seen: continue
                        seen.add(h)
                        if mode == "text":
                            t = tag.get_text(strip=True)
                            if t: results.append(t)
                        elif mode == "html":
                            results.append(h)
                        elif mode == "attr":
                            v = tag.get(cfg["attr"], "")
                            if v: results.append(v)

                for text_node in soup.find_all(string=True):
                    if len(results) >= MAX_COLLECT:
                        break
                    text = str(text_node).strip()
                    if not text: continue
                    if regex.search(text):
                        parent = text_node.parent
                        if not parent or not parent.name: continue
                        h = str(parent)
                        if h in seen: continue
                        seen.add(h)
                        if mode == "text":
                            t = parent.get_text(strip=True)
                            if t: results.append(t)
                        elif mode == "html":
                            results.append(h)
                        elif mode == "attr":
                            v = parent.get(cfg["attr"], "")
                            if v: results.append(v)
        except Exception as e:
            results.append(f"[提取错误] {e}")
        return results

    def check_queue(self):
        try:
            while True:
                msg = self.msg_queue.get_nowait()
                if msg[0] == "log":
                    self.result_text.config(state="normal")
                    self.result_text.insert(tk.END, msg[1] + "\n")
                    self.result_text.see(tk.END)
                    self.result_text.config(state="disabled")
                elif msg[0] == "done":
                    self.is_running = False
                    self.run_btn.config(state="normal", text="🚀 开始爬取")
                    if msg[1]:
                        self.current_result = msg[1]
                        self.status_label.config(text="✅ 完成，可保存结果")
                    else:
                        self.status_label.config(text="❌ 未获取到数据")
        except queue.Empty:
            pass
        self.win.after(100, self.check_queue)

    def save_result(self):
        if not self.current_result:
            messagebox.showwarning("提示", "还没有可保存的结果", parent=self.win)
            return
        name = simpledialog.askstring("保存到虚拟磁盘", "请输入文件名（含 .txt）：",
                                      initialvalue="crawl_result.txt", parent=self.win)
        if not name:
            return
        if not name.endswith(".txt"):
            name += ".txt"
        try:
            new_id = new_file(name, self.current_result, parent_id=0)
            self.status_label.config(text=f"✅ 已保存: {name}")
            messagebox.showinfo("提示", f"已保存：{name}", parent=self.win)
            logger_requests.info(f"结果保存: {name} (id={new_id})")
        except Exception as e:
            messagebox.showerror("错误", f"保存失败：{e}", parent=self.win)


# ---------------------------------------------------------------------------
# ⑬ 正则专用爬虫窗口    ← 原 ui/apps/crawler_regex.py
# ---------------------------------------------------------------------------

logger_regex = get_logger("crawler_regex")

REGEX_UA_OPTIONS = {
    "随机": "RANDOM",
    "Chrome / Win10": USER_AGENTS[0],
    "Chrome / Win11": USER_AGENTS[1],
    "Firefox / Win10": USER_AGENTS[4],
    "Safari / Mac": USER_AGENTS[3],
    "自定义": None,
}

REGEX_PROXIES = {
    "不使用代理": "",
    "Clash (7890)": "http://127.0.0.1:7890",
    "V2Ray (10809)": "http://127.0.0.1:10809",
    "自定义": None,
}

REGEX_PRESETS = {
    "自定义": "",
    "邮箱": r"[\w.\-]+@[\w.\-]+\.\w+",
    "手机号": r"1[3-9]\d{9}",
    "URL": r"https?://[^\s\"'<>]+",
    "IP 地址": r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
    "HTML 标签": r"<(\w+)[^>]*>",
    "所有 a 标签": r"<a\s+[^>]*href=[\"']([^\"']+)[\"'][^>]*>",
    "所有 img 标签": r"<img\s+[^>]*src=[\"']([^\"']+)[\"'][^>]*>",
    "所有 div 内容": r"<div[^>]*>(.*?)</div>",
    "JSON 字段": r"\"(\w+)\"\s*:\s*\"([^\"]*)\"",
}


class CrawlerRegexWindow:
    def __init__(self, parent):
        self.parent = parent
        self.win = tk.Toplevel(parent)
        self.win.title("正则专用爬虫 - Crawler Workbench")
        self.win.geometry("1100x840")
        self.win.configure(bg="#1e1e1e")
        self.win.attributes("-topmost", True)

        self.is_running = False
        self.msg_queue = queue.Queue()
        self.current_result = ""
        self.session = requests.Session()

        self.build_ui()
        self.check_queue()
        logger_regex.info("正则爬虫窗口启动")

    def build_ui(self):
        tk.Label(self.win, text="🔤 正则专用爬虫",
                 font=("微软雅黑", 16, "bold"),
                 fg="#4CAF50", bg="#1e1e1e").pack(pady=(10, 5))
        tk.Label(self.win, text="纯正则匹配 · Referer / Cookie / 自动重试",
                 font=("微软雅黑", 9), fg="#888888", bg="#1e1e1e").pack(pady=(0, 10))

        main = tk.Frame(self.win, bg="#1e1e1e")
        main.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        left = tk.Frame(main, bg="#1e1e1e", width=460)
        left.pack(side="left", fill="y", padx=(0, 5))
        left.pack_propagate(False)
        self._build_left(left)

        right = tk.Frame(main, bg="#1e1e1e")
        right.pack(side="right", fill="both", expand=True, padx=(5, 0))
        self._build_right(right)

        self.status_label = tk.Label(self.win, text="准备就绪", bg="#111111", fg="#aaaaaa",
                                     font=("微软雅黑", 9), anchor="w", padx=10)
        self.status_label.pack(side="bottom", fill="x")

    def _build_left(self, parent):
        tk.Label(parent, text="🌐 目标 URL", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(5, 3))
        self.url_entry = tk.Entry(parent, font=("Consolas", 10),
                                  bg="#0c0c0c", fg="#00ff00",
                                  insertbackground="#00ff00", relief="flat")
        self.url_entry.pack(fill="x", ipady=4)
        self.url_entry.insert(0, "https://quotes.toscrape.com/")

        tk.Label(parent, text="🎭 User-Agent", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.ua_var = tk.StringVar(value="随机")
        ttk.Combobox(parent, textvariable=self.ua_var,
                     values=list(REGEX_UA_OPTIONS.keys()),
                     state="readonly", width=40).pack(fill="x")

        tk.Label(parent, text="🔗 Referer（留空自动生成）", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.referer_entry = tk.Entry(parent, font=("Consolas", 10),
                                      bg="#0c0c0c", fg="#00ff00",
                                      insertbackground="#00ff00", relief="flat")
        self.referer_entry.pack(fill="x", ipady=4)

        tk.Label(parent, text="🍪 Cookie（可选）", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.cookie_entry = tk.Entry(parent, font=("Consolas", 9),
                                     bg="#0c0c0c", fg="#00ff00",
                                     insertbackground="#00ff00", relief="flat")
        self.cookie_entry.pack(fill="x", ipady=4)

        tk.Label(parent, text="🌐 代理", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.proxy_var = tk.StringVar(value="不使用代理")
        ttk.Combobox(parent, textvariable=self.proxy_var,
                     values=list(REGEX_PROXIES.keys()),
                     state="readonly", width=40).pack(fill="x")

        self.auto_retry = tk.BooleanVar(value=True)
        tk.Checkbutton(parent, text="自动重试（429/403 智能处理）",
                       variable=self.auto_retry,
                       bg="#1e1e1e", fg="white", selectcolor="#1e1e1e",
                       activebackground="#1e1e1e", activeforeground="white").pack(anchor="w", pady=5)

        tk.Label(parent, text="📋 正则模板", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(10, 3))
        self.preset_var = tk.StringVar(value="自定义")
        preset_combo = ttk.Combobox(parent, textvariable=self.preset_var,
                                    values=list(REGEX_PRESETS.keys()),
                                    state="readonly", width=40)
        preset_combo.pack(fill="x")
        preset_combo.bind("<<ComboboxSelected>>", self._on_preset_change)

        tk.Label(parent, text="🎯 正则表达式", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.pattern_entry = tk.Entry(parent, font=("Consolas", 10),
                                      bg="#0c0c0c", fg="#00ff00",
                                      insertbackground="#00ff00", relief="flat")
        self.pattern_entry.pack(fill="x", ipady=4)
        self.pattern_entry.insert(0, r"<title>(.*?)</title>")

        tk.Label(parent, text="⚙️ 匹配模式", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.mode_var = tk.StringVar(value="findall")
        mf = tk.Frame(parent, bg="#1e1e1e")
        mf.pack(fill="x")
        for text, val in [("全匹配", "findall"), ("带上下文", "context")]:
            tk.Radiobutton(mf, text=text, variable=self.mode_var, value=val,
                           bg="#1e1e1e", fg="white", selectcolor="#1e1e1e",
                           activebackground="#1e1e1e", activeforeground="white").pack(side="left", padx=5)

        self.dedup_var = tk.BooleanVar(value=True)
        tk.Checkbutton(parent, text="结果去重", variable=self.dedup_var,
                       bg="#1e1e1e", fg="white", selectcolor="#1e1e1e",
                       activebackground="#1e1e1e", activeforeground="white").pack(anchor="w", pady=3)

        tk.Label(parent, text="⏱️ 请求间隔(秒)", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.delay_var = tk.DoubleVar(value=1.0)
        tk.Scale(parent, from_=0.0, to=5.0, resolution=0.5, orient="horizontal",
                 variable=self.delay_var, bg="#1e1e1e", fg="white",
                 highlightthickness=0, troughcolor="#2a2a2a",
                 activebackground="#4CAF50").pack(fill="x")

        btn_frame = tk.Frame(parent, bg="#1e1e1e")
        btn_frame.pack(fill="x", pady=10)
        self.run_btn = tk.Button(btn_frame, text="🚀 开始匹配", command=self.start_crawl,
                                 bg="#4CAF50", fg="white", relief="flat",
                                 font=("微软雅黑", 12, "bold"), width=14)
        self.run_btn.pack(side="left", padx=3)
        tk.Button(btn_frame, text="💾 保存结果", command=self.save_result,
                  bg="#3498db", fg="white", relief="flat",
                  font=("微软雅黑", 11), width=12).pack(side="left", padx=3)

    def _build_right(self, parent):
        tk.Label(parent, text="📄 匹配结果", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(5, 3))
        tf = tk.Frame(parent, bg="#1e1e1e")
        tf.pack(fill="both", expand=True)
        self.result_text = tk.Text(tf, font=("Consolas", 10),
                                   bg="#0c0c0c", fg="#00ff00",
                                   insertbackground="#00ff00", relief="flat", wrap="word")
        self.result_text.pack(side="left", fill="both", expand=True)
        sb = tk.Scrollbar(tf, orient="vertical", command=self.result_text.yview)
        sb.pack(side="right", fill="y")
        self.result_text.config(yscrollcommand=sb.set)

    def _on_preset_change(self, event=None):
        p = self.preset_var.get()
        if p in REGEX_PRESETS and REGEX_PRESETS[p]:
            self.pattern_entry.delete(0, tk.END)
            self.pattern_entry.insert(0, REGEX_PRESETS[p])

    def start_crawl(self):
        url = self.url_entry.get().strip()
        pattern = self.pattern_entry.get().strip()
        if not url or not pattern:
            messagebox.showwarning("提示", "URL 和正则表达式不能为空", parent=self.win)
            return
        if self.is_running:
            return
        try:
            re.compile(pattern)
        except re.error as e:
            messagebox.showerror("正则错误", str(e), parent=self.win)
            return

        self.is_running = True
        self.run_btn.config(state="disabled", text="匹配中...")
        self.status_label.config(text="正在抓取...")
        self.result_text.config(state="normal")
        self.result_text.delete("1.0", tk.END)
        self.result_text.config(state="disabled")

        cfg = {
            "url": url,
            "pattern": pattern,
            "mode": self.mode_var.get(),
            "dedup": self.dedup_var.get(),
            "delay": self.delay_var.get(),
            "user_agent": REGEX_UA_OPTIONS[self.ua_var.get()],
            "referer": self.referer_entry.get().strip(),
            "cookie": self.cookie_entry.get().strip(),
            "proxy": REGEX_PROXIES[self.proxy_var.get()],
            "auto_retry": self.auto_retry.get(),
        }
        threading.Thread(target=self._do_crawl, args=(cfg,), daemon=True).start()

    def _do_crawl(self, cfg):
        try:
            if cfg["delay"] > 0:
                time.sleep(random.uniform(cfg["delay"] * 0.8, cfg["delay"] * 1.2))

            headers = get_headers(url=cfg["url"], user_agent=cfg["user_agent"],
                                  cookie=cfg["cookie"], referer=cfg["referer"])
            proxies = {"http": cfg["proxy"], "https": cfg["proxy"]} if cfg["proxy"] else None
            log_fn = lambda msg: self.msg_queue.put(("log", msg))

            if cfg["auto_retry"]:
                success, resp = request_with_retry(self.session, "GET", cfg["url"],
                                                   headers, proxies, 15, 3, log_fn)
                if not success:
                    self.msg_queue.put(("log", f"❌ {resp}"))
                    self.msg_queue.put(("done", None))
                    return
            else:
                resp = self.session.get(cfg["url"], headers=headers, proxies=proxies, timeout=15)

            resp.encoding = resp.apparent_encoding or "utf-8"
            if resp.status_code != 200:
                self.msg_queue.put(("log", f"❌ HTTP {resp.status_code}"))
                self.msg_queue.put(("done", None))
                return

            html = resp.text
            self.msg_queue.put(("log", f"✅ 收到 {len(html)} 字符，正在匹配...\n"))

            try:
                pattern = re.compile(cfg["pattern"], re.DOTALL)
            except re.error as e:
                self.msg_queue.put(("log", f"❌ 正则错误：{e}"))
                self.msg_queue.put(("done", None))
                return

            results = self._match(html, pattern, cfg)
            if not results:
                self.msg_queue.put(("log", f"⚠️ 未匹配到内容"))
                self.msg_queue.put(("done", None))
                return

            if cfg["dedup"]:
                seen = set()
                results = [x for x in results if not (x in seen or seen.add(x))]

            MAX_DISPLAY = 300
            PREVIEW_LEN = 150
            total = len(results)

            full_output = f"共匹配到 {total} 条结果：\n\n"
            for i, item in enumerate(results, 1):
                full_output += f"[{i}] {item}\n\n"

            preview = f"共匹配到 {total} 条"
            if total > MAX_DISPLAY:
                preview += f"（仅显示前 {MAX_DISPLAY} 条）"
            preview += "：\n\n"
            for i, item in enumerate(results[:MAX_DISPLAY], 1):
                s = str(item)
                if len(s) > PREVIEW_LEN:
                    s = s[:PREVIEW_LEN] + " ...[截断]"
                preview += f"[{i}] {s}\n\n"

            self.msg_queue.put(("log", preview))
            self.msg_queue.put(("done", full_output))
            logger_regex.info(f"正则匹配成功 共{total}条")
        except Exception as e:
            self.msg_queue.put(("log", f"❌ 异常：{e}"))
            self.msg_queue.put(("done", None))

    def _match(self, html, pattern, cfg):
        results = []
        if cfg["mode"] == "findall":
            for m in pattern.findall(html):
                if isinstance(m, tuple):
                    results.append(" | ".join(m))
                else:
                    results.append(m)
        else:
            for m in pattern.finditer(html):
                s = max(0, m.start() - 60)
                e = min(len(html), m.end() + 60)
                ctx = re.sub(r"\s+", " ", html[s:e]).strip()
                results.append(f"...{ctx}...")
        return results

    def check_queue(self):
        try:
            while True:
                msg = self.msg_queue.get_nowait()
                if msg[0] == "log":
                    self.result_text.config(state="normal")
                    self.result_text.insert(tk.END, msg[1] + "\n")
                    self.result_text.see(tk.END)
                    self.result_text.config(state="disabled")
                elif msg[0] == "done":
                    self.is_running = False
                    self.run_btn.config(state="normal", text="🚀 开始匹配")
                    if msg[1]:
                        self.current_result = msg[1]
                        self.status_label.config(text="✅ 完成")
                    else:
                        self.status_label.config(text="❌ 未获取到数据")
        except queue.Empty:
            pass
        self.win.after(100, self.check_queue)

    def save_result(self):
        if not self.current_result:
            messagebox.showwarning("提示", "没有可保存的结果", parent=self.win)
            return
        name = simpledialog.askstring("保存", "文件名（含 .txt）：",
                                      initialvalue="regex_result.txt", parent=self.win)
        if not name:
            return
        if not name.endswith(".txt"):
            name += ".txt"
        try:
            new_file(name, self.current_result, parent_id=0)
            messagebox.showinfo("提示", f"已保存：{name}", parent=self.win)
        except Exception as e:
            messagebox.showerror("错误", f"保存失败：{e}", parent=self.win)


# ---------------------------------------------------------------------------
# ⑭ aiohttp 异步高并发爬虫    ← 原 ui/apps/crawler_async.py
# ---------------------------------------------------------------------------

logger_async = get_logger("crawler_async")

ASYNC_USER_AGENTS = {
    "随机": None,
    "Chrome / Win10": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Chrome / Win11": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Firefox / Win10": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Safari / Mac":    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
}

ASYNC_PROXIES = {
    "不使用代理": "",
    "Clash (7890)": "http://127.0.0.1:7890",
    "V2Ray (10809)": "http://127.0.0.1:10809",
    "自定义": None,
}


class CrawlerAsyncWindow:
    def __init__(self, parent):
        self.parent = parent
        self.win = tk.Toplevel(parent)
        self.win.title("异步高并发爬虫 - Crawler Workbench")
        self.win.geometry("1200x820")
        self.win.configure(bg="#1e1e1e")
        self.win.attributes("-topmost", True)

        self.is_running = False
        self.msg_queue = queue.Queue()
        self.current_result = ""
        self.total_count = 0
        self.done_count = 0

        self.build_ui()
        self.check_queue()
        logger_async.info("异步爬虫窗口启动")

    def build_ui(self):
        tk.Label(self.win, text="⚡ aiohttp 异步高并发爬虫",
                 font=("微软雅黑", 16, "bold"),
                 fg="#4CAF50", bg="#1e1e1e").pack(pady=(10, 5))
        tk.Label(self.win, text="批量 URL 并发抓取 + 统一提取 · 速度极快",
                 font=("微软雅黑", 9), fg="#888888", bg="#1e1e1e").pack(pady=(0, 10))

        main = tk.Frame(self.win, bg="#1e1e1e")
        main.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        left = tk.Frame(main, bg="#1e1e1e", width=460)
        left.pack(side="left", fill="y", padx=(0, 5))
        left.pack_propagate(False)
        self._build_left(left)

        right = tk.Frame(main, bg="#1e1e1e")
        right.pack(side="right", fill="both", expand=True, padx=(5, 0))
        self._build_right(right)

        self.status_label = tk.Label(self.win, text="准备就绪", bg="#111111", fg="#aaaaaa",
                                     font=("微软雅黑", 9), anchor="w", padx=10)
        self.status_label.pack(side="bottom", fill="x")

    def _build_left(self, parent):
        # ===== URL 列表 =====
        tk.Label(parent, text="🌐 URL 列表（每行一个）", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(5, 3))

        url_frame = tk.Frame(parent, bg="#1e1e1e")
        url_frame.pack(fill="x")
        self.url_text = tk.Text(url_frame, height=6, font=("Consolas", 9),
                                bg="#0c0c0c", fg="#00ff00",
                                insertbackground="#00ff00", relief="flat", wrap="none")
        self.url_text.pack(side="left", fill="both", expand=True)
        sb = tk.Scrollbar(url_frame, orient="vertical", command=self.url_text.yview)
        sb.pack(side="right", fill="y")
        self.url_text.config(yscrollcommand=sb.set)
        self.url_text.insert("1.0",
            "https://quotes.toscrape.com/\n"
            "https://quotes.toscrape.com/page/2/\n"
            "https://quotes.toscrape.com/page/3/\n"
        )

        # ===== 并发数 =====
        tk.Label(parent, text="⚙️ 并发数", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.concurrency_var = tk.IntVar(value=10)
        conc_frame = tk.Frame(parent, bg="#1e1e1e")
        conc_frame.pack(fill="x")
        tk.Scale(conc_frame, from_=1, to=50, orient="horizontal",
                 variable=self.concurrency_var,
                 bg="#1e1e1e", fg="white", highlightthickness=0,
                 troughcolor="#2a2a2a", activebackground="#4CAF50").pack(side="left", fill="x", expand=True)
        self.conc_label = tk.Label(conc_frame, text="10", fg="#00ff00", bg="#1e1e1e",
                                   font=("Consolas", 12, "bold"), width=3)
        self.conc_label.pack(side="right")
        self.concurrency_var.trace_add("write", lambda *a: self.conc_label.config(text=str(self.concurrency_var.get())))

        # ===== 提取器（核心新增） =====
        tk.Label(parent, text="🎯 提取器（对每个页面统一提取）", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(10, 3))

        self.extractor_var = tk.StringVar(value="css")
        ext_frame = tk.Frame(parent, bg="#1e1e1e")
        ext_frame.pack(fill="x")
        for text, val in [("CSS 选择器", "css"), ("XPath", "xpath"), ("正则", "regex"), ("不提取", "none")]:
            tk.Radiobutton(ext_frame, text=text, variable=self.extractor_var, value=val,
                           bg="#1e1e1e", fg="white", selectcolor="#1e1e1e",
                           activebackground="#1e1e1e", activeforeground="white").pack(side="left", padx=3)

        self.pattern_entry = tk.Entry(parent, font=("Consolas", 10),
                                      bg="#0c0c0c", fg="#00ff00",
                                      insertbackground="#00ff00", relief="flat")
        self.pattern_entry.pack(fill="x", ipady=4, pady=(5, 0))
        self.pattern_entry.insert(0, ".quote .text")

        # 提取模式
        mode_frame = tk.Frame(parent, bg="#1e1e1e")
        mode_frame.pack(fill="x", pady=(5, 0))
        tk.Label(mode_frame, text="提取：", fg="#aaaaaa", bg="#1e1e1e",
                 font=("微软雅黑", 10)).pack(side="left")
        self.mode_var = tk.StringVar(value="text")
        for text, val in [("文本", "text"), ("HTML", "html"), ("属性值", "attr")]:
            tk.Radiobutton(mode_frame, text=text, variable=self.mode_var, value=val,
                           bg="#1e1e1e", fg="white", selectcolor="#1e1e1e",
                           activebackground="#1e1e1e", activeforeground="white").pack(side="left", padx=3)

        attr_frame = tk.Frame(parent, bg="#1e1e1e")
        attr_frame.pack(fill="x", pady=(3, 0))
        tk.Label(attr_frame, text="属性名：", fg="#aaaaaa", bg="#1e1e1e",
                 font=("微软雅黑", 10)).pack(side="left")
        self.attr_entry = tk.Entry(attr_frame, font=("Consolas", 10), width=15,
                                   bg="#0c0c0c", fg="#00ff00",
                                   insertbackground="#00ff00", relief="flat")
        self.attr_entry.pack(side="left", padx=5)
        self.attr_entry.insert(0, "href")

        # ===== UA =====
        tk.Label(parent, text="🎭 User-Agent", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.ua_var = tk.StringVar(value="随机")
        ttk.Combobox(parent, textvariable=self.ua_var,
                     values=list(ASYNC_USER_AGENTS.keys()),
                     state="readonly", width=40).pack(fill="x")

        # ===== 代理 =====
        tk.Label(parent, text="🌐 代理", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.proxy_var = tk.StringVar(value="不使用代理")
        ttk.Combobox(parent, textvariable=self.proxy_var,
                     values=list(ASYNC_PROXIES.keys()),
                     state="readonly", width=40).pack(fill="x")

        # ===== 延迟 =====
        tk.Label(parent, text="⏱️ 每个请求前的随机延迟(秒)", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.delay_var = tk.DoubleVar(value=0.0)
        tk.Scale(parent, from_=0.0, to=3.0, resolution=0.5, orient="horizontal",
                 variable=self.delay_var, bg="#1e1e1e", fg="white",
                 highlightthickness=0, troughcolor="#2a2a2a",
                 activebackground="#4CAF50").pack(fill="x")

        # ===== 按钮 =====
        btn_frame = tk.Frame(parent, bg="#1e1e1e")
        btn_frame.pack(fill="x", pady=10)
        self.run_btn = tk.Button(btn_frame, text="🚀 开始并发", command=self.start_crawl,
                                 bg="#4CAF50", fg="white", relief="flat",
                                 font=("微软雅黑", 12, "bold"), width=14)
        self.run_btn.pack(side="left", padx=3)
        tk.Button(btn_frame, text="💾 保存结果", command=self.save_result,
                  bg="#3498db", fg="white", relief="flat",
                  font=("微软雅黑", 11), width=12).pack(side="left", padx=3)

    def _build_right(self, parent):
        tk.Label(parent, text="📄 提取结果", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(5, 3))

        self.progress = ttk.Progressbar(parent, mode="determinate")
        self.progress.pack(fill="x", pady=(0, 5))

        text_frame = tk.Frame(parent, bg="#1e1e1e")
        text_frame.pack(fill="both", expand=True)

        self.result_text = tk.Text(text_frame, font=("Consolas", 10),
                                   bg="#0c0c0c", fg="#00ff00",
                                   insertbackground="#00ff00", relief="flat", wrap="word")
        self.result_text.pack(side="left", fill="both", expand=True)

        sb = tk.Scrollbar(text_frame, orient="vertical", command=self.result_text.yview)
        sb.pack(side="right", fill="y")
        self.result_text.config(yscrollcommand=sb.set)

    # ==================== 主流程 ====================
    def start_crawl(self):
        urls = [line.strip() for line in self.url_text.get("1.0", tk.END).splitlines() if line.strip()]
        if not urls:
            messagebox.showwarning("提示", "请输入至少一个 URL", parent=self.win)
            return
        if self.is_running:
            return

        self.is_running = True
        self.total_count = len(urls)
        self.done_count = 0
        self.progress["maximum"] = self.total_count
        self.progress["value"] = 0

        self.run_btn.config(state="disabled", text="并发中...")
        self.status_label.config(text=f"正在并发抓取 {self.total_count} 个 URL...")
        self.result_text.config(state="normal")
        self.result_text.delete("1.0", tk.END)
        self.result_text.config(state="disabled")

        ua_key = self.ua_var.get()
        user_agent = "RANDOM" if ua_key == "随机" else ASYNC_USER_AGENTS[ua_key]

        config = {
            "urls": urls,
            "concurrency": self.concurrency_var.get(),
            "user_agent": user_agent,
            "proxy": ASYNC_PROXIES[self.proxy_var.get()],
            "delay": self.delay_var.get(),
            "extractor": self.extractor_var.get(),
            "pattern": self.pattern_entry.get().strip(),
            "mode": self.mode_var.get(),
            "attr": self.attr_entry.get().strip(),
            "referer": "",  # 可以后续加输入框
            "cookie": "",
            "auto_retry": True,
        }

        threading.Thread(target=self._run_async, args=(config,), daemon=True).start()

    def _run_async(self, cfg):
        try:
            asyncio.run(self._async_main(cfg))
        except Exception as e:
            self.msg_queue.put(("log", f"❌ 主流程异常：{e}"))
            self.msg_queue.put(("done", None))

    async def _async_main(self, cfg):
        semaphore = asyncio.Semaphore(cfg["concurrency"])
        results = [None] * len(cfg["urls"])

        connector = aiohttp.TCPConnector(ssl=False)
        timeout = aiohttp.ClientTimeout(total=20)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = []
            for idx, url in enumerate(cfg["urls"]):
                tasks.append(self._fetch_one(session, semaphore, url, idx, cfg, results))

            await asyncio.gather(*tasks)

        # ===== 生成最终输出（带提取） =====
        output_lines = [f"共抓取 {len(cfg['urls'])} 个 URL\n", "=" * 70, ""]

        total_extracted = 0
        for i, item in enumerate(results, 1):
            if not item:
                output_lines.append(f"[{i}] 无结果\n")
                continue

            output_lines.append(f"[{i}] {item['url']}")
            output_lines.append(f"    状态: {item['status']} | 长度: {item['length']} 字符")

            # ===== 核心：执行提取 =====
            if cfg["extractor"] != "none" and item.get("content"):
                extracted = self._extract(item["content"], cfg)
                if extracted:
                    total_extracted += len(extracted)
                    output_lines.append(f"    ─── 提取 {len(extracted)} 条 ───")
                    for j, val in enumerate(extracted, 1):
                        # 每条限制 300 字符
                        val_str = str(val)
                        if len(val_str) > 300:
                            val_str = val_str[:300] + " ...[截断]"
                        output_lines.append(f"      ({j}) {val_str}")
                else:
                    output_lines.append("    ─── 未提取到内容 ───")

            output_lines.append("")

        output_lines.append("=" * 70)
        output_lines.append(f"总计提取 {total_extracted} 条数据")

        full_output = "\n".join(output_lines)
        self.msg_queue.put(("done", full_output))

    def _extract(self, html, cfg):
        """对单个页面的 HTML 执行提取（正则支持标签名/属性/文本三合一）"""
        results = []
        mode = cfg["mode"]
        ext = cfg["extractor"]
        pattern = cfg["pattern"]

        if not pattern:
            return results

        try:
            if ext == "css":
                soup = BeautifulSoup(html, "lxml")
                elements = soup.select(pattern)
                for el in elements:
                    if mode == "text":
                        t = el.get_text(strip=True)
                        if t: results.append(t)
                    elif mode == "html":
                        results.append(str(el))
                    elif mode == "attr":
                        v = el.get(cfg["attr"], "")
                        if v: results.append(v)

            elif ext == "xpath":
                from lxml import etree
                dom = etree.HTML(html)
                elements = dom.xpath(pattern)
                for el in elements:
                    if isinstance(el, str):
                        results.append(el)
                    else:
                        if mode == "text":
                            t = el.text_content().strip() if hasattr(el, "text_content") else str(el)
                            if t: results.append(t)
                        elif mode == "html":
                            results.append(etree.tostring(el, encoding="unicode"))
                        elif mode == "attr":
                            v = el.get(cfg["attr"], "")
                            if v: results.append(v)

            elif ext == "regex":
                # ===== 三合一：标签名 / 属性 / 文本 =====
                soup = BeautifulSoup(html, "lxml")
                try:
                    regex = re.compile(pattern, re.DOTALL | re.IGNORECASE)
                except re.error:
                    regex = re.compile(re.escape(pattern), re.DOTALL | re.IGNORECASE)

                seen = set()
                MAX_COLLECT = 200  # 每个页面最多提取 200 条

                # 第一轮：匹配标签名和属性
                for tag in soup.find_all(True):
                    if len(results) >= MAX_COLLECT:
                        break
                    name_hit = bool(tag.name and regex.search(tag.name))
                    attr_hit = bool(tag.attrs and regex.search(str(tag.attrs)))

                    if name_hit or attr_hit:
                        html_str = str(tag)
                        if html_str in seen:
                            continue
                        seen.add(html_str)

                        if mode == "text":
                            t = tag.get_text(strip=True)
                            if t: results.append(t)
                        elif mode == "html":
                            results.append(html_str)
                        elif mode == "attr":
                            v = tag.get(cfg["attr"], "")
                            if v: results.append(v)

                # 第二轮：匹配文本内容
                for text_node in soup.find_all(string=True):
                    if len(results) >= MAX_COLLECT:
                        break
                    text = str(text_node).strip()
                    if not text:
                        continue
                    if regex.search(text):
                        parent = text_node.parent
                        if not parent or not parent.name:
                            continue
                        html_str = str(parent)
                        if html_str in seen:
                            continue
                        seen.add(html_str)

                        if mode == "text":
                            t = parent.get_text(strip=True)
                            if t: results.append(t)
                        elif mode == "html":
                            results.append(html_str)
                        elif mode == "attr":
                            v = parent.get(cfg["attr"], "")
                            if v: results.append(v)

        except Exception as e:
            results.append(f"[提取错误] {e}")

        return results

    async def _fetch_one(self, session, semaphore, url, idx, cfg, results):
        async with semaphore:
            max_retries = 3 if cfg.get("auto_retry", True) else 1
            headers = get_headers(url=url, user_agent=cfg["user_agent"],
                                  cookie=cfg.get("cookie", ""),
                                  referer=cfg.get("referer", ""))

            for attempt in range(max_retries):
                try:
                    if cfg["delay"] > 0:
                        await asyncio.sleep(random.uniform(cfg["delay"] * 0.8, cfg["delay"] * 1.2))

                    start = time.time()
                    async with session.get(url, headers=headers, proxy=cfg["proxy"] or None) as resp:
                        content = await resp.text()
                        elapsed = time.time() - start

                        if resp.status == 429:
                            wait = 5 * (attempt + 1)
                            self.msg_queue.put(("log", f"⚠️ [{idx + 1}] 429，等待 {wait}s"))
                            await asyncio.sleep(wait)
                            continue
                        if resp.status in (403, 401):
                            headers["User-Agent"] = random.choice(USER_AGENTS)  # UA 池来自 ④ 反爬工具
                            self.msg_queue.put(("log", f"⚠️ [{idx + 1}] {resp.status}，换 UA 重试"))
                            await asyncio.sleep(2)
                            continue

                        results[idx] = {"url": url, "status": resp.status,
                                        "content": content, "length": len(content)}
                        self.msg_queue.put(
                            ("log", f"✅ [{idx + 1}] {resp.status} | {len(content)} 字符 | {elapsed:.2f}s"))
                        break

                except asyncio.TimeoutError:
                    self.msg_queue.put(("log", f"⏱️ [{idx + 1}] 超时（{attempt + 1}/{max_retries}）"))
                    await asyncio.sleep(2)
                except Exception as e:
                    self.msg_queue.put(("log", f"❌ [{idx + 1}] {str(e)[:60]}"))
                    await asyncio.sleep(1)
            else:
                results[idx] = {"url": url, "status": "FAILED", "content": "", "length": 0}

            self.done_count += 1
            self.msg_queue.put(("progress", self.done_count))

    def check_queue(self):
        try:
            while True:
                msg = self.msg_queue.get_nowait()
                if msg[0] == "log":
                    self.result_text.config(state="normal")
                    self.result_text.insert(tk.END, msg[1] + "\n")
                    self.result_text.see(tk.END)
                    self.result_text.config(state="disabled")
                elif msg[0] == "progress":
                    self.progress["value"] = msg[1]
                    self.status_label.config(text=f"进度：{msg[1]}/{self.total_count}")
                elif msg[0] == "done":
                    self.is_running = False
                    self.run_btn.config(state="normal", text="🚀 开始并发")
                    if msg[1]:
                        self.current_result = msg[1]
                        self.status_label.config(text=f"✅ 完成 {self.total_count} 个请求")
                        # 清空日志区，显示最终提取结果
                        self.result_text.config(state="normal")
                        self.result_text.delete("1.0", tk.END)
                        self.result_text.insert(tk.END, msg[1])
                        self.result_text.config(state="disabled")
                    else:
                        self.status_label.config(text="❌ 未获取到数据")
        except queue.Empty:
            pass
        self.win.after(100, self.check_queue)

    def save_result(self):
        if not self.current_result:
            messagebox.showwarning("提示", "还没有可保存的结果", parent=self.win)
            return
        name = simpledialog.askstring("保存到虚拟磁盘", "请输入文件名（含 .txt）：",
                                      initialvalue="async_result.txt", parent=self.win)
        if not name:
            return
        if not name.endswith(".txt"):
            name += ".txt"
        try:
            new_id = new_file(name, self.current_result, parent_id=0)
            self.status_label.config(text=f"✅ 已保存到虚拟磁盘: {name}")
            messagebox.showinfo("提示", f"已保存到虚拟磁盘：{name}", parent=self.win)
            logger_async.info(f"异步结果保存: {name} (id={new_id})")
        except Exception as e:
            messagebox.showerror("错误", f"保存失败：{e}", parent=self.win)


# ---------------------------------------------------------------------------
# ⑮ Playwright 浏览器爬虫    ← 原 ui/apps/crawler_browser.py
# ---------------------------------------------------------------------------

logger_browser = get_logger("crawler_browser")


class CrawlerBrowserWindow:
    def __init__(self, parent):
        self.parent = parent
        self.win = tk.Toplevel(parent)
        self.win.title("Playwright 浏览器爬虫 - Crawler Workbench")
        self.win.geometry("1180x820")
        self.win.configure(bg="#1e1e1e")
        self.win.attributes("-topmost", True)

        self.is_running = False
        self.msg_queue = queue.Queue()
        self.current_result = ""
        self.last_screenshot_path = None

        self.build_ui()
        self.check_queue()
        logger_browser.info("Playwright 爬虫窗口启动")

    def build_ui(self):
        tk.Label(self.win, text="🌐 Playwright 浏览器爬虫",
                 font=("微软雅黑", 16, "bold"),
                 fg="#4CAF50", bg="#1e1e1e").pack(pady=(10, 5))
        tk.Label(self.win, text="动态渲染 · 点击 / 滚动 / 等待 · 截图",
                 font=("微软雅黑", 9), fg="#888888", bg="#1e1e1e").pack(pady=(0, 10))

        main = tk.Frame(self.win, bg="#1e1e1e")
        main.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        left = tk.Frame(main, bg="#1e1e1e", width=460)
        left.pack(side="left", fill="y", padx=(0, 5))
        left.pack_propagate(False)
        self._build_left(left)

        right = tk.Frame(main, bg="#1e1e1e")
        right.pack(side="right", fill="both", expand=True, padx=(5, 0))
        self._build_right(right)

        self.status_label = tk.Label(self.win, text="准备就绪", bg="#111111", fg="#aaaaaa",
                                     font=("微软雅黑", 9), anchor="w", padx=10)
        self.status_label.pack(side="bottom", fill="x")

    def _build_left(self, parent):
        # URL
        tk.Label(parent, text="🌐 目标 URL", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(5, 3))
        self.url_entry = tk.Entry(parent, font=("Consolas", 10),
                                  bg="#0c0c0c", fg="#00ff00",
                                  insertbackground="#00ff00", relief="flat")
        self.url_entry.pack(fill="x", ipady=4)
        self.url_entry.insert(0, "https://quotes.toscrape.com/js/")

        # 无头模式
        self.headless_var = tk.BooleanVar(value=True)
        tk.Checkbutton(parent, text="无头模式（不显示浏览器窗口，更快）",
                       variable=self.headless_var,
                       bg="#1e1e1e", fg="white", selectcolor="#1e1e1e",
                       activebackground="#1e1e1e", activeforeground="white").pack(anchor="w", pady=3)

        # 等待策略
        tk.Label(parent, text="⏳ 加载策略", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.wait_var = tk.StringVar(value="domcontentloaded")
        wait_frame = tk.Frame(parent, bg="#1e1e1e")
        wait_frame.pack(fill="x")
        for text, val in [("DOM 加载完", "domcontentloaded"),
                          ("网络空闲", "networkidle"),
                          ("页面完全加载", "load")]:
            tk.Radiobutton(wait_frame, text=text, variable=self.wait_var, value=val,
                           bg="#1e1e1e", fg="white", selectcolor="#1e1e1e",
                           activebackground="#1e1e1e", activeforeground="white",
                           font=("微软雅黑", 9)).pack(side="left", padx=3)

        # 等待选择器
        tk.Label(parent, text="🎯 等待选择器出现（可选，留空则只按策略等待）",
                 fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.wait_selector_entry = tk.Entry(parent, font=("Consolas", 10),
                                            bg="#0c0c0c", fg="#00ff00",
                                            insertbackground="#00ff00", relief="flat")
        self.wait_selector_entry.pack(fill="x", ipady=4)
        self.wait_selector_entry.insert(0, ".quote")

        # 额外等待
        tk.Label(parent, text="⏱️ 额外等待（秒）", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.extra_wait = tk.DoubleVar(value=1.0)
        tk.Scale(parent, from_=0.0, to=10.0, resolution=0.5, orient="horizontal",
                 variable=self.extra_wait, bg="#1e1e1e", fg="white",
                 highlightthickness=0, troughcolor="#2a2a2a",
                 activebackground="#4CAF50").pack(fill="x")

        # 操作：滚动
        tk.Label(parent, text="🖱️ 滚动到底部（次数，0 表示不滚动）",
                 fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.scroll_var = tk.IntVar(value=0)
        tk.Spinbox(parent, from_=0, to=20, textvariable=self.scroll_var,
                   width=5, font=("Consolas", 11),
                   bg="#0c0c0c", fg="#00ff00",
                   buttonbackground="#4CAF50", relief="flat").pack(anchor="w")

        # 操作：点击
        tk.Label(parent, text="👆 点击某元素（可选，CSS 选择器）",
                 fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.click_entry = tk.Entry(parent, font=("Consolas", 10),
                                    bg="#0c0c0c", fg="#00ff00",
                                    insertbackground="#00ff00", relief="flat")
        self.click_entry.pack(fill="x", ipady=4)
        self.click_entry.insert(0, "")

        # 截图
        self.screenshot_var = tk.BooleanVar(value=False)
        tk.Checkbutton(parent, text="保存页面截图到虚拟磁盘",
                       variable=self.screenshot_var,
                       bg="#1e1e1e", fg="white", selectcolor="#1e1e1e",
                       activebackground="#1e1e1e", activeforeground="white").pack(anchor="w", pady=3)

        # 提取器
        tk.Label(parent, text="🎯 提取器（HTML 提取）", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(8, 3))
        self.extractor_var = tk.StringVar(value="css")
        ext_frame = tk.Frame(parent, bg="#1e1e1e")
        ext_frame.pack(fill="x")
        for text, val in [("CSS 选择器", "css"), ("XPath", "xpath"), ("正则", "regex"), ("不提取", "none")]:
            tk.Radiobutton(ext_frame, text=text, variable=self.extractor_var, value=val,
                           bg="#1e1e1e", fg="white", selectcolor="#1e1e1e",
                           activebackground="#1e1e1e", activeforeground="white",
                           font=("微软雅黑", 9)).pack(side="left", padx=3)

        self.pattern_entry = tk.Entry(parent, font=("Consolas", 10),
                                      bg="#0c0c0c", fg="#00ff00",
                                      insertbackground="#00ff00", relief="flat")
        self.pattern_entry.pack(fill="x", ipady=4, pady=(5, 0))
        self.pattern_entry.insert(0, ".quote .text")

        # 提取模式
        mode_frame = tk.Frame(parent, bg="#1e1e1e")
        mode_frame.pack(fill="x", pady=(5, 0))
        tk.Label(mode_frame, text="提取：", fg="#aaaaaa", bg="#1e1e1e",
                 font=("微软雅黑", 10)).pack(side="left")
        self.mode_var = tk.StringVar(value="text")
        for text, val in [("文本", "text"), ("HTML", "html"), ("属性值", "attr")]:
            tk.Radiobutton(mode_frame, text=text, variable=self.mode_var, value=val,
                           bg="#1e1e1e", fg="white", selectcolor="#1e1e1e",
                           activebackground="#1e1e1e", activeforeground="white",
                           font=("微软雅黑", 9)).pack(side="left", padx=3)

        attr_frame = tk.Frame(parent, bg="#1e1e1e")
        attr_frame.pack(fill="x", pady=(3, 0))
        tk.Label(attr_frame, text="属性名：", fg="#aaaaaa", bg="#1e1e1e",
                 font=("微软雅黑", 10)).pack(side="left")
        self.attr_entry = tk.Entry(attr_frame, font=("Consolas", 10), width=12,
                                   bg="#0c0c0c", fg="#00ff00",
                                   insertbackground="#00ff00", relief="flat")
        self.attr_entry.pack(side="left", padx=5)
        self.attr_entry.insert(0, "href")

        # 按钮
        btn_frame = tk.Frame(parent, bg="#1e1e1e")
        btn_frame.pack(fill="x", pady=15)
        self.run_btn = tk.Button(btn_frame, text="🚀 开始抓取", command=self.start_crawl,
                                 bg="#4CAF50", fg="white", relief="flat",
                                 font=("微软雅黑", 12, "bold"), width=14, height=1)
        self.run_btn.pack(side="left", padx=3)
        self.stop_btn = tk.Button(btn_frame, text="🛑 停止", command=self.stop_crawl,
                                  bg="#e74c3c", fg="white", relief="flat",
                                  font=("微软雅黑", 11), width=10, height=1, state="disabled")
        self.stop_btn.pack(side="left", padx=3)
        tk.Button(btn_frame, text="💾 保存结果", command=self.save_result,
                  bg="#3498db", fg="white", relief="flat",
                  font=("微软雅黑", 11), width=12).pack(side="left", padx=3)

    def _build_right(self, parent):
        tk.Label(parent, text="📄 运行日志与结果", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(5, 3))
        tf = tk.Frame(parent, bg="#1e1e1e")
        tf.pack(fill="both", expand=True)
        self.result_text = tk.Text(tf, font=("Consolas", 10),
                                   bg="#0c0c0c", fg="#00ff00",
                                   insertbackground="#00ff00", relief="flat", wrap="word")
        self.result_text.pack(side="left", fill="both", expand=True)
        sb = tk.Scrollbar(tf, orient="vertical", command=self.result_text.yview)
        sb.pack(side="right", fill="y")
        self.result_text.config(yscrollcommand=sb.set)

    # ==================== 主流程 ====================
    def start_crawl(self):
        url = self.url_entry.get().strip()
        if not url:
            messagebox.showwarning("提示", "请输入 URL", parent=self.win)
            return
        if self.is_running:
            return

        self.is_running = True
        self.run_btn.config(state="disabled", text="抓取中...")
        self.stop_btn.config(state="normal")
        self.status_label.config(text="正在启动浏览器...")
        self.result_text.config(state="normal")
        self.result_text.delete("1.0", tk.END)
        self.result_text.config(state="disabled")

        cfg = {
            "url": url,
            "headless": self.headless_var.get(),
            "wait_until": self.wait_var.get(),
            "wait_selector": self.wait_selector_entry.get().strip(),
            "extra_wait": self.extra_wait.get(),
            "scroll": self.scroll_var.get(),
            "click": self.click_entry.get().strip(),
            "screenshot": self.screenshot_var.get(),
            "extractor": self.extractor_var.get(),
            "pattern": self.pattern_entry.get().strip(),
            "mode": self.mode_var.get(),
            "attr": self.attr_entry.get().strip(),
        }

        threading.Thread(target=self._do_crawl, args=(cfg,), daemon=True).start()

    def _do_crawl(self, cfg):
        """在子线程里同步执行 Playwright"""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            self.msg_queue.put(("log", "❌ 未安装 Playwright。请执行：\n"
                                     "   pip install playwright\n"
                                     "   playwright install chromium"))
            self.msg_queue.put(("done", None))
            return

        try:
            with sync_playwright() as p:
                self.msg_queue.put(("log", "[1/4] 启动 Chromium..."))
                browser = p.chromium.launch(headless=cfg["headless"])
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1366, "height": 768},
                )
                page = context.new_page()

                self.msg_queue.put(("log", f"[2/4] 访问 {cfg['url']}..."))
                page.goto(cfg["url"], wait_until=cfg["wait_until"], timeout=30000)

                # 等待选择器
                if cfg["wait_selector"]:
                    self.msg_queue.put(("log", f"[2/4] 等待元素出现: {cfg['wait_selector']}"))
                    try:
                        page.wait_for_selector(cfg["wait_selector"], timeout=10000)
                        self.msg_queue.put(("log", "      ✅ 元素已出现"))
                    except Exception as e:
                        self.msg_queue.put(("log", f"      ⚠️ 等待超时: {e}"))

                # 额外等待
                if cfg["extra_wait"] > 0:
                    self.msg_queue.put(("log", f"[2/4] 额外等待 {cfg['extra_wait']}s..."))
                    page.wait_for_timeout(int(cfg["extra_wait"] * 1000))

                # 点击
                if cfg["click"]:
                    self.msg_queue.put(("log", f"[2/4] 点击元素: {cfg['click']}"))
                    try:
                        page.click(cfg["click"], timeout=5000)
                        page.wait_for_timeout(1000)
                    except Exception as e:
                        self.msg_queue.put(("log", f"      ⚠️ 点击失败: {e}"))

                # 滚动
                if cfg["scroll"] > 0:
                    self.msg_queue.put(("log", f"[2/4] 滚动到底部 {cfg['scroll']} 次..."))
                    for i in range(cfg["scroll"]):
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        page.wait_for_timeout(800)

                # 截图
                if cfg["screenshot"]:
                    self.msg_queue.put(("log", "[3/4] 截图..."))
                    try:
                        shot_bytes = page.screenshot(full_page=True)
                        name = simpledialog.askstring("保存截图", "请输入文件名（含 .png）：",
                                                      initialvalue="screenshot.png",
                                                      parent=self.win)
                        if name:
                            if not name.endswith(".png"):
                                name += ".png"
                            create_binary_file(name, shot_bytes, 0)
                            self.msg_queue.put(("log", f"      ✅ 截图已保存: {name}"))
                    except Exception as e:
                        self.msg_queue.put(("log", f"      ⚠️ 截图失败: {e}"))

                # 获取最终 HTML
                html = page.content()
                self.msg_queue.put(("log", f"[3/4] 已获取 HTML（{len(html)} 字符）"))

                browser.close()

            # ===== 提取 =====
            if cfg["extractor"] == "none":
                self.current_result = html
                self.msg_queue.put(("log", "\n✅ 完成（未提取，保存后是完整 HTML）"))
                self.msg_queue.put(("done", html))
                return

            self.msg_queue.put(("log", "[4/4] 提取数据..."))
            results = self._extract(html, cfg)
            if not results:
                self.msg_queue.put(("log", "⚠️ 未提取到内容"))
                self.msg_queue.put(("done", None))
                return

            MAX_DISPLAY = 300
            PREVIEW_LEN = 150
            total = len(results)

            full_output = f"共提取 {total} 条：\n\n"
            for i, item in enumerate(results, 1):
                full_output += f"[{i}] {item}\n\n"

            preview = f"共提取 {total} 条：\n\n"
            for i, item in enumerate(results[:MAX_DISPLAY], 1):
                s = str(item)
                if len(s) > PREVIEW_LEN:
                    s = s[:PREVIEW_LEN] + " ...[截断]"
                preview += f"[{i}] {s}\n\n"

            self.msg_queue.put(("log", "\n" + preview))
            self.msg_queue.put(("done", full_output))
            logger_browser.info(f"Playwright 抓取成功 共{total}条")

        except Exception as e:
            self.msg_queue.put(("log", f"❌ 异常：{e}"))
            self.msg_queue.put(("done", None))
            logger_browser.error(f"Playwright 抓取失败: {e}")

    def _extract(self, html, cfg):
        """从 HTML 提取数据（正则支持标签名/属性/文本三合一）"""
        results = []
        mode = cfg["mode"]
        ext = cfg["extractor"]
        pattern = cfg["pattern"]

        if not pattern:
            return results

        try:
            if ext == "css":
                soup = BeautifulSoup(html, "lxml")
                for el in soup.select(pattern):
                    if mode == "text":
                        t = el.get_text(strip=True)
                        if t: results.append(t)
                    elif mode == "html":
                        results.append(str(el))
                    elif mode == "attr":
                        v = el.get(cfg["attr"], "")
                        if v: results.append(v)

            elif ext == "xpath":
                from lxml import etree
                dom = etree.HTML(html)
                for el in dom.xpath(pattern):
                    if isinstance(el, str):
                        results.append(el)
                    else:
                        if mode == "text":
                            t = el.text_content().strip() if hasattr(el, "text_content") else str(el)
                            if t: results.append(t)
                        elif mode == "html":
                            results.append(etree.tostring(el, encoding="unicode"))
                        elif mode == "attr":
                            v = el.get(cfg["attr"], "")
                            if v: results.append(v)

            elif ext == "regex":
                # ===== 三合一：标签名 / 属性 / 文本 =====
                soup = BeautifulSoup(html, "lxml")
                try:
                    regex = re.compile(pattern, re.DOTALL | re.IGNORECASE)
                except re.error:
                    regex = re.compile(re.escape(pattern), re.DOTALL | re.IGNORECASE)

                seen = set()
                MAX_COLLECT = 300

                # 第一轮：匹配标签名和属性
                for tag in soup.find_all(True):
                    if len(results) >= MAX_COLLECT:
                        break
                    name_hit = bool(tag.name and regex.search(tag.name))
                    attr_hit = bool(tag.attrs and regex.search(str(tag.attrs)))

                    if name_hit or attr_hit:
                        h = str(tag)
                        if h in seen:
                            continue
                        seen.add(h)

                        if mode == "text":
                            t = tag.get_text(strip=True)
                            if t: results.append(t)
                        elif mode == "html":
                            results.append(h)
                        elif mode == "attr":
                            v = tag.get(cfg["attr"], "")
                            if v: results.append(v)

                # 第二轮：匹配文本内容（返回最近父节点）
                for text_node in soup.find_all(string=True):
                    if len(results) >= MAX_COLLECT:
                        break
                    text = str(text_node).strip()
                    if not text:
                        continue
                    if regex.search(text):
                        parent = text_node.parent
                        if not parent or not parent.name:
                            continue
                        h = str(parent)
                        if h in seen:
                            continue
                        seen.add(h)

                        if mode == "text":
                            t = parent.get_text(strip=True)
                            if t: results.append(t)
                        elif mode == "html":
                            results.append(h)
                        elif mode == "attr":
                            v = parent.get(cfg["attr"], "")
                            if v: results.append(v)

        except Exception as e:
            results.append(f"[提取错误] {e}")

        return results

    def stop_crawl(self):
        self.msg_queue.put(("log", "[系统] 停止信号已发送（等待当前操作完成）"))
        # Playwright 的 stop 需要通过异常机制，简化处理：忽略，让用户等待完成
        self.stop_btn.config(state="disabled")

    def check_queue(self):
        try:
            while True:
                msg = self.msg_queue.get_nowait()
                if msg[0] == "log":
                    self.result_text.config(state="normal")
                    self.result_text.insert(tk.END, msg[1] + "\n")
                    self.result_text.see(tk.END)
                    self.result_text.config(state="disabled")
                elif msg[0] == "done":
                    self.is_running = False
                    self.run_btn.config(state="normal", text="🚀 开始抓取")
                    self.stop_btn.config(state="disabled")
                    if msg[1]:
                        self.current_result = msg[1]
                        self.status_label.config(text="✅ 完成，可保存结果")
                    else:
                        self.status_label.config(text="❌ 未获取到数据")
        except queue.Empty:
            pass
        self.win.after(100, self.check_queue)

    # ==================== 保存 ====================
    def save_result(self):
        if not self.current_result:
            messagebox.showwarning("提示", "没有可保存的结果", parent=self.win)
            return
        name = simpledialog.askstring("保存到虚拟磁盘", "文件名（含 .txt）：",
                                      initialvalue="browser_result.txt", parent=self.win)
        if not name:
            return
        if not name.endswith(".txt"):
            name += ".txt"
        try:
            new_file(name, self.current_result, parent_id=0)
            messagebox.showinfo("提示", f"已保存：{name}", parent=self.win)
            logger_browser.info(f"Playwright 结果保存: {name}")
        except Exception as e:
            messagebox.showerror("错误", f"保存失败：{e}", parent=self.win)


# ---------------------------------------------------------------------------
# ⑯ Scrapy 高级爬虫（沙盒运行）    ← 原 ui/apps/crawler_scrapy.py
# ---------------------------------------------------------------------------

logger_scrapy = get_logger("crawler_scrapy")

SANDBOX_DIR = os.path.join(get_base_dir(), "temp_scrapy")


class ScrapyResultPreview:
    """运行结果预览窗口"""

    def __init__(self, parent, data, sandbox_dir):
        self.parent = parent
        self.data = data
        self.sandbox_dir = sandbox_dir

        self.win = tk.Toplevel(parent)
        self.win.title("Scrapy 运行结果 - Crawler Workbench")
        self.win.geometry("800x620")
        self.win.configure(bg="#1e1e1e")
        self.win.attributes("-topmost", True)
        self.win.protocol("WM_DELETE_WINDOW", self.discard)

        # 标题
        tk.Label(self.win, text="✅ Scrapy 运行完成",
                 font=("微软雅黑", 16, "bold"),
                 fg="#4CAF50", bg="#1e1e1e").pack(pady=(15, 5))
        count = len(data) if isinstance(data, list) else 1
        tk.Label(self.win, text=f"共抓取 {count} 条数据",
                 font=("微软雅黑", 10), fg="#aaaaaa", bg="#1e1e1e").pack(pady=(0, 10))

        # 结果展示
        text_frame = tk.Frame(self.win, bg="#1e1e1e")
        text_frame.pack(fill="both", expand=True, padx=15, pady=5)

        self.result_text = tk.Text(text_frame, font=("Consolas", 10),
                                   bg="#0c0c0c", fg="#00ff00",
                                   insertbackground="#00ff00",
                                   relief="flat", wrap="word")
        self.result_text.pack(side="left", fill="both", expand=True)

        sb = tk.Scrollbar(text_frame, orient="vertical", command=self.result_text.yview)
        sb.pack(side="right", fill="y")
        self.result_text.config(yscrollcommand=sb.set)

        pretty = json.dumps(data, ensure_ascii=False, indent=2)
        # 如果太长，只展示前 5000 字符
        if len(pretty) > 20000:
            pretty = pretty[:20000] + "\n\n... [预览已截断，完整内容点'保存'会全量保存] ..."
        self.result_text.insert("1.0", pretty)
        self.result_text.config(state="disabled")

        # 底部按钮
        btn_frame = tk.Frame(self.win, bg="#1e1e1e")
        btn_frame.pack(fill="x", padx=15, pady=15)

        tk.Button(btn_frame, text="💾 保存到虚拟磁盘", command=self.save_to_virtual,
                  bg="#4CAF50", fg="white", relief="flat",
                  font=("微软雅黑", 12, "bold"), width=18, height=2).pack(side="left", padx=5)
        tk.Button(btn_frame, text="🗑️ 放弃并清理", command=self.discard,
                  bg="#e74c3c", fg="white", relief="flat",
                  font=("微软雅黑", 12, "bold"), width=18, height=2).pack(side="left", padx=5)

    def save_to_virtual(self):
        name = simpledialog.askstring("保存到虚拟磁盘", "请输入文件名（含 .json）：",
                                      initialvalue="scrapy_result.json", parent=self.win)
        if not name:
            return
        if not name.endswith(".json"):
            name += ".json"
        try:
            content = json.dumps(self.data, ensure_ascii=False, indent=2)
            new_id = new_file(name, content, parent_id=0)
            messagebox.showinfo("提示", f"✅ 已保存到虚拟磁盘：{name}", parent=self.win)
            logger_scrapy.info(f"Scrapy 结果保存: {name} (id={new_id})")
            self._cleanup_and_close()
        except Exception as e:
            messagebox.showerror("错误", f"保存失败：{e}", parent=self.win)

    def discard(self):
        if messagebox.askyesno("确认", "确定放弃本次结果并清理沙箱吗？", parent=self.win):
            self._cleanup_and_close()

    def _cleanup_and_close(self):
        """清理沙箱目录并关闭窗口"""
        try:
            if os.path.exists(self.sandbox_dir):
                shutil.rmtree(self.sandbox_dir, ignore_errors=True)
                logger_scrapy.info(f"沙盒已清理: {self.sandbox_dir}")
        except Exception as e:
            logger_scrapy.error(f"沙盒清理失败: {e}")
        self.win.destroy()


class CrawlerScrapyWindow:
    def __init__(self, parent):
        self.parent = parent
        self.win = tk.Toplevel(parent)
        self.win.title("Scrapy 高级爬虫 - Crawler Workbench")
        self.win.geometry("1200x820")
        self.win.configure(bg="#1e1e1e")
        self.win.attributes("-topmost", True)
        self.win.protocol("WM_DELETE_WINDOW", self.on_close)

        self.is_running = False
        self.current_process = None
        self.msg_queue = queue.Queue()
        self.field_rows = []  # 保存字段输入行控件

        # 清理上次残留的沙箱
        if os.path.exists(SANDBOX_DIR):
            shutil.rmtree(SANDBOX_DIR, ignore_errors=True)

        self.build_ui()
        self.check_queue()
        logger_scrapy.info("Scrapy 爬虫窗口启动")

    def build_ui(self):
        tk.Label(self.win, text="🕸️ Scrapy 高级爬虫（沙盒运行）",
                 font=("微软雅黑", 16, "bold"),
                 fg="#4CAF50", bg="#1e1e1e").pack(pady=(10, 5))
        tk.Label(self.win, text="填表单 → 生成 spider → 沙盒运行 → 预览结果 → 保存或清理",
                 font=("微软雅黑", 9), fg="#888888", bg="#1e1e1e").pack(pady=(0, 10))

        main = tk.Frame(self.win, bg="#1e1e1e")
        main.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        # ===== 左侧配置区 =====
        left = tk.Frame(main, bg="#1e1e1e", width=560)
        left.pack(side="left", fill="y", padx=(0, 5))
        left.pack_propagate(False)
        self._build_left(left)

        # ===== 右侧日志区 =====
        right = tk.Frame(main, bg="#1e1e1e")
        right.pack(side="right", fill="both", expand=True, padx=(5, 0))
        self._build_right(right)

        # 状态栏
        self.status_label = tk.Label(self.win, text="准备就绪", bg="#111111", fg="#aaaaaa",
                                     font=("微软雅黑", 9), anchor="w", padx=10)
        self.status_label.pack(side="bottom", fill="x")

    def _build_left(self, parent):
        # ===== 基本配置 =====
        tk.Label(parent, text="📝 基本配置", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 12, "bold")).pack(anchor="w", pady=(5, 5))

        # 项目名
        tk.Label(parent, text="项目名（英文）", fg="#aaaaaa", bg="#1e1e1e",
                 font=("微软雅黑", 10)).pack(anchor="w")
        self.name_entry = tk.Entry(parent, font=("Consolas", 10),
                                   bg="#0c0c0c", fg="#00ff00",
                                   insertbackground="#00ff00", relief="flat")
        self.name_entry.pack(fill="x", ipady=4)
        self.name_entry.insert(0, "my_spider")

        # 起始 URL
        tk.Label(parent, text="起始 URL", fg="#aaaaaa", bg="#1e1e1e",
                 font=("微软雅黑", 10)).pack(anchor="w", pady=(8, 0))
        self.url_entry = tk.Entry(parent, font=("Consolas", 10),
                                  bg="#0c0c0c", fg="#00ff00",
                                  insertbackground="#00ff00", relief="flat")
        self.url_entry.pack(fill="x", ipady=4)
        self.url_entry.insert(0, "https://quotes.toscrape.com/")

        # 允许域名
        tk.Label(parent, text="允许域名（留空则不过滤）", fg="#aaaaaa", bg="#1e1e1e",
                 font=("微软雅黑", 10)).pack(anchor="w", pady=(8, 0))
        self.domain_entry = tk.Entry(parent, font=("Consolas", 10),
                                     bg="#0c0c0c", fg="#00ff00",
                                     insertbackground="#00ff00", relief="flat")
        self.domain_entry.pack(fill="x", ipady=4)
        self.domain_entry.insert(0, "quotes.toscrape.com")

        # 列表容器选择器
        tk.Label(parent, text="列表容器选择器（每个条目的 CSS 选择器）",
                 fg="#aaaaaa", bg="#1e1e1e",
                 font=("微软雅黑", 10)).pack(anchor="w", pady=(8, 0))
        self.list_entry = tk.Entry(parent, font=("Consolas", 10),
                                   bg="#0c0c0c", fg="#00ff00",
                                   insertbackground="#00ff00", relief="flat")
        self.list_entry.pack(fill="x", ipady=4)
        self.list_entry.insert(0, ".quote")

        # ===== 字段配置 =====
        tk.Label(parent, text="🎯 字段配置", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 12, "bold")).pack(anchor="w", pady=(15, 5))

        # 表头
        header = tk.Frame(parent, bg="#1e1e1e")
        header.pack(fill="x")
        tk.Label(header, text="字段名", fg="#888888", bg="#1e1e1e", width=10,
                 font=("微软雅黑", 9)).pack(side="left")
        tk.Label(header, text="CSS 选择器（相对）", fg="#888888", bg="#1e1e1e", width=22,
                 font=("微软雅黑", 9)).pack(side="left")
        tk.Label(header, text="模式", fg="#888888", bg="#1e1e1e", width=8,
                 font=("微软雅黑", 9)).pack(side="left")
        tk.Label(header, text="属性名", fg="#888888", bg="#1e1e1e", width=8,
                 font=("微软雅黑", 9)).pack(side="left")

        # 字段容器（可滚动）
        self.fields_frame = tk.Frame(parent, bg="#1e1e1e")
        self.fields_frame.pack(fill="x")

        # 默认 3 行
        self.add_field("text", ".text", "text")
        self.add_field("author", ".author", "text")
        self.add_field("tags", ".tags .tag", "text")

        # 添加按钮
        tk.Button(parent, text="➕ 添加字段", command=lambda: self.add_field("", "", "text"),
                  bg="#3498db", fg="white", relief="flat",
                  font=("微软雅黑", 9)).pack(anchor="w", pady=(5, 0))

        # ===== 翻页 =====
        tk.Label(parent, text="🔗 翻页选择器（可选）", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 12, "bold")).pack(anchor="w", pady=(15, 3))
        self.next_entry = tk.Entry(parent, font=("Consolas", 10),
                                   bg="#0c0c0c", fg="#00ff00",
                                   insertbackground="#00ff00", relief="flat")
        self.next_entry.pack(fill="x", ipady=4)
        self.next_entry.insert(0, ".next a::attr(href)")

        # ===== 按钮 =====
        btn_frame = tk.Frame(parent, bg="#1e1e1e")
        btn_frame.pack(fill="x", pady=15)

        self.run_btn = tk.Button(btn_frame, text="🚀 生成并运行", command=self.start_run,
                                 bg="#4CAF50", fg="white", relief="flat",
                                 font=("微软雅黑", 12, "bold"), width=14, height=1)
        self.run_btn.pack(side="left", padx=3)

        self.stop_btn = tk.Button(btn_frame, text="🛑 停止", command=self.stop_run,
                                  bg="#e74c3c", fg="white", relief="flat",
                                  font=("微软雅黑", 11), width=10, height=1,
                                  state="disabled")
        self.stop_btn.pack(side="left", padx=3)

    def _build_right(self, parent):
        tk.Label(parent, text="📄 运行日志", fg="#4CAF50", bg="#1e1e1e",
                 font=("微软雅黑", 11, "bold")).pack(anchor="w", pady=(5, 3))

        text_frame = tk.Frame(parent, bg="#1e1e1e")
        text_frame.pack(fill="both", expand=True)

        self.log_text = tk.Text(text_frame, font=("Consolas", 10),
                                bg="#0c0c0c", fg="#00ff00",
                                insertbackground="#00ff00",
                                relief="flat", wrap="word", state="disabled")
        self.log_text.pack(side="left", fill="both", expand=True)

        sb = tk.Scrollbar(text_frame, orient="vertical", command=self.log_text.yview)
        sb.pack(side="right", fill="y")
        self.log_text.config(yscrollcommand=sb.set)

    # ==================== 字段管理 ====================
    def add_field(self, name, selector, mode):
        row = tk.Frame(self.fields_frame, bg="#1e1e1e")
        row.pack(fill="x", pady=2)

        name_var = tk.StringVar(value=name)
        selector_var = tk.StringVar(value=selector)
        mode_var = tk.StringVar(value=mode)
        attr_var = tk.StringVar(value="href")

        tk.Entry(row, textvariable=name_var, width=12, font=("Consolas", 9),
                 bg="#0c0c0c", fg="#00ff00", insertbackground="#00ff00",
                 relief="flat").pack(side="left", padx=2, ipady=2)
        tk.Entry(row, textvariable=selector_var, width=26, font=("Consolas", 9),
                 bg="#0c0c0c", fg="#00ff00", insertbackground="#00ff00",
                 relief="flat").pack(side="left", padx=2, ipady=2)
        ttk.Combobox(row, textvariable=mode_var, values=["text", "attr", "html"],
                     state="readonly", width=6).pack(side="left", padx=2)
        tk.Entry(row, textvariable=attr_var, width=8, font=("Consolas", 9),
                 bg="#0c0c0c", fg="#00ff00", insertbackground="#00ff00",
                 relief="flat").pack(side="left", padx=2, ipady=2)

        def remove():
            row.destroy()
            self.field_rows = [r for r in self.field_rows if r["row"] != row]

        tk.Button(row, text="✖", command=remove,
                  bg="#e74c3c", fg="white", relief="flat",
                  font=("微软雅黑", 8), width=2).pack(side="left", padx=2)

        self.field_rows.append({
            "row": row,
            "name": name_var,
            "selector": selector_var,
            "mode": mode_var,
            "attr": attr_var,
        })

    def _collect_fields(self):
        """收集所有字段配置"""
        fields = []
        for r in self.field_rows:
            name = r["name"].get().strip()
            selector = r["selector"].get().strip()
            if not name or not selector:
                continue
            fields.append({
                "name": name,
                "selector": selector,
                "mode": r["mode"].get(),
                "attr": r["attr"].get().strip(),
            })
        return fields

    # ==================== 日志 ====================
    def log(self, text):
        self.log_text.config(state="normal")
        self.log_text.insert(tk.END, text + "\n")
        self.log_text.see(tk.END)
        self.log_text.config(state="disabled")

    # ==================== 主流程 ====================
    def start_run(self):
        if self.is_running:
            return

        # 收集配置
        project_name = self.name_entry.get().strip()
        start_url = self.url_entry.get().strip()
        allowed = self.domain_entry.get().strip()
        list_sel = self.list_entry.get().strip()
        fields = self._collect_fields()
        next_sel = self.next_entry.get().strip()

        if not project_name or not start_url or not list_sel:
            messagebox.showwarning("提示", "项目名、起始 URL、列表选择器都不能为空", parent=self.win)
            return
        if not fields:
            messagebox.showwarning("提示", "请至少配置一个字段", parent=self.win)
            return

        config = {
            "project_name": project_name,
            "start_url": start_url,
            "allowed_domain": allowed,
            "list_selector": list_sel,
            "fields": fields,
            "next_selector": next_sel,
        }

        self.is_running = True
        self.run_btn.config(state="disabled", text="运行中...")
        self.stop_btn.config(state="normal")
        self.status_label.config(text="正在生成 spider 并运行...")
        self.log_text.config(state="normal")
        self.log_text.delete("1.0", tk.END)
        self.log_text.config(state="disabled")

        threading.Thread(target=self._do_run, args=(config,), daemon=True).start()

    def _do_run(self, config):
        try:
            # 1. 清理旧沙箱
            if os.path.exists(SANDBOX_DIR):
                shutil.rmtree(SANDBOX_DIR, ignore_errors=True)

            # 2. 生成 spider
            self.msg_queue.put(("log", "[1/3] 正在生成 spider.py..."))
            spider_path, code = write_spider(SANDBOX_DIR, config)
            self.msg_queue.put(("log", f"[1/3] ✅ 已生成: {spider_path}\n"))

            # 3. 启动自己（带 --scrapy-runner），在子进程里跑 Scrapy
            self.msg_queue.put(("log", "[2/3] 正在启动 Scrapy..."))
            self.msg_queue.put(("log", "[提示] 首次运行可能较慢（10~30 秒），请耐心等待...\n"))

            result_path = os.path.join(SANDBOX_DIR, "result.json")

            cmd = [sys.executable]
            if not getattr(sys, "frozen", False):
                # 源码运行时要把本文件作为脚本传给解释器
                cmd.append(os.path.abspath(__file__))
            cmd += [
                "--scrapy-runner",
                "runspider", "spider.py",
                "-o", "result.json",
                "-s", "LOG_LEVEL=INFO",
                "-s", "LOG_STDOUT=True",
                "-s", "CLOSESPIDER_TIMEOUT=180",  # 3 分钟强制结束
                "-s", "CLOSESPIDER_ITEMCOUNT=2000",  # 最多 2000 条
            ]

            self.current_process = subprocess.Popen(
                cmd,
                cwd=SANDBOX_DIR,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # 合并 stderr 到 stdout
                stdin=subprocess.DEVNULL,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                env={
                    **os.environ,
                    "PYTHONUNBUFFERED": "1",
                    "PYTHONIOENCODING": "utf-8",
                }
            )

            # 实时读取子进程输出
            def read_stream(stream):
                for line in iter(stream.readline, ""):
                    if line:
                        line = line.rstrip()
                        if line:
                            self.msg_queue.put(("log", line))
                stream.close()

            reader = threading.Thread(target=read_stream, args=(self.current_process.stdout,), daemon=True)
            reader.start()
            reader.join()

            self.current_process.wait()
            exit_code = self.current_process.returncode
            self.msg_queue.put(("log", f"\n[2/3] 子进程退出码: {exit_code}"))

            if exit_code != 0:
                self.msg_queue.put(("log", "[2/3] ❌ Scrapy 运行失败，请检查上面的错误信息"))
                self.msg_queue.put(("done", None, None))
                return

            # 4. 读取结果
            self.msg_queue.put(("log", "[3/3] 正在读取 result.json..."))
            if not os.path.exists(result_path):
                self.msg_queue.put(("log", "[3/3] ⚠️ 未生成 result.json（可能没抓到数据）"))
                self.msg_queue.put(("done", None, None))
                return

            with open(result_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            self.msg_queue.put(("log", f"[3/3] ✅ 共抓取 {len(data)} 条数据"))
            self.msg_queue.put(("done", data, SANDBOX_DIR))

        except Exception as e:
            import traceback
            err = traceback.format_exc()
            self.msg_queue.put(("log", f"❌ 异常：{e}\n{err}"))
            self.msg_queue.put(("done", None, None))
            logger_scrapy.error(f"Scrapy 运行异常: {e}")

            def read_stream(stream, tag):
                for line in iter(stream.readline, ""):
                    if line:
                        self.msg_queue.put(("log", f"[{tag}] {line.rstrip()}"))
                stream.close()

            t1 = threading.Thread(target=read_stream, args=(self.current_process.stdout, "OUT"), daemon=True)
            t2 = threading.Thread(target=read_stream, args=(self.current_process.stderr, "ERR"), daemon=True)
            t1.start()
            t2.start()
            t1.join()
            t2.join()

            self.current_process.wait()
            exit_code = self.current_process.returncode

            self.msg_queue.put(("log", f"\n[2/3] 退出码: {exit_code}"))

            if exit_code != 0:
                self.msg_queue.put(("log", "[2/3] ❌ scrapy 运行失败，请检查上面的错误信息"))
                self.msg_queue.put(("done", None, None))
                return

            # 4. 读取结果
            self.msg_queue.put(("log", "[3/3] 正在读取结果..."))
            if not os.path.exists(result_path):
                self.msg_queue.put(("log", "[3/3] ⚠️ 未生成 result.json（可能没抓到数据）"))
                self.msg_queue.put(("done", None, None))
                return

            with open(result_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            self.msg_queue.put(("log", f"[3/3] ✅ 共抓取 {len(data)} 条数据"))
            self.msg_queue.put(("done", data, SANDBOX_DIR))

        except Exception as e:
            self.msg_queue.put(("log", f"❌ 异常：{e}"))
            self.msg_queue.put(("done", None, None))
            logger_scrapy.error(f"Scrapy 运行异常: {e}")

    def stop_run(self):
        if self.current_process and self.current_process.poll() is None:
            try:
                self.current_process.terminate()
                self.log("[系统] 🛑 已发送停止信号")
            except Exception as e:
                self.log(f"[系统] 停止失败: {e}")

    def check_queue(self):
        try:
            while True:
                msg = self.msg_queue.get_nowait()
                if msg[0] == "log":
                    self.log(msg[1])
                elif msg[0] == "done":
                    self.is_running = False
                    self.run_btn.config(state="normal", text="🚀 生成并运行")
                    self.stop_btn.config(state="disabled")
                    self.current_process = None

                    if msg[1] is not None:
                        self.status_label.config(text="✅ 运行完成，请查看结果窗口")
                        # 弹出结果预览窗口
                        ScrapyResultPreview(self.win, msg[1], msg[2])
                    else:
                        self.status_label.config(text="❌ 运行失败或未抓取到数据")
        except queue.Empty:
            pass
        self.win.after(100, self.check_queue)

    def on_close(self):
        """关闭窗口时清理沙箱"""
        if self.current_process and self.current_process.poll() is None:
            try:
                self.current_process.terminate()
            except Exception:
                pass

        try:
            if os.path.exists(SANDBOX_DIR):
                shutil.rmtree(SANDBOX_DIR, ignore_errors=True)
        except Exception:
            pass

        self.win.destroy()


# ---------------------------------------------------------------------------
# ⑰ 主终端    ← 原 ui/terminal.py
# ---------------------------------------------------------------------------

logger_terminal = get_logger("terminal")


class CommandManual:
    """指令大全弹窗"""

    MANUAL_DATA = {
        "🖥️ 系统命令": [
            ("help", "在终端内列出所有可用指令"),
            ("manual", "打开本指令大全窗口"),
            ("clear", "清空终端屏幕"),
            ("exit", "退出程序"),
            ("disk", "查看虚拟磁盘使用情况"),
            ("guides", "列出所有爬虫说明的概览"),
            ("guide <名字>", "查看指定爬虫的详细说明（如 guide crawler）"),
        ],
        "🕷️ 爬虫命令": [
            ("crawler", "requests + bs4 经典爬虫（代理/Session/Headers/正则）"),
            ("scrapy", "Scrapy 高级爬虫（沙盒运行）"),
            ("regex", "正则专用爬虫（纯文本匹配）"),
            ("async", "aiohttp 异步高并发爬虫"),
            ("browser", "Playwright 浏览器爬虫（动态渲染）"),
        ],
        "📁 文件管理命令": [
            ("files", "打开图形化文件管理器"),
            ("ls", "列出当前目录内容"),
            ("cd <目录>", "切换当前目录（支持 cd .. 返回上级，cd / 回到根目录）"),
            ("mkdir <名字>", "创建文件夹"),
            ("touch <名字>", "创建空文件"),
            ("rm <名字>", "删除文件或文件夹"),
            ("rename <原名> <新名>", "重命名文件或文件夹"),
            ("edit <文件名>", "用文本编辑器打开文件"),
            ("view <文件名>", "查看图片或视频"),
            ("zip", "打开压缩/解压工具"),
        ],
    }

    @classmethod
    def show(cls, parent):
        win = tk.Toplevel(parent)
        win.title("指令大全 - Crawler Workbench")
        win.geometry("700x650")
        win.configure(bg="#0c0c0c")
        win.attributes("-topmost", True)

        tk.Label(win, text="📖 指令大全",
                 font=("微软雅黑", 18, "bold"),
                 fg="#00ff00", bg="#0c0c0c").pack(pady=(15, 5))
        tk.Label(win, text="输入指令后按回车即可执行",
                 font=("微软雅黑", 9), fg="#888888", bg="#0c0c0c").pack(pady=(0, 10))

        canvas = tk.Canvas(win, bg="#0c0c0c", highlightthickness=0)
        scrollbar = tk.Scrollbar(win, orient="vertical", command=canvas.yview)
        inner = tk.Frame(canvas, bg="#0c0c0c")

        inner.bind("<Configure>",
                   lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=inner, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True, padx=10, pady=(0, 10))
        scrollbar.pack(side="right", fill="y", pady=(0, 10))

        for category, commands in cls.MANUAL_DATA.items():
            tk.Label(inner, text=category, font=("微软雅黑", 13, "bold"),
                     fg="#4CAF50", bg="#0c0c0c").pack(anchor="w", pady=(15, 5), padx=10)

            for cmd, desc in commands:
                row = tk.Frame(inner, bg="#1a1a1a")
                row.pack(fill="x", padx=20, pady=2)

                tk.Label(row, text=cmd, font=("Consolas", 11, "bold"),
                         fg="#00ff00", bg="#1a1a1a", width=22, anchor="w").pack(side="left", padx=10, pady=5)
                tk.Label(row, text=desc, font=("微软雅黑", 10),
                         fg="#dddddd", bg="#1a1a1a", anchor="w").pack(side="left", fill="x", expand=True)


class Terminal:
    """主终端窗口"""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Crawler Workbench - 终端")
        self.root.geometry("900x600")
        self.root.configure(bg="#0c0c0c")

        # 当前工作目录（虚拟磁盘的 id 和显示路径）
        self.cwd_id = 0
        self.cwd_path = "/"

        # 历史命令
        self.history = []
        self.history_index = -1

        # 命令映射表
        self.commands = self._build_commands()

        self.build_ui()
        self.print_welcome()
        logger_terminal.info("爬虫工作台启动")

    def build_ui(self):
        # ===== 输出区 =====
        self.output = scrolledtext.ScrolledText(
            self.root, wrap="word", font=("Consolas", 11),
            bg="#0c0c0c", fg="#00ff00", insertbackground="#00ff00",
            relief="flat", state="disabled"
        )
        self.output.pack(fill="both", expand=True, padx=10, pady=(10, 0))

        # ===== 输入区 =====
        input_frame = tk.Frame(self.root, bg="#0c0c0c")
        input_frame.pack(fill="x", padx=10, pady=10)

        self.prompt_label = tk.Label(input_frame, text=f"{self.cwd_path} >",
                                     fg="#00ff00", bg="#0c0c0c",
                                     font=("Consolas", 12, "bold"))
        self.prompt_label.pack(side="left")

        self.entry = tk.Entry(input_frame, font=("Consolas", 12),
                              bg="#1a1a1a", fg="#00ff00",
                              insertbackground="#00ff00", relief="flat")
        self.entry.pack(side="left", fill="x", expand=True, padx=(5, 0), ipady=4)
        self.entry.bind("<Return>", self.execute_command)
        self.entry.bind("<Up>", self.history_up)
        self.entry.bind("<Down>", self.history_down)
        self.entry.focus()

    def _build_commands(self):
        return {
            # 系统命令
            "help":      ("列出所有指令", self.cmd_help),
            "manual":    ("打开指令大全", self.cmd_manual),
            "clear":     ("清空屏幕", self.cmd_clear),
            "exit":      ("退出程序", self.cmd_exit),
            "disk":      ("查看虚拟磁盘", self.cmd_disk),
            "guide": ("查看爬虫使用说明", self.cmd_guide),
            "guides": ("列出所有爬虫说明", self.cmd_guides),

            # 爬虫命令
            "crawler":   ("requests 经典爬虫", self.cmd_crawler),
            "scrapy":    ("Scrapy 高级爬虫", self.cmd_scrapy),
            "regex":     ("正则专用爬虫", self.cmd_regex),
            "async":     ("异步高并发爬虫", self.cmd_async),
            "browser":   ("Playwright 浏览器爬虫", self.cmd_browser),

            # 文件管理
            "files":     ("打开文件管理器", self.cmd_files),
            "ls":        ("列出当前目录", self.cmd_ls),
            "cd":        ("切换目录", self.cmd_cd),
            "mkdir":     ("创建文件夹", self.cmd_mkdir),
            "touch":     ("创建文件", self.cmd_touch),
            "rm":        ("删除文件/文件夹", self.cmd_rm),
            "rename":    ("重命名", self.cmd_rename),
            "edit":      ("编辑文本文件", self.cmd_edit),
            "view":      ("查看图片/视频", self.cmd_view),
            "zip":       ("压缩/解压", self.cmd_zip),
        }

    # ==================== 输出辅助 ====================
    def print_text(self, text):
        self.output.config(state="normal")
        self.output.insert(tk.END, text + "\n")
        self.output.see(tk.END)
        self.output.config(state="disabled")

    def print_welcome(self):
        self.print_text("=" * 60)
        self.print_text("   🕷️  Crawler Workbench - 爬虫工作台 v1.0")
        self.print_text("=" * 60)
        self.print_text("   输入 'help' 查看所有指令")
        self.print_text("   输入 'manual' 打开图形化指令大全")
        self.print_text("=" * 60)
        self.print_text("")

    def update_prompt(self):
        self.prompt_label.config(text=f"{self.cwd_path} >")

    def _update_path_string(self):
        """根据 cwd_id 往上回溯，拼出显示用的路径字符串"""
        parts = []
        current = self.cwd_id
        while current and current != 0:
            item = get_info(current)
            if not item:
                break
            parts.insert(0, item["name"])
            current = item["parent_id"]
        self.cwd_path = "/" + "/".join(parts) if parts else "/"
        self.update_prompt()

    # ==================== 历史命令 ====================
    def history_up(self, event):
        if self.history and self.history_index > 0:
            self.history_index -= 1
            self.entry.delete(0, tk.END)
            self.entry.insert(0, self.history[self.history_index])

    def history_down(self, event):
        if self.history and self.history_index < len(self.history) - 1:
            self.history_index += 1
            self.entry.delete(0, tk.END)
            self.entry.insert(0, self.history[self.history_index])
        else:
            self.history_index = len(self.history)
            self.entry.delete(0, tk.END)

    # ==================== 命令执行入口 ====================
    def execute_command(self, event=None):
        raw = self.entry.get().strip()
        self.entry.delete(0, tk.END)

        if not raw:
            return

        self.history.append(raw)
        self.history_index = len(self.history)

        self.print_text(f"{self.cwd_path} > {raw}")

        parts = raw.split()
        cmd = parts[0].lower()
        args = parts[1:]

        if cmd in self.commands:
            desc, func = self.commands[cmd]
            try:
                func(args)
            except Exception as e:
                self.print_text(f"[错误] {e}")
                logger_terminal.error(f"命令执行失败 [{raw}]: {e}")
        else:
            self.print_text(f"[错误] 未知指令: '{cmd}'")
            self.print_text("       输入 'help' 查看所有可用指令")

    # ==================== 系统命令实现 ====================
    def cmd_help(self, args):
        self.print_text("")
        self.print_text("=" * 60)
        self.print_text("📖 可用指令列表")
        self.print_text("=" * 60)

        categories = {
            "系统命令": ["help", "manual", "guides", "guide", "clear", "exit", "disk"],
            "爬虫命令": ["crawler", "scrapy", "regex", "async", "browser"],
            "文件管理": ["files", "ls", "cd", "mkdir", "touch", "rm", "rename", "edit", "view", "zip"],
        }

        for cat, cmds in categories.items():
            self.print_text(f"\n【{cat}】")
            for c in cmds:
                desc = self.commands[c][0]
                self.print_text(f"  {c:<12} {desc}")

        self.print_text("")
        self.print_text("  输入 'manual' 可打开图形化指令大全")
        self.print_text("")

    def cmd_manual(self, args):
        CommandManual.show(self.root)
        self.print_text("[系统] 已打开指令大全窗口")

    def cmd_guides(self, args):
        """列出所有爬虫的概览"""
        self.print_text("")
        self.print_text("=" * 60)
        self.print_text("📚 爬虫使用说明 - 概览")
        self.print_text("=" * 60)
        self.print_text(get_overview())
        self.print_text("  输入 guide <名字> 查看详细说明")
        self.print_text("  例：guide crawler / guide scrapy")
        self.print_text("")

    def cmd_guide(self, args):
        """查看某个爬虫的详细说明"""
        if not args:
            self.cmd_guides(args)
            return

        cmd = args[0].lower()
        content = get_guide(cmd)

        if not content:
            self.print_text(f"[错误] 没有名为 '{cmd}' 的爬虫说明")
            self.print_text("       可用名字: crawler / regex / async / scrapy / browser")
            return

        # 弹窗显示
        win = tk.Toplevel(self.root)
        win.title(f"爬虫说明 - {cmd}")
        win.geometry("800x680")
        win.configure(bg="#1e1e1e")
        win.attributes("-topmost", True)

        tk.Label(win, text=f"📖 {cmd} 使用说明",
                 font=("微软雅黑", 15, "bold"),
                 fg="#4CAF50", bg="#1e1e1e").pack(pady=(15, 5))

        text_frame = tk.Frame(win, bg="#1e1e1e")
        text_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))

        text = tk.Text(text_frame, font=("Consolas", 10),
                       bg="#0c0c0c", fg="#00ff00",
                       insertbackground="#00ff00",
                       relief="flat", wrap="word")
        text.pack(side="left", fill="both", expand=True)

        sb = tk.Scrollbar(text_frame, orient="vertical", command=text.yview)
        sb.pack(side="right", fill="y")
        text.config(yscrollcommand=sb.set)

        text.insert("1.0", content)
        text.config(state="disabled")

        self.print_text(f"[系统] 已打开 {cmd} 使用说明窗口")

    def cmd_clear(self, args):
        self.output.config(state="normal")
        self.output.delete("1.0", tk.END)
        self.output.config(state="disabled")

    def cmd_exit(self, args):
        if messagebox.askyesno("退出", "确定要退出爬虫工作台吗？"):
            logger_terminal.info("用户主动退出")
            self.root.destroy()

    def cmd_disk(self, args):
        self.print_text("[系统] 虚拟磁盘统计功能开发中...")

    # ==================== 爬虫命令（占位） ====================
    def cmd_crawler(self, args):
        self.print_text("[爬虫] 正在打开 requests 经典爬虫窗口...")
        CrawlerRequestsWindow(self.root)

    def cmd_scrapy(self, args):
        self.print_text("[爬虫] 正在打开 Scrapy 高级爬虫窗口...")
        CrawlerScrapyWindow(self.root)

    def cmd_regex(self, args):
        self.print_text("[爬虫] 正在打开正则专用爬虫窗口...")
        CrawlerRegexWindow(self.root)

    def cmd_async(self, args):
        self.print_text("[爬虫] 正在打开异步高并发爬虫窗口...")
        CrawlerAsyncWindow(self.root)

    def cmd_browser(self, args):
        self.print_text("[爬虫] 正在打开 Playwright 浏览器爬虫窗口...")
        CrawlerBrowserWindow(self.root)

    # ==================== 文件管理命令 ====================
    def cmd_files(self, args):
        self.print_text("[文件] 正在打开图形化文件管理器...")
        FileManagerWindow(self.root)

    def cmd_ls(self, args):
        items = list_dir(self.cwd_id)
        if not items:
            self.print_text("(空目录)")
            return
        for item in items:
            if item["is_dir"]:
                self.print_text(f"  [DIR]  {item['name']}")
            else:
                self.print_text(f"  [FILE] {item['name']}")

    def cmd_cd(self, args):
        if not args:
            self.print_text("用法: cd <目录名> 或 cd .. 或 cd /")
            return
        target = args[0]

        if target == "/":
            self.cwd_id = 0
            self.cwd_path = "/"
            self.update_prompt()
            return

        if target == "..":
            if self.cwd_id == 0:
                return
            item = get_info(self.cwd_id)
            if item:
                self.cwd_id = item["parent_id"]
                self._update_path_string()
            return

        item = find(target, self.cwd_id)
        if item and item["is_dir"]:
            self.cwd_id = item["id"]
            self._update_path_string()
        else:
            self.print_text(f"目录不存在: {target}")

    def cmd_mkdir(self, args):
        if not args:
            self.print_text("用法: mkdir <目录名>")
            return
        name = args[0]
        if find(name, self.cwd_id):
            self.print_text(f"已存在同名文件或文件夹: {name}")
            return
        new_folder(name, self.cwd_id)
        self.print_text(f"已创建文件夹: {name}")

    def cmd_touch(self, args):
        if not args:
            self.print_text("用法: touch <文件名>")
            return
        name = args[0]
        if find(name, self.cwd_id):
            self.print_text(f"已存在同名文件或文件夹: {name}")
            return
        new_file(name, "", self.cwd_id)
        self.print_text(f"已创建文件: {name}")

    def cmd_rm(self, args):
        if not args:
            self.print_text("用法: rm <名称>")
            return
        name = args[0]
        item = find(name, self.cwd_id)
        if item:
            remove(item["id"])
            self.print_text(f"已删除: {name}")
        else:
            self.print_text(f"未找到: {name}")

    def cmd_rename(self, args):
        if len(args) < 2:
            self.print_text("用法: rename <原名> <新名>")
            return
        old_name, new_name = args[0], args[1]
        item = find(old_name, self.cwd_id)
        if item:
            rename(item["id"], new_name)
            self.print_text(f"已重命名: {old_name} -> {new_name}")
        else:
            self.print_text(f"未找到: {old_name}")

    def cmd_edit(self, args):
        if not args:
            self.print_text("用法: edit <文件名>")
            return
        self.print_text(f"[文件] 编辑文件 {args[0]}（开发中）")

    def cmd_view(self, args):
        if not args:
            self.print_text("用法: view <文件名>")
            return
        name = args[0]
        item = find(name, self.cwd_id)
        if not item or item["is_dir"]:
            self.print_text(f"未找到文件: {name}")
            return

        ext = os.path.splitext(item["name"])[1].lower()
        if ext in (".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp"):
            ImageViewerWindow(self.root, item["id"])
            self.print_text(f"[文件] 已打开图片: {name}")
        elif ext in (".mp3", ".wav", ".ogg"):
            MusicPlayerWindow(self.root, item["id"])
            self.print_text(f"[文件] 已打开音乐: {name}")
        elif ext in (".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv"):
            open_video(self.root, item["id"])
            self.print_text(f"[文件] 已调用系统播放器: {name}")
        elif ext in (".txt", ".md", ".py", ".json", ".csv", ".log", ".html", ".css", ".js"):
            fm = FileManagerWindow(self.root)
            # 直接编辑
            fm.open_text_editor(item)
            self.print_text(f"[文件] 已打开文本编辑: {name}")
        else:
            self.print_text(f"[文件] 未知类型，无法预览: {name}")

    def cmd_zip(self, args):
        self.print_text("[文件] 正在打开压缩/解压工具...")
        ZipToolWindow(self.root)

    def run(self):
        self.root.mainloop()


# ---------------------------------------------------------------------------
# ⑱ 程序入口    ← 原 main.py
# ---------------------------------------------------------------------------

logger_main = get_logger("main")


def main():
    logger_main.info("=" * 40)
    logger_main.info("爬虫工作台启动")
    app = Terminal()
    app.run()


if __name__ == "__main__":
    main()
