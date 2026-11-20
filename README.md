# Crawler Workbench · 爬虫工作台

一个跑在 Windows 上的**终端式爬虫工作台**：命令行界面 + 图形化文件管理器 + 五个独立爬虫窗口，
全部代码收在单个 `main.py` 里，不需要安装、不写注册表，`python main.py` 就能用。

内置一套基于 SQLite 的**虚拟磁盘**，抓到的网页、图片、视频、压缩包都存在里面，可以像本地文件一样
浏览、编辑、打包、导出到你电脑上的真实目录。

---

## 注意事项
1:爬虫工具自身携带存储库，可以在打开files功能后选择在硬盘导入文件或者文件夹，并且可以导出选中文件

2:强烈建议将其放入文件夹打开，运行过程中会自动创建logs，data文件夹并存储相关数据


---
## 功能一览

| 模块 | 说明 |
|---|---|
| 终端 | 命令输入、历史记录（上下键）、`manual` 打开指令大全 |
| 虚拟磁盘 | SQLite 存储；文件夹 / 文本 / 二进制（图片、音视频、压缩包）；导入导出真实文件 |
| 文件管理器 | 图形化浏览、新建、重命名、删除、文本编辑器、双击打开 |
| 媒体预览 | 图片查看（缩放、自适应）、音乐播放、视频调用系统默认播放器 |
| 压缩 / 解压 | 基于虚拟磁盘打 zip、解 zip，支持多选打包 |
| 说明手册 | 每个爬虫窗口内置参数说明、示例和常见坑（`guide <名字>` 也能看） |

### 五个爬虫

| 命令 | 内核 | 适用场景 |
|---|---|---|
| `crawler` | requests + BeautifulSoup | 静态页面，数据直接在 HTML 源码里 |
| `regex` | 纯正则 | 快速捞散落文本，不做 DOM 解析 |
| `async` | aiohttp + asyncio | 批量 URL 高并发抓取 |
| `browser` | Playwright（Chromium） | JS 动态渲染、懒加载、需要点击的页面 |
| `scrapy` | Scrapy 2.x | 大规模、多页面、自动翻页（沙盒里生成代码并运行） |

统一反爬能力：6 个内置 UA + 自定义、Referer 自动生成、Cookie、常用代理端口预设、
**429 等待重试 / 403 换 UA 重试**、随机请求间隔、结果去重。

---

## 环境要求

- **Windows 10 / 11**（依赖 tkinter 图形界面；其他系统没有测试）
- **Python 3.10 及以上**（开发、验证、打包都在 3.10.10 上进行）
- 需要能显示窗口的桌面环境（终端界面本身就是 tkinter 窗口）

## 安装依赖

至少装这几个，否则程序启动就会因为导入失败而退出：

```bash
pip install requests beautifulsoup4 pillow pygame aiohttp
```

再用到高级功能时补装：

```bash
pip install scrapy                          # ⑯ Scrapy 爬虫窗口
pip install playwright && playwright install chromium   # ⑮ 浏览器爬虫窗口
```

### 依赖明细

| 库 | 开发时版本 | 用途 | 不装会怎样 |
|---|---|---|---|
| `requests` | 2.34.2 | 经典爬虫、反爬重试 | **启动即报错** |
| `beautifulsoup4` | 4.15.0 | HTML 解析（经典 / 异步 / 浏览器三个窗口） | **启动即报错** |
| `pillow` | 12.3.0 | 图片查看器 | **启动即报错** |
| `pygame` | 2.6.1 | 音乐播放器 | **启动即报错** |
| `aiohttp` | 3.14.5 | 异步并发爬虫 | **启动即报错** |
| `scrapy` | 2.19.0 | Scrapy 爬虫（含 twisted 26.4、lxml 6.1、parsel 1.12 等） | 只在打开 Scrapy 窗口时报错 |
| `playwright` | 1.63.0 + Chromium | 浏览器爬虫 | 只在打开浏览器窗口时报错，并提示缺少依赖 |

> 前 5 个是**硬依赖**（模块顶层导入）；`scrapy` 和 `playwright` 是延迟导入，只在用到对应窗口时才需要。
> 版本列是开发时验证过的组合，写 README 时没有固定上限，装新版一般也能跑。

## 运行

```bash
python main.py
```

首次运行会在 `main.py` 同级目录自动创建 `data/` 和 `logs/`。

---

## 命令一览

**系统**

| 命令 | 作用 |
|---|---|
| `help` | 列出全部可用指令 |
| `manual` | 打开图形化指令大全 |
| `clear` | 清屏 |
| `exit` | 退出 |
| `disk` | 查看虚拟磁盘用量 |
| `guides` | 列出所有爬虫说明的概览 |
| `guide <名字>` | 查看某个爬虫的详细说明，如 `guide crawler` |

**爬虫**

| 命令 | 打开 |
|---|---|
| `crawler` | requests + bs4 经典爬虫 |
| `regex` | 正则专用爬虫 |
| `async` | aiohttp 异步高并发 |
| `browser` | Playwright 浏览器爬虫 |
| `scrapy` | Scrapy 高级爬虫（沙盒运行） |

**文件**

| 命令 | 作用 |
|---|---|
| `files` | 打开图形化文件管理器 |
| `ls` | 列出当前目录 |
| `cd <目录>` | 切换目录（支持 `cd ..`、`cd /`） |
| `mkdir <名字>` | 新建文件夹 |
| `touch <名字>` | 新建空文件 |
| `rm <名字>` | 删除文件或文件夹（递归） |
| `rename <原名> <新名>` | 重命名 |
| `edit <文件名>` | 用内置文本编辑器打开 |
| `view <文件名>` | 按扩展名预览图片 / 播放音乐 / 调起视频播放器 |
| `zip` | 打开压缩解压工具 |

---

## 数据存放

运行时产生的文件都在 `main.py` 同级目录，不会写入系统其他位置：

```
data/disk.db          虚拟磁盘（SQLite），删掉即重置成空盘
logs/2026-01-01.log   每天的运行日志
temp_scrapy/          Scrapy 沙盒：生成的 spider.py 和抓取结果，运行完自动清理
```

**不要把这些提交进 Git**（尤其 `data/disk.db`，里面是你真实抓过的数据）。

## 项目结构

单文件版，`main.py` 内按 ①–⑱ 顺序分成 18 个章节，原来分散在 `core/` 和 `ui/` 里的模块都在里面：

```
①  基础设施 · 日志系统        ②  虚拟磁盘 · SQLite 底座
③  虚拟磁盘 · 文件操作封装    ④  反爬工具 · 请求头与智能重试
⑤  爬虫说明文档 · 内置攻略    ⑥  Scrapy 代码生成器
⑦  图片查看器                 ⑧  音乐播放器
⑨  视频播放                   ⑩  图形化文件管理器
⑪  压缩 / 解压工具            ⑫  经典爬虫窗口（requests + bs4）
⑬  正则专用爬虫               ⑭  aiohttp 异步高并发爬虫
⑮  Playwright 浏览器爬虫      ⑯  Scrapy 高级爬虫（沙盒运行）
⑰  主终端                     ⑱  程序入口
```

改代码时直接在上面的章节标题里搜索定位，比按文件名翻更直观。

## 打包成 exe（可选）

```bash
pip install pyinstaller
pyinstaller --noconfirm --windowed --name CrawlerWorkbench main.py
```

- 产物在 `dist/CrawlerWorkbench/`，建议用 **onedir 模式**（默认），不要用 `--onefile`：
  Scrapy 窗口会以 `--scrapy-runner` 重新启动自身来跑子进程，单文件模式每次都要重新解压，很慢。
- Playwright 的 Chromium **不会**自动打包进去。想带上，先设 `PLAYWRIGHT_BROWSERS_PATH=0`
  再 `playwright install chromium`，并让 PyInstaller 收集整个 `playwright` 包。
- `data/`、`logs/`、`temp_scrapy/` 会建在 exe 旁边，所以**不要放到 `Program Files`** 这类只读目录。
- 如果打包后 Scrapy 窗口里看不到子进程日志，把 `--windowed` 去掉、改用控制台构建再试一次：
  无控制台时子进程的 stdout 可能拿不到。
- 打包后的 exe 体积较大（带 Scrapy 约 100MB 起，带 Chromium 还要再加 150MB 左右）。

---

## 使用须知

- 只抓你自己有权访问的内容；遵守目标网站的 `robots.txt` 和服务条款。
- 并发数、请求间隔都内置了保守默认值，别为了快把站点打死——被封 IP 的是你自己。
- 遇到 Cloudflare 等强反爬站的验证码，这五个窗口都过不去，这是浏览器自动化的能力边界。

## 重要声明
本程序借助DSH进行了对原先ui，app，core三大软件包的整合(三大软件包均未上传)，主要是为了方便上传至仓库。

## License

[MIT](LICENSE) © 2026 zzricls

第三方库遵循各自的许可（Scrapy/BSD、Playwright/Apache-2.0、pygame/LGPL、Pillow、aiohttp、requests 等），
打包分发时请一并保留它们的许可声明。
