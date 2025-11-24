#!/usr/bin/env python3
"""
Coleman async downloader
────────────────────────
- Nhập mã sản phẩm thủ công, queue cho đến khi gõ 'yes'
- Tải ảnh + mô tả (JP) + dịch sang TIẾNG VIỆT
"""

import asyncio
import logging
import re
import sys
from pathlib import Path

import aiofiles

# Import tkinter cho file picker (có sẵn trong Python)
try:
    import tkinter as tk
    from tkinter import filedialog
    HAS_TKINTER = True
except ImportError:
    HAS_TKINTER = False
import aiohttp
from aiohttp import CookieJar
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm_asyncio
from deep_translator import GoogleTranslator

# ─────────── CONFIG ───────────
MAX_CONN = 10                  # tổng socket đồng thời
MAX_RETRIES = 3                # số lần thử lại khi lỗi
SLIDE_RANGE = range(1, 16)     # slide 1-15
SRC_LANG, DST_LANG = "ja", "vi"
translator = GoogleTranslator(source=SRC_LANG, target=DST_LANG)

# ─────────── LOGGING ───────────
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-8s %(message)s",
    handlers=[logging.StreamHandler(),
              logging.FileHandler("download.log", encoding="utf-8")]
)
log = logging.getLogger("coleman")


# ─────────── NETWORK HELPERS ───────────
def get_headers(referer: str | None = None) -> dict:
    """Tạo headers giống browser để tránh 403"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none" if not referer else "same-origin",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }
    if referer:
        headers["Referer"] = referer
    return headers

async def fetch_html(product_id: str, session: aiohttp.ClientSession, 
                     retries: int = MAX_RETRIES) -> str | None:
    url = f"https://ec.coleman.co.jp/item/{product_id}.html"
    headers = get_headers()
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers, allow_redirects=True) as r:
                if r.status == 200:
                    return await r.text()
                elif r.status == 404:
                    log.error("%s – không tìm thấy (404)", product_id)
                    return None
                elif r.status == 403:
                    log.warning("%s – HTTP 403 Forbidden (lần thử %d/%d) - Có thể bị chặn bởi server", 
                              product_id, attempt + 1, retries)
                else:
                    log.warning("%s – HTTP %s (lần thử %d/%d)", product_id, r.status, attempt + 1, retries)
        except asyncio.TimeoutError:
            log.warning("%s – timeout (lần thử %d/%d)", product_id, attempt + 1, retries)
        except Exception as e:
            log.warning("%s – lỗi: %s (lần thử %d/%d)", product_id, e, attempt + 1, retries)
        
        if attempt < retries - 1:
            # Thêm delay để tránh rate limiting, tăng dần theo số lần thử
            delay = 2 ** attempt + 0.5  # Thêm 0.5s base delay
            await asyncio.sleep(delay)
    
    log.error("%s – thất bại sau %d lần thử", product_id, retries)
    return None


async def download_image(url: str, path: Path,
                         session: aiohttp.ClientSession,
                         product_id: str, slide: int,
                         sem: asyncio.Semaphore,
                         retries: int = MAX_RETRIES):
    # Kiểm tra file đã tồn tại và có kích thước hợp lệ
    if path.exists():
        try:
            if path.stat().st_size > 0:
                return  # File đã tồn tại và có dữ liệu
        except OSError:
            pass  # File có thể bị lỗi, tải lại
    
    async with sem:
        # Headers cho ảnh
        headers = get_headers(referer=f"https://ec.coleman.co.jp/item/{product_id}.html")
        headers.update({
            "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            "Sec-Fetch-Dest": "image",
            "Sec-Fetch-Mode": "no-cors",
        })
        
        for attempt in range(retries):
            try:
                async with session.get(url, headers=headers) as r:
                    if r.status == 200:
                        data = await r.read()
                        if len(data) > 0:  # Kiểm tra dữ liệu không rỗng
                            async with aiofiles.open(path, "wb") as f:
                                await f.write(data)
                            return  # Thành công
                        else:
                            log.warning("%s – slide %d: dữ liệu rỗng", product_id, slide)
                    elif r.status == 404:
                        log.warning("%s – slide %d không tồn tại (404)", product_id, slide)
                        return  # Không cần thử lại cho 404
                    else:
                        log.warning("%s – slide %d HTTP %s (lần thử %d/%d)", 
                                  product_id, slide, r.status, attempt + 1, retries)
            except asyncio.TimeoutError:
                log.warning("%s – slide %d timeout (lần thử %d/%d)", 
                          product_id, slide, attempt + 1, retries)
            except Exception as e:
                log.warning("%s – lỗi slide %d: %s (lần thử %d/%d)", 
                          product_id, slide, e, attempt + 1, retries)
            
            if attempt < retries - 1:
                await asyncio.sleep(1 * (attempt + 1))  # Linear backoff
        
        log.error("%s – slide %d thất bại sau %d lần thử", product_id, slide, retries)

# ─────────── CORE ───────────
async def translate_text(text: str) -> str:
    """Dịch text trong thread pool để không block event loop"""
    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(None, translator.translate, text)
    except Exception as e:
        log.error("Lỗi dịch: %s", e)
        return text  # Trả về text gốc nếu lỗi

async def save_descriptions(soup: BeautifulSoup, out_dir: Path,
                            product_id: str):
    ul = soup.find("ul", class_="p-item_info_indt")
    if not ul:
        log.warning("%s – không tìm <ul>", product_id)
        return
    jp_lines = [li.get_text(strip=True) for li in ul.find_all("li") if li.get_text(strip=True)]
    if not jp_lines:
        log.warning("%s – <ul> rỗng", product_id)
        return

    # JP
    jp_path = out_dir / f"{product_id}.jp.txt"
    async with aiofiles.open(jp_path, "w", encoding="utf-8") as f:
        await f.write("\n".join(jp_lines))
    
    # VI - dịch song song để tăng tốc
    vi_tasks = [translate_text(line) for line in jp_lines]
    vi_lines = await asyncio.gather(*vi_tasks)
    
    vi_path = out_dir / f"{product_id}.vi.txt"
    async with aiofiles.open(vi_path, "w", encoding="utf-8") as f:
        await f.write("\n".join(vi_lines))
    log.info("%s – đã lưu jp.txt & vi.txt", product_id)

async def handle_product(pid: str, session: aiohttp.ClientSession,
                         sem: asyncio.Semaphore):
    html = await fetch_html(pid, session)
    if html is None:
        return
    soup = BeautifulSoup(html, "html.parser")
    out_dir = Path(pid)
    out_dir.mkdir(exist_ok=True)

    # 1) Mô tả
    await save_descriptions(soup, out_dir, pid)

    # 2) Ảnh
    tasks = []
    for slide in SLIDE_RANGE:
        tag = soup.find(attrs={"data-slide": str(slide)})
        img_tag = tag.find("img") if tag else None
        src = img_tag["src"] if img_tag and img_tag.has_attr("src") else None
        if not src:
            log.warning("%s – thiếu slide %d", pid, slide)
            continue
        if src.startswith("//"):
            src = "https:" + src
        elif src.startswith("/"):
            src = "https://ec.coleman.co.jp" + src
        path = out_dir / f"{slide}.jpg"
        tasks.append(download_image(src, path, session, pid, slide, sem))

    if tasks:
        await tqdm_asyncio.gather(*tasks, desc=pid, unit="img")
    else:
        log.warning("%s – không có ảnh", pid)

# ─────────── MAIN ───────────
async def main(product_ids: list[str]):
    sem = asyncio.Semaphore(MAX_CONN)
    # Tối ưu connector: tăng limit_per_host để tải ảnh nhanh hơn
    connector = aiohttp.TCPConnector(
        limit=MAX_CONN * 2,  # Tăng tổng connection pool
        limit_per_host=MAX_CONN,  # Cho phép nhiều connection hơn đến cùng host
        ttl_dns_cache=300,  # Cache DNS 5 phút
        force_close=False  # Tái sử dụng connection
    )

    timeout = aiohttp.ClientTimeout(total=180, connect=30)
    
    # Headers mặc định cho session (sẽ được override bởi headers cụ thể trong mỗi request)
    default_headers = get_headers()
    
    # Sử dụng CookieJar để lưu cookies và giữ session
    cookie_jar = CookieJar(unsafe=True)  # unsafe=True để chấp nhận cookies từ mọi domain
    
    async with aiohttp.ClientSession(
        connector=connector, 
        timeout=timeout,
        headers=default_headers,
        cookie_jar=cookie_jar
    ) as sess:
        # Warm-up: request đến trang chủ để lấy cookies ban đầu
        try:
            log.info("Đang khởi tạo session...")
            async with sess.get("https://ec.coleman.co.jp/", headers=get_headers()) as r:
                if r.status == 200:
                    log.info("✓ Session đã được khởi tạo")
                else:
                    log.warning("⚠️  Warm-up request trả về HTTP %s", r.status)
        except Exception as e:
            log.warning("⚠️  Lỗi warm-up: %s (tiếp tục...)", e)
        
        tasks = [handle_product(pid, sess, sem) for pid in product_ids]
        await tqdm_asyncio.gather(*tasks, desc="Tổng tiến độ", unit="sản phẩm")

# ─────────── FILE PARSING ───────────
def select_file_dialog() -> str | None:
    """
    Mở hộp thoại chọn file txt.
    Trả về đường dẫn file hoặc None nếu hủy.
    """
    if not HAS_TKINTER:
        return None
    
    try:
        # Tạo root window ẩn
        root = tk.Tk()
        root.withdraw()  # Ẩn cửa sổ chính
        root.attributes('-topmost', True)  # Đưa lên trên cùng
        
        # Mở file dialog
        file_path = filedialog.askopenfilename(
            title="Chọn file txt chứa mã sản phẩm",
            filetypes=[
                ("Text files", "*.txt"),
                ("All files", "*.*")
            ],
            initialdir="."  # Bắt đầu từ thư mục hiện tại
        )
        
        root.destroy()  # Đóng root window
        
        return file_path if file_path else None
    
    except Exception as e:
        log.error("Lỗi mở file dialog: %s", e)
        return None

def parse_product_ids_from_file(file_path: str) -> list[str]:
    """
    Đọc mã sản phẩm từ file txt.
    Hỗ trợ các định dạng: dấu phẩy, xuống dòng, hoặc dấu cách
    """
    try:
        path = Path(file_path)
        if not path.exists():
            log.error("File không tồn tại: %s", file_path)
            return []
        
        content = path.read_text(encoding="utf-8")
        if not content.strip():
            log.warning("File rỗng: %s", file_path)
            return []
        
        # Tách theo nhiều delimiter: dấu phẩy, xuống dòng, dấu cách
        # Sử dụng regex để tách theo tất cả các delimiter
        ids = re.split(r'[,\s\n]+', content)
        
        # Lọc và làm sạch: chỉ lấy số, loại bỏ rỗng
        product_ids = []
        for item in ids:
            cleaned = item.strip()
            if cleaned and cleaned.isdigit():
                product_ids.append(cleaned)
        
        # Loại bỏ trùng lặp nhưng giữ nguyên thứ tự
        seen = set()
        unique_ids = []
        for pid in product_ids:
            if pid not in seen:
                seen.add(pid)
                unique_ids.append(pid)
        
        log.info("Đã đọc %d mã sản phẩm từ file %s", len(unique_ids), file_path)
        return unique_ids
    
    except Exception as e:
        log.error("Lỗi đọc file %s: %s", file_path, e)
        return []

if __name__ == "__main__":
    queue: list[str] = []
    
    print("=" * 50)
    print("Coleman Product Downloader")
    print("=" * 50)
    print("Chọn chế độ:")
    print("  1. Nhập mã thủ công")
    print("  2. Đọc từ file txt")
    print("=" * 50)
    
    mode = input("Chọn (1 hoặc 2): ").strip()
    
    if mode == "2":
        # Đọc từ file
        print("\nChọn file txt:")
        print("  1. Chọn file từ hộp thoại (khuyến nghị)")
        print("  2. Nhập đường dẫn thủ công")
        print("  3. Dùng file mặc định 'products.txt'")
        
        choice = input("\nChọn (1/2/3): ").strip()
        file_path = None
        
        if choice == "1":
            # Chọn file từ dialog
            if not HAS_TKINTER:
                print("⚠️  tkinter không khả dụng, chuyển sang nhập thủ công...")
                file_path = input("\nNhập đường dẫn file txt: ").strip()
                if not file_path:
                    print("❌ Không có đường dẫn. Thoát.")
                    exit(1)
            else:
                print("\n📂 Đang mở hộp thoại chọn file...")
                file_path = select_file_dialog()
                if not file_path:
                    print("❌ Không chọn file. Thoát.")
                    exit(0)
                print(f"✓ Đã chọn: {file_path}")
        
        elif choice == "2":
            # Nhập đường dẫn thủ công
            file_path = input("\nNhập đường dẫn file txt: ").strip()
            if not file_path:
                print("❌ Không có đường dẫn. Thoát.")
                exit(1)
        
        elif choice == "3":
            # Dùng file mặc định
            file_path = "products.txt"
            print(f"✓ Sử dụng file mặc định: {file_path}")
        
        else:
            print("❌ Lựa chọn không hợp lệ. Thoát.")
            exit(1)
        
        queue = parse_product_ids_from_file(file_path)
        if not queue:
            print(f"❌ Không đọc được mã nào từ file '{file_path}'")
            print("   Kiểm tra lại đường dẫn và định dạng file.")
            exit(1)
        
        print(f"\n✓ Đã đọc {len(queue)} mã sản phẩm từ file '{file_path}'")
        print(f"  Danh sách: {', '.join(queue[:10])}{'...' if len(queue) > 10 else ''}")
        confirm = input("\nBắt đầu tải? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("Đã hủy.")
            exit(0)
    
    elif mode == "1":
        # Nhập thủ công
        print("\nNhập mã sản phẩm (mỗi mã một dòng), gõ 'yes' để bắt đầu tải:")
        while True:
            s = input("> ").strip()
            if s.lower() == "yes":
                break
            elif s.isdigit():
                if s not in queue:  # Tránh trùng lặp
                    queue.append(s)
                    print(f"✓ đã thêm {s} (tổng: {len(queue)})")
                else:
                    print(f"⚠️  {s} đã có trong danh sách")
            else:
                print("⚠️  chỉ nhập số hoặc 'yes'.")
    
    else:
        print("❌ Lựa chọn không hợp lệ. Thoát.")
        exit(1)

    if not queue:
        print("Chưa có mã nào ➜ thoát.")
    else:
        print(f"\n🚀 Bắt đầu tải {len(queue)} sản phẩm...\n")
        asyncio.run(main(queue))
        print("\n🎉  Xong! Kiểm tra các thư mục sản phẩm và file download.log.")
