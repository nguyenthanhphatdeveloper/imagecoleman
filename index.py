#!/usr/bin/env python3
"""
Coleman async downloader
────────────────────────
- Nhập mã sản phẩm thủ công, queue cho đến khi gõ 'yes'
- Tải ảnh + mô tả (JP) + dịch sang TIẾNG VIỆT
"""

import asyncio
import logging
from pathlib import Path

import aiofiles
import aiohttp
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
async def fetch_html(product_id: str, session: aiohttp.ClientSession, 
                     retries: int = MAX_RETRIES) -> str | None:
    url = f"https://ec.coleman.co.jp/item/{product_id}.html"
    for attempt in range(retries):
        try:
            async with session.get(url) as r:
                if r.status == 200:
                    return await r.text()
                elif r.status == 404:
                    log.error("%s – không tìm thấy (404)", product_id)
                    return None
                log.warning("%s – HTTP %s (lần thử %d/%d)", product_id, r.status, attempt + 1, retries)
        except asyncio.TimeoutError:
            log.warning("%s – timeout (lần thử %d/%d)", product_id, attempt + 1, retries)
        except Exception as e:
            log.warning("%s – lỗi: %s (lần thử %d/%d)", product_id, e, attempt + 1, retries)
        
        if attempt < retries - 1:
            await asyncio.sleep(2 ** attempt)  # exponential backoff
    
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
        for attempt in range(retries):
            try:
                async with session.get(url) as r:
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
    
    async with aiohttp.ClientSession(
        connector=connector, 
        timeout=timeout,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    ) as sess:
        tasks = [handle_product(pid, sess, sem) for pid in product_ids]
        await tqdm_asyncio.gather(*tasks, desc="Tổng tiến độ", unit="sản phẩm")

if __name__ == "__main__":
    # 1) Nhập queue
    queue: list[str] = []
    print("Nhập mã sản phẩm, gõ 'yes' để bắt đầu tải:")
    while True:
        s = input("> ").strip().lower()
        if s == "yes":
            break
        elif s.isdigit():
            queue.append(s)
            print(f"✓ đã thêm {s}")
        else:
            print("⚠️  chỉ nhập số hoặc 'yes'.")

    if not queue:
        print("Chưa có mã nào ➜ thoát.")
    else:
        asyncio.run(main(queue))
        print("\n🎉  Xong! Kiểm tra các thư mục sản phẩm và file download.log.")
