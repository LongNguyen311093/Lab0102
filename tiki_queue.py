#!/usr/bin/env python3
"""
Tiki Product Scraper with Async DB & Checkpoint Resume
- Async HTTP requests with retries & backoff
- Async PostgreSQL inserts using asyncpg
- Checkpoint/resume via tiki_queue
- Deduplication and batch processing
- Retries handled via tiki_queue status (no new columns)
"""

import asyncio
import aiohttp
import asyncpg
import pandas as pd
import re
from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm
import logging
from configparser import ConfigParser

# -------------------- Config --------------------
INPUT_FILE = "product_id.csv"
API_URL = "https://api.tiki.vn/product-detail/api/v1/products/{}"

RETRIES = 5
CONCURRENT = 50
BATCH_SIZE = 5000

LOG_FILE = "errors.log"
logging.basicConfig(filename=LOG_FILE, level=logging.ERROR,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# -------------------- DB Config --------------------
def load_db_config(filename="database.ini", section="postgresql_tiki"):
    parser = ConfigParser()
    parser.read(filename)
    if parser.has_section(section):
        return {k: v for k, v in parser.items(section)}
    else:
        raise Exception(f"Section {section} not found in {filename}")

# -------------------- Custom Exception --------------------
class ProductNotFoundError(Exception):
    def __init__(self, pid):
        super().__init__(f"Product {pid} not found (404)")
        self.pid = pid

# -------------------- Text Cleaning --------------------
def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\b(p|img|id|style|src)\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"[^a-zA-Z0-9À-ỹà-ỹ\s.,!?():;\"'-]", " ", text)
    text = text.lower()
    return " ".join(text.split())

# -------------------- Queue Management --------------------
async def init_queue(conn, ids):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS tiki_queue (
            id BIGINT PRIMARY KEY,
            status VARCHAR(20) DEFAULT 'pending',
            last_error TEXT
        )
    """)
    records = [(pid,) for pid in ids]
    await conn.executemany("""
        INSERT INTO tiki_queue (id) VALUES ($1)
        ON CONFLICT (id) DO NOTHING
    """, records)

async def fetch_pending_or_error_ids(conn):
    rows = await conn.fetch("""
        SELECT id FROM tiki_queue
        WHERE status='pending' OR status='error'
    """)
    return [r['id'] for r in rows]

async def update_queue_status(conn, ids, status, error_message=None):
    if not ids:
        return
    if error_message:
        await conn.execute("""
            UPDATE tiki_queue SET status=$1, last_error=$2
            WHERE id = ANY($3::bigint[])
        """, status, error_message, ids)
    else:
        await conn.execute("""
            UPDATE tiki_queue SET status=$1, last_error=NULL
            WHERE id = ANY($2::bigint[])
        """, status, ids)

# -------------------- Async DB Inserts --------------------
async def insert_batch_to_db(conn, records, table_name):
    if not records:
        return
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id BIGINT PRIMARY KEY,
            name TEXT,
            url_key TEXT,
            price NUMERIC,
            description TEXT,
            image_url TEXT,
            missing_fields TEXT
        )
    """)
    await conn.executemany(f"""
        INSERT INTO {table_name} (id, name, url_key, price, description, image_url, missing_fields)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            url_key = EXCLUDED.url_key,
            price = EXCLUDED.price,
            description = EXCLUDED.description,
            image_url = EXCLUDED.image_url,
            missing_fields = EXCLUDED.missing_fields
    """, [(r.get("id"), r.get("name"), r.get("url_key"), r.get("price"),
           r.get("description"), r.get("image_url"), r.get("missing_fields")) for r in records])

# -------------------- Fetch Product --------------------
async def fetch_product(session, pid):
    url = API_URL.format(pid)
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(1, RETRIES+1):
        try:
            async with session.get(url, timeout=15, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    result = {
                        "id": pid,
                        "name": clean_text(data.get("name")),
                        "url_key": data.get("url_key"),
                        "price": data.get("price"),
                        "description": clean_text(data.get("description")),
                        "image_url": (data.get("images", [{}])[0].get("base_url") if data.get("images") else None)
                    }
                    missing = [k for k,v in result.items() if k!="id" and not v]
                    result["missing_fields"] = ",".join(missing) if missing else ""
                    return result
                elif resp.status == 404:
                    raise ProductNotFoundError(pid)
                elif resp.status in (429,500,502,503,504):
                    await asyncio.sleep(2*attempt)
                else:
                    logging.error(f"FAILED {pid}, status {resp.status}")
                    break
        except ProductNotFoundError:
            raise
        except Exception as e:
            logging.error(f"EXCEPTION {pid}, error {e}")
            await asyncio.sleep(2*attempt)
    return None

# -------------------- Process Batch --------------------
async def process_batch(conn, batch_ids):
    good_records, error_records, error_ids = [], [], []

    connector = aiohttp.TCPConnector(limit=CONCURRENT)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_product(session, pid) for pid in batch_ids]
        for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Batch"):
            try:
                r = await coro
                if r:
                    good_records.append(r)
            except ProductNotFoundError as e:
                error_records.append({
                    "id": e.pid,
                    "name": None,
                    "url_key": None,
                    "price": None,
                    "description": None,
                    "image_url": None,
                    "missing_fields": "all_fields"
                })
                error_ids.append(e.pid)
            except Exception as e:
                logging.error(f"Unknown exception: {e}")

    # insert results
    await insert_batch_to_db(conn, good_records, "tiki_product")
    await insert_batch_to_db(conn, error_records, "tiki_error")
    await update_queue_status(conn, [r["id"] for r in good_records], "done")
    await update_queue_status(conn, error_ids, "error", "404 Not Found")

    return len(good_records), len(error_records)

# -------------------- Main --------------------
async def main():
    db_config = load_db_config()
    conn = await asyncpg.connect(**db_config)

    # Load IDs from CSV
    df = pd.read_csv(INPUT_FILE)
    df.columns = df.columns.str.strip().str.lower()
    ids = df['id'].dropna().astype(int).tolist() if 'id' in df.columns else df.iloc[:,0].dropna().astype(int).tolist()
    ids = list(dict.fromkeys(ids))  # deduplicate

    # Initialize queue if empty
    queue_count = await conn.fetchval("SELECT COUNT(*) FROM tiki_queue")
    if queue_count == 0:
        await init_queue(conn, ids)
        print(f"Queue initialized with {len(ids)} IDs.")

    # Fetch pending/error IDs
    pending_ids = await fetch_pending_or_error_ids(conn)
    print(f"Total pending/error IDs: {len(pending_ids)}")

    total_good, total_error = 0, 0

    # Full-run progress bar
    for i in tqdm_asyncio(range(0, len(pending_ids), BATCH_SIZE), desc="Overall Progress"):
        batch_ids = pending_ids[i:i+BATCH_SIZE]
        good, error = await process_batch(conn, batch_ids)
        total_good += good
        total_error += error

    print("\n========== Crawl Summary ==========")
    print(f"Total Records Processed: {len(pending_ids)}")
    print(f"✅ Completed: {total_good}")
    print(f"❌ Errors: {total_error}")
    print("==================================\n")

    await conn.close()

# -------------------- Run --------------------
if __name__ == "__main__":
    asyncio.run(main())
