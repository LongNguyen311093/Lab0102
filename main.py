#!/usr/bin/env python3

import asyncio
import aiohttp
from tqdm import tqdm
import re
import pandas as pd
from configparser import ConfigParser
import asyncpg

# -------------------- CONFIG --------------------
INPUT_FILE = "product_id.csv"  # <-- CSV input
API_URL = "https://api.tiki.vn/product-detail/api/v1/products/{}"
CONCURRENT = 20
BATCH_SIZE = 5000
RETRIES = 5
FIELDS = ["id", "name", "url_key", "price", "description", "image_url", "missing_fields"]

DB_FILE = "database.ini"
DB_SECTION = "postgresql_tiki"

# -------------------- DB --------------------
def load_db_config(filename=DB_FILE, section=DB_SECTION):
    parser = ConfigParser()
    parser.read(filename)
    if not parser.has_section(section):
        raise Exception(f"Section {section} not found in {filename}")
    return {k: v for k, v in parser.items(section)}

async def get_pg_pool():
    db_config = load_db_config()
    return await asyncpg.create_pool(**db_config)

# -------------------- TEXT CLEAN --------------------
def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\b(p|img|id|style|src)\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"[^a-zA-Z0-9À-ỹà-ỹ\s.,!?():;\"'-]", " ", text)
    text = text.lower()
    text = " ".join(text.split())
    return text

# -------------------- INIT QUEUE --------------------
async def init_queue(pool, input_file=INPUT_FILE):
    df = pd.read_csv(input_file)
    df.columns = df.columns.str.strip().str.lower()
    if "id" in df.columns:
        ids = df["id"].dropna().astype(int).tolist()
    else:
        ids = df.iloc[:, 0].dropna().astype(int).tolist()
    ids = list(dict.fromkeys(ids))  # Deduplicate quietly

    async with pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO tiki_queue (id, status) VALUES ($1,'pending') ON CONFLICT(id) DO NOTHING",
            [(pid,) for pid in ids]
        )

# -------------------- FETCH QUEUE --------------------
async def fetch_pending_ids(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id FROM tiki_queue WHERE status='pending'")
        return [r["id"] for r in rows]

async def update_queue_status(pool, ids, status, error_message=None):
    if not ids:
        return
    async with pool.acquire() as conn:
        if error_message:
            await conn.executemany(
                "UPDATE tiki_queue SET status=$1, last_error=$2 WHERE id=$3",
                [(status, error_message, pid) for pid in ids]
            )
        else:
            await conn.executemany(
                "UPDATE tiki_queue SET status=$1, last_error=NULL WHERE id=$2",
                [(status, pid) for pid in ids]
            )

# -------------------- INSERT --------------------
async def insert_batch(pool, records, table_name):
    if not records:
        return
    async with pool.acquire() as conn:
        values = [(r.get("id"), r.get("name"), r.get("url_key"), r.get("price"),
                   r.get("description"), r.get("image_url"), r.get("missing_fields")) for r in records]
        query = f"""
        INSERT INTO {table_name} (id, name, url_key, price, description, image_url, missing_fields)
        VALUES($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT(id) DO UPDATE SET
            name=EXCLUDED.name,
            url_key=EXCLUDED.url_key,
            price=EXCLUDED.price,
            description=EXCLUDED.description,
            image_url=EXCLUDED.image_url,
            missing_fields=EXCLUDED.missing_fields
        """
        await conn.executemany(query, values)

# -------------------- FETCH PRODUCT --------------------
async def fetch_product(session, pid, semaphore, retry_counter):
    url = API_URL.format(pid)
    async with semaphore:
        for attempt in range(RETRIES):
            try:
                async with session.get(url, timeout=15) as resp:
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
                        missing = [k for k, v in result.items() if k != "id" and (v is None or v == "")]
                        result["missing_fields"] = ",".join(missing) if missing else ""
                        return result
                    elif resp.status in (429, 500, 502, 503, 504):
                        await asyncio.sleep(2*(attempt+1))
                    else:
                        break
            except Exception:
                pass  # silently ignore, will count as error
        retry_counter[pid] = retry_counter.get(pid, 0) + 1
        return {field: None if field != "id" else pid for field in FIELDS}

# -------------------- PROCESS BATCH --------------------
async def process_batch(batch_ids, semaphore):
    connector = aiohttp.TCPConnector(limit=CONCURRENT)
    retry_counter = {}
    good_records = []
    error_records = []

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_product(session, pid, semaphore, retry_counter) for pid in batch_ids]
        for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Batch", ncols=80):
            result = await coro
            if result and all(result[k] is None for k in FIELDS if k != "id"):
                error_records.append(result)
            else:
                good_records.append(result)
    return good_records, error_records, retry_counter

# -------------------- MAIN --------------------
async def main():
    pool = await get_pg_pool()
    semaphore = asyncio.Semaphore(CONCURRENT)

    # Init queue from CSV input
    await init_queue(pool, INPUT_FILE)

    pending_ids = await fetch_pending_ids(pool)
    print(f"Total pending IDs: {len(pending_ids)}")

    all_good = []
    all_error = []

    for i in range(0, len(pending_ids), BATCH_SIZE):
        batch = pending_ids[i:i+BATCH_SIZE]
        print(f"\n🚀 Processing batch {i//BATCH_SIZE + 1} ({len(batch)} items)...")
        good, error, retries = await process_batch(batch, semaphore)

        all_good.extend(good)
        all_error.extend(error)

        await insert_batch(pool, good, "tiki_product")
        await insert_batch(pool, error, "tiki_error")
        await update_queue_status(pool, [r["id"] for r in good], "done")
        await update_queue_status(pool, [r["id"] for r in error], "error", "Failed after retries")

    # -------------------- SUMMARY --------------------
    print("\n========== Crawl Summary ==========")
    print(f"Total Records Processed: {len(pending_ids)}")
    print(f"✅ Completed: {len(all_good)}")
    print(f"❌ Errors: {len(all_error)}")
    print("==================================\n")

# -------------------- RUN --------------------
if __name__ == "__main__":
    asyncio.run(main())
