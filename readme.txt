# Tiki API Data to PostgreSQL

[![Python](https://img.shields.io/badge/Built%20With-Python-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![Data Source](https://img.shields.io/badge/API-Tiki-orange)](https://tiki.vn/)
[![Database](https://img.shields.io/badge/Database-PostgreSQL-blueviolet)](https://www.postgresql.org/)
[![Process Manager](https://img.shields.io/badge/Managed%20By-Supervisor-lightgrey)](http://supervisord.org/)


---

## Overview

This repository provides a Python-based script (main.py) that fetches product data from the Tiki API and saves it into a PostgreSQL database.
Designed for stability, resumability, and scalability, capable of processing hundreds of thousands of API calls while handling errors gracefully.

---

## Features

- 🔄 Reads product IDs from a CSV input file
- 🚀 Fetches live product data from Tiki API endpoints
- 🧾 Saves results directly to PostgreSQL
- ⚠️ Logs all failed requests and exceptions
- ♻️ Supports resuming partially completed runs
- ⚙️ Compatible with Supervisord for continuous background operation 

---

## Project Structure

| File                   | Description                                       |
| ---------------------- | ------------------------------------------------- |
| `main.py` 🐍           | Main Python script to fetch API data              |
| `product_id.csv` 📄    | Input list of product IDs                         |
| `database.ini` 🗄️     | PostgreSQL configuration file                     |
| `requirements.txt` 📦  | Python dependencies                               |
| `supervisord.conf` 🛠️ | Supervisor configuration for background execution |

---

## Installation

Clone the repository:

```bash
git clone https://github.com/ndlryan/API-Data-with-Postgres.git
cd API-Data-with-Postgres
```

Install dependencies:
```bash
pip install -r requirements.txt
```

---

## Running the Crawler

Run directly from terminal:

```bash
python main.py
```

This will:
1. Load product IDs from product_id.csv
2. Fetch product details from Tiki API
3. Save results into PostgreSQL
4. Record any failed requests or exceptions in logs

---

## Process Management with Supervisord
For long-running or auto-restarting crawls, you can manage the crawler with Supervisord.

### 1. Install Supervisor

```bash
pip install supervisor
```

### 2. Create Configuration File

```ini
[unix_http_server]
file=/tmp/supervisor.sock

[supervisord]
logfile=supervisord.log
pidfile=/tmp/supervisord.pid
childlogdir=./logs

[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[supervisorctl]
serverurl=unix:///tmp/supervisor.sock

[program:api_data]
command=python3 /path/to/API-Data-with-Postgres/main.py
directory=/path/to/API-Data-with-Postgres
autostart=true
autorestart=true
stderr_logfile=./logs/api_data.err.log
stdout_logfile=./logs/api_data.out.log
```
  - 🔧 Replace /path/to/API-Data-Crawling with your actual project path.

### 3. Start and Monitor

```bash
supervisord -c supervisord.conf
supervisorctl -c supervisord.conf status
```

Restart or stop the crawler anytime:
```bash
supervisorctl -c supervisord.conf restart api_data
supervisorctl -c supervisord.conf stop api_data
```

---

## Logs and Outputs

Database: PostgreSQL (records inserted from API)

Error logs: Logs exceptions or 404 errors

Supervisor logs: Stored under ./logs/ when using supervisord.conf

---

## Summary

```markdown
EST. Runtime: ~1h
Total Processed: 200,000
    - Good Records (Including missing field ones) = 198,942
    - Exceptions (404 - Not found) = 1,058
```

---

## Notes

Ensure database.ini has correct credentials

Running multiple times does not duplicate records

Use Supervisor to prevent downtime or data loss

---

## Author

**Ryan**  
[GitHub Profile](https://github.com/ndlryan)

A robust, fault-tolerant Tiki API data loader — lightweight, automated, and production-ready.
