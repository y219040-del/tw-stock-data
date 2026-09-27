#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股資料爬蟲（FinMind 版）

資料來源：FinMind API (https://api.finmindtrade.com)
- 未登入額度：300 次請求 / 小時
- 本程式用固定間隔（13 秒/次）控制請求速率，換算約 277 次/小時，留有安全餘裕
- 若被 FinMind 暫時封鎖 IP（403），會直接安全結束，等下一次排程（6 小時後）自動重試
- 支援跨執行續傳：進度存在 output/progress.json，下次執行會接著跑未完成的股票

用法：
    TIME_BUDGET_MINUTES=340 python3 tw_stock_scraper.py
"""

import os
import sys
import json
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta

# ---------- 設定 ----------
BASE_URL = "https://api.finmindtrade.com/api/v4/data"
DATASET_INFO = "TaiwanStockInfo"
DATASET_PRICE = "TaiwanStockPrice"

OUTPUT_DIR = "output"
PRICE_DIR = os.path.join(OUTPUT_DIR, "prices")
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "progress.json")
LOG_FILE = "scraper.log"

START_DATE = (datetime.today() - timedelta(days=365 * 10)).strftime("%Y-%m-%d")

REQUEST_INTERVAL_SECONDS = 13   # 3600/13 ≈ 277 次/hr，低於 300 次/hr 上限留安全餘裕
MAX_RETRY_PER_STOCK = 3         # 單一股票遇到暫時性錯誤時的重試次數

TIME_BUDGET_MINUTES = float(os.environ.get("TIME_BUDGET_MINUTES", "340"))

# ---------- 記錄設定 ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


def ensure_dirs():
    os.makedirs(PRICE_DIR, exist_ok=True)


def load_progress():
    """讀取進度檔；若不存在，回傳 None 代表需要重新初始化股票清單"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def save_progress(progress):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def fetch_stock_list():
    """取得全部台股代碼清單（含上市、上櫃）"""
    log.info("首次執行，向 FinMind 取得股票清單（%s）...", DATASET_INFO)
    resp = requests.get(BASE_URL, params={"dataset": DATASET_INFO}, timeout=30)
    resp.raise_for_status()
    data = resp.json().get("data", [])
    stock_ids = sorted({row["stock_id"] for row in data if row.get("stock_id")})
    log.info("取得 %d 檔股票代碼", len(stock_ids))
    return stock_ids


def fetch_price(stock_id):
    """向 FinMind 要單一股票從 START_DATE 到今天的所有日線資料"""
    params = {
        "dataset": DATASET_PRICE,
        "data_id": stock_id,
        "start_date": START_DATE,
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    return resp


def save_price_csv(stock_id, rows):
    if not rows:
        log.info("[%s] 無資料，略過存檔", stock_id)
        return
    df = pd.DataFrame(rows)
    path = os.path.join(PRICE_DIR, f"{stock_id}.csv")
    df.to_csv(path, index=False, encoding="utf-8-sig")
    log.info("[%s] 已存 %d 筆 → %s", stock_id, len(df), path)


def main():
    ensure_dirs()
    start_time = time.monotonic()
    deadline_seconds = TIME_BUDGET_MINUTES * 60

    progress = load_progress()
    if progress is None:
        all_ids = fetch_stock_list()
        progress = {"all_stock_ids": all_ids, "done": [], "failed": []}
        save_progress(progress)

    all_ids = progress["all_stock_ids"]
    done = set(progress["done"])
    failed = set(progress.get("failed", []))
    pending = [s for s in all_ids if s not in done and s not in failed]

    log.info(
        "進度：共 %d 檔，已完成 %d，失敗略過 %d，待抓 %d",
        len(all_ids), len(done), len(failed), len(pending),
    )

    if not pending:
        log.info("所有股票資料皆已抓取完成，本次無需執行。")
        return

    for stock_id in pending:
        elapsed = time.monotonic() - start_time
        if elapsed > deadline_seconds:
            log.info("已達時間預算（%.1f 分鐘），安全中斷，剩餘 %d 檔留給下次排程。",
                      TIME_BUDGET_MINUTES, len(pending) - pending.index(stock_id))
            break

        retry = 0
        while retry <= MAX_RETRY_PER_STOCK:
            try:
                resp = fetch_price(stock_id)
            except requests.RequestException as e:
                retry += 1
                log.warning("[%s] 網路錯誤：%s，重試 %d/%d", stock_id, e, retry, MAX_RETRY_PER_STOCK)
                time.sleep(5 * retry)
                continue

            if resp.status_code == 200:
                rows = resp.json().get("data", [])
                save_price_csv(stock_id, rows)
                done.add(stock_id)
                progress["done"] = sorted(done)
                save_progress(progress)
                break

            elif resp.status_code == 402:
                # 超過本小時額度上限，等待到下個整點再繼續
                log.warning("[%s] 額度已滿（402），暫停等待下個小時...", stock_id)
                time.sleep(65 * 60 - (time.time() % 3600))
                continue

            elif resp.status_code == 403:
                # IP 被暫時封鎖，直接安全結束，交給下一次排程（6小時後）處理
                log.error("[%s] IP 被暫時封鎖（403），本次執行安全結束，等待下次排程重試。", stock_id)
                save_progress(progress)
                return

            else:
                # 其他 4xx/5xx：記為失敗，避免卡住整個流程，之後可人工檢查
                log.error("[%s] 請求失敗，status=%d，body=%s", stock_id, resp.status_code, resp.text[:200])
                failed.add(stock_id)
                progress["failed"] = sorted(failed)
                save_progress(progress)
                break

        time.sleep(REQUEST_INTERVAL_SECONDS)

    remaining = len(all_ids) - len(done) - len(failed)
    log.info("本次執行結束。已完成 %d / %d 檔，尚餘 %d 檔待下次排程繼續。",
              len(done), len(all_ids), remaining)


if __name__ == "__main__":
    main()

