#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股全市場歷史資料爬蟲 (Yahoo Finance 版)
========================================
改版原因：
  原本直接打台灣證交所(TWSE) API 的版本，在 GitHub Actions 上會被證交所的
  地區防火牆擋掉（非台灣IP直接回傳擋頁），所以改用 Yahoo Finance 當資料源：
  - 全球都能存取，不會被地區防火牆擋
  - 一檔股票一次 API 呼叫就能拿到完整多年資料（不用像原本那樣一個月一個月抓）
  - 股票清單改用 twstock 套件內建的資料（不用再打證交所的網頁）

輸出：
  - ./output/<股票代號>_<股票名稱>.csv   （每檔股票一個檔案，欄位：Date, Open, High, Low, Close, Volume）
  - ./output/stock_list.csv              （完整股票清單：代號/名稱/市場別）

使用方式：
  1. pip install twstock yfinance pandas requests
  2. 依需求調整下方「設定區」的參數
  3. python3 tw_stock_scraper.py
"""

import os
import time
import random
import datetime
import logging
from pathlib import Path

import pandas as pd
import twstock
import yfinance as yf

# ============================================================
# 設定區 — 依你的需求調整
# ============================================================

# 要抓幾年的資料。Yahoo Finance 的 period 參數只接受特定字串，
# 這裡會自動對應到最接近的合法值：1y,2y,5y,10y,max
YEARS_BACK = 10

# 輸出資料夾
OUTPUT_DIR = Path("./output")

# 是否包含上市(TWSE, 代號後綴 .TW)、上櫃(TPEx, 代號後綴 .TWO)
INCLUDE_TWSE = True
INCLUDE_TPEX = True

# 每次抓取之間的延遲秒數（避免被 Yahoo Finance 短時間限流 HTTP 429）
REQUEST_DELAY_SEC = 1.2
REQUEST_DELAY_JITTER = 0.8

# 遇到錯誤時最大重試次數
MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 15

# 測試模式：True 的話只抓前 N 檔股票，方便先驗證流程
TEST_MODE = False
TEST_STOCK_LIMIT = 5

# 單次執行的時間預算(分鐘)。GitHub Actions 免費方案單次工作最長跑 6 小時，
# 這裡設定成 340 分鐘(約5小時40分)，跑到時間快到就安全中斷、存檔，
# 下次排程觸發時會自動從斷點繼續。本機執行可以設 None 代表不限制。
TIME_BUDGET_MINUTES = int(os.environ.get("TIME_BUDGET_MINUTES", "340"))

# 已經抓過、檔案存在就跳過（除非你想強制全部重抓）
SKIP_EXISTING = True

# ============================================================
# 記錄檔設定
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scraper.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def sleep_politely():
    time.sleep(REQUEST_DELAY_SEC + random.uniform(0, REQUEST_DELAY_JITTER))


def years_to_period(years):
    """把想要的年數對應到 yfinance 合法的 period 字串"""
    valid = [1, 2, 5, 10]
    for v in valid:
        if years <= v:
            return f"{v}y"
    return "max"


PERIOD = years_to_period(YEARS_BACK)


# ============================================================
# 第一步：取得完整股票清單（改用 twstock 內建資料，不再打證交所網頁）
# ============================================================
def fetch_stock_list():
    log.info("正在從 twstock 套件取得股票清單（使用套件內建資料，不會被防火牆擋）...")

    rows = []
    for code, info in twstock.codes.items():
        if info.type != "股票":
            continue
        if info.market == "上市" and not INCLUDE_TWSE:
            continue
        if info.market == "上櫃" and not INCLUDE_TPEX:
            continue
        if info.market not in ("上市", "上櫃"):
            continue
        if not code.isdigit():
            continue

        suffix = ".TW" if info.market == "上市" else ".TWO"
        rows.append(
            {
                "code": code,
                "name": info.name,
                "market": info.market,
                "yahoo_symbol": f"{code}{suffix}",
            }
        )

    df_list = pd.DataFrame(rows).drop_duplicates(subset=["code"])
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_list.to_csv(OUTPUT_DIR / "stock_list.csv", index=False, encoding="utf-8-sig")
    log.info(f"股票清單取得完成，共 {len(df_list)} 檔，已存到 stock_list.csv")
    return df_list


# ============================================================
# 第二步：用 yfinance 抓單一股票的歷史資料
# ============================================================
def fetch_one_stock(yahoo_symbol, period):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            ticker = yf.Ticker(yahoo_symbol)
            df = ticker.history(period=period, auto_adjust=False)
            if df is None or df.empty:
                return None
            df = df.reset_index()
            # 只保留常用欄位，日期轉成純日期字串
            df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
            keep_cols = [c for c in ["Date", "Open", "High", "Low", "Close", "Volume"] if c in df.columns]
            return df[keep_cols]
        except Exception as e:
            log.warning(f"[{yahoo_symbol}] 第{attempt}次抓取失敗: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SEC)
    log.error(f"[{yahoo_symbol}] 已達最大重試次數，放棄。")
    return None


def safe_filename(code, name):
    # 股票名稱可能含有斜線等符號，簡單清理一下避免存檔失敗
    safe_name = "".join(c for c in name if c not in r'\/:*?"<>|')
    return f"{code}_{safe_name}.csv"


# ============================================================
# 第三步：主流程
# ============================================================
def main():
    start_time = time.monotonic()
    deadline = None
    if TIME_BUDGET_MINUTES:
        deadline = start_time + TIME_BUDGET_MINUTES * 60
        log.info(f"本次執行時間預算：{TIME_BUDGET_MINUTES} 分鐘，時間到會安全中斷並保留進度。")

    log.info("=== 台股全市場歷史資料爬蟲開始 (Yahoo Finance 版) ===")
    log.info(f"抓取範圍：近 {YEARS_BACK} 年 (yfinance period={PERIOD})")

    stock_list_path = OUTPUT_DIR / "stock_list.csv"
    if stock_list_path.exists():
        stock_list = pd.read_csv(stock_list_path, dtype=str)
        log.info(f"使用已快取的股票清單，共 {len(stock_list)} 檔")
    else:
        stock_list = fetch_stock_list()

    if TEST_MODE:
        stock_list = stock_list.head(TEST_STOCK_LIMIT)
        log.info(f"*** 測試模式開啟：只抓 {len(stock_list)} 檔股票 ***")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    total = len(stock_list)
    done_count = 0
    skip_count = 0
    fail_count = 0

    for i, row in enumerate(stock_list.itertuples(), 1):
        if deadline and time.monotonic() >= deadline:
            log.warning(
                f"已達本次時間預算上限，於第 {i}/{total} 檔股票前安全停止。"
                "已完成的資料都已存檔，下次排程執行會自動接續。"
            )
            break

        out_path = OUTPUT_DIR / safe_filename(row.code, row.name)
        if SKIP_EXISTING and out_path.exists():
            skip_count += 1
            continue

        log.info(f"進度 {i}/{total}：{row.market} {row.code} {row.name} ({row.yahoo_symbol})")
        df = fetch_one_stock(row.yahoo_symbol, PERIOD)

        if df is not None and not df.empty:
            df.to_csv(out_path, index=False, encoding="utf-8-sig")
            done_count += 1
            log.info(f"[{row.code} {row.name}] 已存檔，共 {len(df)} 筆")
        else:
            fail_count += 1
            log.info(f"[{row.code} {row.name}] 無資料或抓取失敗")

        sleep_politely()

    log.info(
        f"=== 本次執行結束：新完成 {done_count} 檔，跳過已存在 {skip_count} 檔，"
        f"失敗 {fail_count} 檔（共 {total} 檔）==="
    )


if __name__ == "__main__":
    main()
