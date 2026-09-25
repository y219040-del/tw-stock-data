#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
台股全市場歷史資料爬蟲
========================
抓取範圍:
  - 上市股票 (TWSE)：使用證交所 STOCK_DAY API，每次抓一檔股票、一個月的資料
  - 上櫃股票 (TPEx)：使用櫃買中心 st43 API，每次抓一檔股票、一個月的資料

輸出：
  - ./output/twse/<股票代號>.csv   (上市個股)
  - ./output/tpex/<股票代號>.csv   (上櫃個股)
  - ./output/all_stocks_merged.csv (可選，最後合併成一個大檔案)
  - ./output/stock_list.csv        (完整股票清單，代號/名稱/市場別)

特色：
  - 具備斷點續傳：已經抓過的股票/月份會自動跳過，可以中斷後重新執行
  - 內建延遲與重試機制，避免被證交所/櫃買中心封鎖
  - 完成後可選擇自動上傳到 Dropbox (需先安裝 dropbox套件並設定 ACCESS_TOKEN)

使用方式：
  1. pip install requests pandas dropbox
  2. 依需求調整下方「設定區」的參數
  3. python3 tw_stock_scraper.py

需要抓「全部台股 x 近10年」，資料量非常大：
  - 上市約 1000+ 檔、上櫃約 800+ 檔，共約 1800 檔
  - 每檔股票需要抓 120 個月 (10年)
  - 總請求數約 1800 x 120 ≈ 21 萬次，若每次間隔 2 秒，全部跑完大約需要 4-5 天
  - 建議先用 TEST_MODE 或縮小 YEARS_BACK 測試流程沒問題後，再跑正式全量

若你想大幅縮短時間，可以只抓「上市」或縮小 YEARS_BACK，或用多台機器/多組IP分工。
"""

import os
import csv
import time
import json
import random
import datetime
import logging
from pathlib import Path

import requests
import pandas as pd

# ============================================================
# 設定區 — 依你的需求調整
# ============================================================

# 要抓幾年的資料(從今天回推)
YEARS_BACK = 10

# 輸出資料夾
OUTPUT_DIR = Path("./output")

# 是否包含上市(TWSE)、上櫃(TPEx)
INCLUDE_TWSE = True
INCLUDE_TPEX = True

# 每次 API 請求之間的延遲秒數(避免被鎖 IP，太小容易被 ban)
REQUEST_DELAY_SEC = 2.0
REQUEST_DELAY_JITTER = 1.0  # 額外隨機延遲 0~1 秒

# 遇到錯誤時最大重試次數
MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 10

# 測試模式：True 的話只抓前 N 檔股票、最近 3 個月，方便先驗證流程
TEST_MODE = False
TEST_STOCK_LIMIT = 5
TEST_MONTHS = 3

# 是否在全部抓完後自動上傳到 Dropbox(本機執行才需要，GitHub Actions 版本不使用這個)
UPLOAD_TO_DROPBOX = False
DROPBOX_ACCESS_TOKEN = ""  # 在 https://www.dropbox.com/developers/apps 建立 App 後取得
DROPBOX_TARGET_FOLDER = "/台股歷史資料"

# 單次執行的時間預算(分鐘)。GitHub Actions 免費方案單次工作最長跑 6 小時，
# 這裡設定成 340 分鐘(約5小時40分)，跑到時間快到就安全中斷、存檔，
# 下次排程觸發時會自動從斷點繼續。本機執行可以設 None 代表不限制。
TIME_BUDGET_MINUTES = int(os.environ.get("TIME_BUDGET_MINUTES", "340"))

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

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


def sleep_politely():
    time.sleep(REQUEST_DELAY_SEC + random.uniform(0, REQUEST_DELAY_JITTER))


def request_with_retry(url, params=None, timeout=15):
    """帶重試機制的 GET 請求"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as e:
            log.warning(f"請求失敗 (第{attempt}次): {url} params={params} 錯誤: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SEC)
            else:
                log.error(f"放棄請求: {url} params={params}")
                return None


# ============================================================
# 第一步：取得完整股票清單 (代號、名稱、市場別)
# ============================================================
def fetch_stock_list():
    """
    從證交所 ISIN 查詢頁面取得上市 + 上櫃股票清單。
    strMode=2 → 上市 (TWSE)
    strMode=4 → 上櫃 (TPEx)
    只保留股票(不含 ETF權證等，但保留一般股票/ETF可自行調整篩選條件)
    """
    stock_list = []
    isin_url = "https://isin.twse.com.tw/isin/C_public.jsp"

    modes = []
    if INCLUDE_TWSE:
        modes.append(("2", "TWSE"))
    if INCLUDE_TPEX:
        modes.append(("4", "TPEX"))

    for mode, market in modes:
        log.info(f"正在取得 {market} 股票清單...")
        resp = request_with_retry(isin_url, params={"strMode": mode})
        if resp is None:
            continue
        resp.encoding = "big5"
        tables = pd.read_html(resp.text)
        df = tables[0]
        # 第一列是欄位標題，重新設定欄名
        df.columns = df.iloc[0]
        df = df.iloc[1:]

        for _, row in df.iterrows():
            code_name = str(row.iloc[0]).strip()
            if "\u3000" not in code_name and " " not in code_name:
                continue
            parts = code_name.replace("\u3000", " ").split(" ", 1)
            if len(parts) != 2:
                continue
            code, name = parts[0].strip(), parts[1].strip()
            # 只保留 4 碼數字的一般股票代號(排除權證、期貨等雜項)
            if not code.isdigit() or len(code) != 4:
                continue
            stock_list.append({"code": code, "name": name, "market": market})

        sleep_politely()

    df_list = pd.DataFrame(stock_list).drop_duplicates(subset=["code"])
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_list.to_csv(OUTPUT_DIR / "stock_list.csv", index=False, encoding="utf-8-sig")
    log.info(f"股票清單取得完成，共 {len(df_list)} 檔，已存到 stock_list.csv")
    return df_list


# ============================================================
# 第二步：抓取單一上市股票(TWSE)的月資料
# ============================================================
def fetch_twse_month(stock_code, year, month):
    """
    TWSE STOCK_DAY API：一次回傳「該股票、該月份」的每日資料
    """
    date_str = f"{year}{month:02d}01"
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
    resp = request_with_retry(
        url, params={"response": "json", "date": date_str, "stockNo": stock_code}
    )
    if resp is None:
        return []
    try:
        data = resp.json()
    except Exception:
        return []
    if data.get("stat") != "OK" or "data" not in data:
        return []

    rows = []
    for row in data["data"]:
        # 欄位: 日期, 成交股數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, 漲跌價差, 成交筆數
        try:
            roc_date = row[0]  # 例如 113/09/02
            y, m, d = roc_date.split("/")
            greg_date = f"{int(y) + 1911}-{m}-{d}"
            rows.append(
                {
                    "date": greg_date,
                    "stock_code": stock_code,
                    "volume": row[1].replace(",", ""),
                    "turnover": row[2].replace(",", ""),
                    "open": row[3],
                    "high": row[4],
                    "low": row[5],
                    "close": row[6],
                    "change": row[7],
                    "transactions": row[8].replace(",", ""),
                }
            )
        except Exception:
            continue
    return rows


# ============================================================
# 第三步：抓取單一上櫃股票(TPEx)的月資料
# ============================================================
def fetch_tpex_month(stock_code, year, month):
    """
    TPEx 櫃買中心月成交行情 API (民國年格式)
    """
    roc_year = year - 1911
    date_str = f"{roc_year}/{month:02d}"
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
    resp = request_with_retry(
        url,
        params={"l": "zh-tw", "d": date_str, "stkno": stock_code},
    )
    if resp is None:
        return []
    try:
        data = resp.json()
    except Exception:
        return []
    if "aaData" not in data or not data["aaData"]:
        return []

    rows = []
    for row in data["aaData"]:
        # 欄位: 日期, 成交股數, 成交金額, 開盤, 最高, 最低, 收盤, 漲跌, 筆數
        try:
            roc_date = row[0]  # 例如 113/09/02
            y, m, d = roc_date.split("/")
            greg_date = f"{int(y) + 1911}-{m}-{d}"
            rows.append(
                {
                    "date": greg_date,
                    "stock_code": stock_code,
                    "volume": row[1].replace(",", ""),
                    "turnover": row[2].replace(",", ""),
                    "open": row[3],
                    "high": row[4],
                    "low": row[5],
                    "close": row[6],
                    "change": row[7],
                    "transactions": row[8].replace(",", "") if len(row) > 8 else "",
                }
            )
        except Exception:
            continue
    return rows


# ============================================================
# 第四步：針對單一股票，抓取整個時間範圍(逐月)，並存成 CSV
# ============================================================
def month_range(years_back):
    today = datetime.date.today()
    months = []
    y, m = today.year, today.month
    total_months = years_back * 12
    for _ in range(total_months):
        months.append((y, m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return list(reversed(months))  # 由舊到新


def scrape_one_stock(code, name, market, months):
    out_subdir = OUTPUT_DIR / ("twse" if market == "TWSE" else "tpex")
    out_subdir.mkdir(parents=True, exist_ok=True)
    out_path = out_subdir / f"{code}.csv"

    # 斷點續傳：檢查已經抓過哪些月份
    done_dates = set()
    if out_path.exists():
        try:
            existing = pd.read_csv(out_path, dtype=str)
            done_dates = set(existing["date"].str[:7])  # yyyy-mm
        except Exception:
            existing = pd.DataFrame()
    else:
        existing = pd.DataFrame()

    all_rows = [] if existing.empty else existing.to_dict("records")

    for year, month in months:
        ym_key = f"{year}-{month:02d}"
        if ym_key in done_dates:
            continue  # 已經抓過，跳過

        if market == "TWSE":
            rows = fetch_twse_month(code, year, month)
        else:
            rows = fetch_tpex_month(code, year, month)

        if rows:
            all_rows.extend(rows)
            log.info(f"[{market}][{code} {name}] {ym_key} 取得 {len(rows)} 筆")
        else:
            log.info(f"[{market}][{code} {name}] {ym_key} 無資料(可能未上市/當月無交易)")

        sleep_politely()

    if all_rows:
        df_out = pd.DataFrame(all_rows).drop_duplicates(subset=["date"])
        df_out = df_out.sort_values("date")
        df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
        log.info(f"[{market}][{code} {name}] 已存檔: {out_path} (共 {len(df_out)} 筆)")


# ============================================================
# 第五步：主流程
# ============================================================
def main():
    start_time = time.monotonic()
    deadline = None
    if TIME_BUDGET_MINUTES:
        deadline = start_time + TIME_BUDGET_MINUTES * 60
        log.info(f"本次執行時間預算：{TIME_BUDGET_MINUTES} 分鐘，時間到會安全中斷並保留進度。")

    log.info("=== 台股全市場歷史資料爬蟲開始 ===")

    # 股票清單也快取起來，避免每次執行都重新爬一次 ISIN 頁面
    stock_list_path = OUTPUT_DIR / "stock_list.csv"
    if stock_list_path.exists():
        stock_list = pd.read_csv(stock_list_path, dtype=str)
        log.info(f"使用已快取的股票清單，共 {len(stock_list)} 檔")
    else:
        stock_list = fetch_stock_list()

    months = month_range(YEARS_BACK)
    if TEST_MODE:
        stock_list = stock_list.head(TEST_STOCK_LIMIT)
        months = months[-TEST_MONTHS:]
        log.info(f"*** 測試模式開啟：只抓 {len(stock_list)} 檔股票、{len(months)} 個月 ***")

    total = len(stock_list)
    for i, row in enumerate(stock_list.itertuples(), 1):
        if deadline and time.monotonic() >= deadline:
            log.warning(
                f"已達本次時間預算上限，於第 {i}/{total} 檔股票前安全停止。"
                "已完成的資料都已存檔，下次排程執行會自動接續。"
            )
            return

        log.info(f"進度 {i}/{total}：{row.market} {row.code} {row.name}")
        try:
            scrape_one_stock(row.code, row.name, row.market, months)
        except KeyboardInterrupt:
            log.warning("使用者中斷，已儲存進度，下次執行會自動從斷點繼續。")
            return
        except Exception as e:
            log.error(f"{row.code} 發生未預期錯誤: {e}")
            continue

    log.info("=== 全部股票抓取完成 ===")

    if UPLOAD_TO_DROPBOX:
        upload_all_to_dropbox()


# ============================================================
# 選用：抓完後自動上傳整個 output 資料夾到 Dropbox
# ============================================================
def upload_all_to_dropbox():
    try:
        import dropbox
    except ImportError:
        log.error("尚未安裝 dropbox 套件，請先執行: pip install dropbox")
        return

    if not DROPBOX_ACCESS_TOKEN:
        log.error("請先在設定區填入 DROPBOX_ACCESS_TOKEN")
        return

    dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
    log.info("開始上傳到 Dropbox...")

    for local_path in OUTPUT_DIR.rglob("*.csv"):
        rel_path = local_path.relative_to(OUTPUT_DIR)
        dropbox_path = f"{DROPBOX_TARGET_FOLDER}/{rel_path.as_posix()}"
        with open(local_path, "rb") as f:
            content = f.read()
        try:
            dbx.files_upload(
                content, dropbox_path, mode=dropbox.files.WriteMode.overwrite
            )
            log.info(f"已上傳: {dropbox_path}")
        except Exception as e:
            log.error(f"上傳失敗 {dropbox_path}: {e}")
        time.sleep(0.5)

    log.info("Dropbox 上傳完成")


if __name__ == "__main__":
    main()
