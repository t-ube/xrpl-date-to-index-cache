#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clio API を使った hourly キャッシュ生成スクリプト（ローカル版）

Clio サーバーの ledger_index コマンドを使って、
指定した日付範囲の 1時間ごとの ledger_index を取得し、
ローカルファイルに保存します。

使い方:
  python generate_hourly_clio_local.py ledger_cache_2025.json 2025-01-01 2025-12-31
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# ====== Clio 設定 ======
CLIO_URL = "https://s1.ripple.com:51234/"


def clio_ledger_index(dt: datetime) -> dict | None:
    """
    Clio の ledger_index コマンドを呼び出す
    
    Returns:
        {
            "ledger_index": int,
            "ledger_hash": str,
            "closed": str,  # ISO8601
            "validated": bool
        }
        または None（未来の日時など）
    """
    date_str = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    payload = {
        "method": "ledger_index",
        "params": [{
            "date": date_str
        }]
    }
    
    response = requests.post(CLIO_URL, json=payload, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    
    if "result" not in data:
        return None
    
    result = data["result"]
    
    if "error" in result:
        # lgrNotFound = 指定時刻のレジャーがない（未来など）
        if result.get("error") == "lgrNotFound":
            return None
        raise RuntimeError(f"Clio error: {result['error']} - {result.get('error_message', '')}")
    
    return result


def infer_year_from_path(path: str) -> int | None:
    """パスから年を推測"""
    m = re.search(r"(\d{4})", os.path.basename(path))
    if m:
        return int(m.group(1))
    return None


def make_empty_cache(path: str, dt_hint: datetime | None = None) -> dict:
    """新フォーマットの空キャッシュを生成"""
    year = infer_year_from_path(path)
    if year is None and dt_hint is not None:
        year = dt_hint.year
    if year is None:
        year = 0

    return {
        "meta": {
            "year": year,
            "version": 2,  # Clio版
        },
        "daily": {},
        "hourly": {},
    }


def load_cache(path: str) -> dict:
    """ローカルから既存のJSONキャッシュを読み込む"""
    if not Path(path).exists():
        print(f"{path} が存在しないため、新規作成として扱います。")
        return make_empty_cache(path)
    
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    
    print(f"ローカルから読み込み完了: {path}")

    if isinstance(raw, dict) and "meta" in raw and "hourly" in raw:
        return raw

    # 旧フォーマットからのマイグレーション
    print(f"{path} は旧フォーマットとみなし、新フォーマットへ変換します。")
    cache = make_empty_cache(path)
    if isinstance(raw, dict):
        for k, v in raw.items():
            if isinstance(k, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", k):
                cache["daily"][k] = v
    return cache


def save_cache(path: str, cache: dict) -> None:
    """キャッシュをローカルに保存"""
    meta = cache.get("meta", {})
    daily = cache.get("daily", {})
    hourly = cache.get("hourly", {})

    daily_sorted = {k: daily[k] for k in sorted(daily.keys())}
    hourly_sorted = {k: hourly[k] for k in sorted(hourly.keys())}

    out = {
        "meta": meta,
        "daily": daily_sorted,
        "hourly": hourly_sorted,
    }

    # ディレクトリがなければ作成
    parent = Path(path).parent
    if parent and not parent.exists():
        parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"ローカルに保存完了: {path} (daily={len(daily_sorted)}, hourly={len(hourly_sorted)})")


def generate_hourly_for_range(path: str, dt_start: datetime, dt_end: datetime) -> None:
    """
    Clio API を使って hourly キャッシュを生成
    """
    cache = load_cache(path)
    hourly: dict = cache.setdefault("hourly", {})

    # 現在時刻（UTC）を取得し、未来は処理しない
    now = datetime.now(timezone.utc)
    effective_end = min(dt_end, now)

    cur = dt_start
    total_hours = int(((effective_end - dt_start).total_seconds() // 3600) + 1)
    if total_hours < 1:
        print("処理対象の時間がありません（開始時刻が未来）")
        return

    processed = 0
    added = 0
    skipped_existing = 0

    print(f"{dt_start} ～ {effective_end} の hourly を {path} に生成します。")
    if dt_end > now:
        print(f"（元の終了日 {dt_end} は未来のため {effective_end} まで処理）")
    print(f"Clio サーバー: {CLIO_URL}")
    print("-" * 60)

    prev_date = None

    while cur <= effective_end:
        processed += 1
        current_date = cur.date()
        
        # hourly のキー: 2025-01-01T00:00:00Z 形式
        key_iso = cur.strftime("%Y-%m-%dT%H:%M:%SZ")

        if prev_date is None or current_date != prev_date:
            if prev_date is not None:
                print(f"日付が変わったため保存: {prev_date}")
                save_cache(path, cache)
            prev_date = current_date

        if key_iso in hourly:
            print(f"[{processed}/{total_hours}] {key_iso}: 既存 → スキップ")
            skipped_existing += 1
            cur += timedelta(hours=1)
            continue

        print(f"[{processed}/{total_hours}] {key_iso}: Clio 問い合わせ ... ", end="", flush=True)

        try:
            result = clio_ledger_index(cur)
            
            if result is None:
                print(f"⏭ データなし（終了）")
                break
            
            hourly[key_iso] = {
                "ledger_index": result["ledger_index"],
                "close_time": result["closed"],
            }
            added += 1
            print(f"✓ ledger={result['ledger_index']}, closed={result['closed']}")
            
            time.sleep(0.2)  # レート制限対策
            
        except Exception as e:
            print(f"✗ エラー: {e}")
            time.sleep(1)

        cur += timedelta(hours=1)

    print()
    print("=" * 60)
    print(f"処理完了:")
    print(f"  処理数: {processed}")
    print(f"  追加: {added}")
    print(f"  既存スキップ: {skipped_existing}")

    if added > 0:
        save_cache(path, cache)
    else:
        print("変更なしのため保存スキップ")


def parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("使い方:")
        print("  python generate_hourly_clio_local.py <path> <start_date> <end_date>")
        print("例:")
        print("  python generate_hourly_clio_local.py ledger_cache_2025.json 2025-01-01 2025-12-31")
        print("  python generate_hourly_clio_local.py cache/ledger_cache_2024.json 2024-01-01 2024-12-31")
        sys.exit(1)

    file_path = sys.argv[1]
    start_dt = parse_date(sys.argv[2])
    end_dt = parse_date(sys.argv[3]) + timedelta(days=1)  # 終了日の翌日0時まで

    generate_hourly_for_range(file_path, start_dt, end_dt)
