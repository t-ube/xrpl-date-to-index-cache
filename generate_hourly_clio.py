#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clio API を使った hourly キャッシュ生成スクリプト

Clio サーバーの ledger_index コマンドを使って、
指定した日付範囲の 1時間ごとの ledger_index を取得し、
R2 に保存します。

従来の二分探索方式と比べて：
- 1時間あたり 1回の API 呼び出しで済む（従来は 10〜20回）
- コードがシンプル
- Clio が返す値なので精度が保証される

環境変数:
  R2_ACCOUNT_ID: CloudflareアカウントID
  R2_ACCESS_KEY_ID: R2のアクセスキーID
  R2_SECRET_ACCESS_KEY: R2のシークレットアクセスキー
  R2_BUCKET_NAME: バケット名

使い方:
  python generate_hourly_clio.py ledger_cache_2025.json 2025-01-01 2025-12-31
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from io import BytesIO

import boto3
import requests
from botocore.config import Config

# ====== Clio 設定 ======
CLIO_URL = "https://s1.ripple.com:51234/"

# ====== R2 設定 ======
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "")


def get_r2_client():
    """R2用のboto3クライアントを取得"""
    if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
        raise ValueError(
            "R2の認証情報が設定されていません。\n"
            "環境変数 R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY を設定してください。"
        )
    
    return boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
        ),
        region_name="auto",
    )


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


def infer_year_from_key(key: str) -> int | None:
    """キーから年を推測"""
    m = re.search(r"(\d{4})", os.path.basename(key))
    if m:
        return int(m.group(1))
    return None


def make_empty_cache(key: str, dt_hint: datetime | None = None) -> dict:
    """新フォーマットの空キャッシュを生成"""
    year = infer_year_from_key(key)
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


def load_cache(key: str) -> dict:
    """R2から既存のJSONキャッシュを読み込む"""
    s3 = get_r2_client()
    
    try:
        response = s3.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        raw = json.loads(response["Body"].read().decode("utf-8"))
        print(f"R2から読み込み完了: {key}")
    except s3.exceptions.NoSuchKey:
        print(f"{key} がR2に存在しないため、新規作成として扱います。")
        return make_empty_cache(key)
    except Exception as e:
        if "NoSuchKey" in str(e) or "404" in str(e):
            print(f"{key} がR2に存在しないため、新規作成として扱います。")
            return make_empty_cache(key)
        raise

    if isinstance(raw, dict) and "meta" in raw and "hourly" in raw:
        return raw

    # 旧フォーマットからのマイグレーション
    print(f"{key} は旧フォーマットとみなし、新フォーマットへ変換します。")
    cache = make_empty_cache(key)
    if isinstance(raw, dict):
        for k, v in raw.items():
            if isinstance(k, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", k):
                cache["daily"][k] = v
    return cache


def save_cache(key: str, cache: dict) -> None:
    """キャッシュをR2に保存"""
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

    json_bytes = json.dumps(out, ensure_ascii=False, indent=2).encode("utf-8")
    
    s3 = get_r2_client()
    s3.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=key,
        Body=BytesIO(json_bytes),
        ContentType="application/json",
    )

    print(f"R2に保存完了: {key} (daily={len(daily_sorted)}, hourly={len(hourly_sorted)})")


def generate_hourly_for_range(key: str, dt_start: datetime, dt_end: datetime) -> None:
    """
    Clio API を使って hourly キャッシュを生成
    """
    cache = load_cache(key)
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

    print(f"{dt_start} ～ {effective_end} の hourly を {key} に生成します。")
    if dt_end > now:
        print(f"（元の終了日 {dt_end} は未来のため {effective_end} まで処理）")
    print(f"Clio サーバー: {CLIO_URL}")
    print("-" * 60)

    while cur <= effective_end:
        processed += 1
        
        # hourly のキー: 2025-01-01T00:00:00Z 形式
        key_iso = cur.strftime("%Y-%m-%dT%H:%M:%SZ")

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
        save_cache(key, cache)
    else:
        print("変更なしのため保存スキップ")


def parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("使い方:")
        print("  python generate_hourly_clio.py <r2_key> <start_date> <end_date>")
        print("例:")
        print("  python generate_hourly_clio.py ledger_cache_2025.json 2025-01-01 2025-12-31")
        print()
        print("環境変数:")
        print("  R2_ACCOUNT_ID       - CloudflareアカウントID")
        print("  R2_ACCESS_KEY_ID    - R2のアクセスキーID")
        print("  R2_SECRET_ACCESS_KEY - R2のシークレットアクセスキー")
        print("  R2_BUCKET_NAME      - バケット名")
        sys.exit(1)

    r2_key = sys.argv[1]
    start_dt = parse_date(sys.argv[2])
    end_dt = parse_date(sys.argv[3]) + timedelta(days=1)  # 終了日の翌日0時まで

    generate_hourly_for_range(r2_key, start_dt, end_dt)
