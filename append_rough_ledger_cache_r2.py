#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
既存の ledger_cache_XXXX.json に対して、
指定した日付範囲の「存在しない日だけ」ラフ値を追記するスクリプト。

Cloudflare R2 対応版:
- R2バケットから直接読み書き
- 環境変数で認証情報を設定

環境変数:
  R2_ACCOUNT_ID: CloudflareアカウントID
  R2_ACCESS_KEY_ID: R2のアクセスキーID
  R2_SECRET_ACCESS_KEY: R2のシークレットアクセスキー
  R2_BUCKET_NAME: バケット名

使い方:
  python append_rough_ledger_cache.py ledger_cache_2025.json 2025-01-01 2025-12-31
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from io import BytesIO

import boto3
from botocore.config import Config
from xrpl.clients import JsonRpcClient
from xrpl.models.requests import Ledger

# ====== 環境設定 ======
GENESIS_INDEX = 32570
RIPPLE_EPOCH = 946684800  # 2000-01-01T00:00:00Z
JSON_RPC_URL = "https://xrplcluster.com/"
client = JsonRpcClient(JSON_RPC_URL)

# ====== R2設定 ======
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


class FutureLedgerError(Exception):
    """指定日時に対応するレジャーがまだ存在しない場合に使う例外"""
    pass


def ripple_time_to_datetime(ripple_time: int) -> datetime:
    """Rippleエポック秒 → Python datetime (UTC)"""
    return datetime.fromtimestamp(ripple_time + RIPPLE_EPOCH, tz=timezone.utc)


def infer_year_from_path(path: str) -> int | None:
    """
    ファイルパスから 4桁の年を推測 (例: ledger_cache_2025.json → 2025)。
    見つからなければ None。
    """
    m = re.search(r"(\d{4})", os.path.basename(path))
    if m:
        return int(m.group(1))
    return None


def make_empty_cache(path: str, dt_hint: datetime | None = None) -> dict:
    """
    新フォーマットの空キャッシュを生成する。
    year は path から推測し、無ければ dt_hint.year、さらに無ければ 0。
    """
    year = infer_year_from_path(path)
    if year is None and dt_hint is not None:
        year = dt_hint.year
    if year is None:
        year = 0

    return {
        "meta": {
            "year": year,
            "version": 1,
        },
        "daily": {},
        "hourly": {},
    }


def migrate_old_flat_format(old: dict, path: str) -> dict:
    """
    旧フォーマット (トップレベルが "YYYY-MM-DD" キーの dict) を
    新フォーマットに変換する。
    """
    cache = make_empty_cache(path)
    daily = cache["daily"]

    for k, v in old.items():
        # "YYYY-MM-DD" っぽいキーだけ拾う
        if isinstance(k, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", k):
            daily[k] = v

    return cache


def load_cache(key: str) -> dict:
    """
    R2から既存のJSONキャッシュを読み込む。
    - ファイルが無ければ新フォーマットの空キャッシュを返す。
    - 旧フォーマットだった場合は新フォーマットへマイグレーションする。
    
    key: R2上のオブジェクトキー (例: "ledger_cache_2025.json")
    """
    s3 = get_r2_client()
    
    try:
        response = s3.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        raw = json.loads(response["Body"].read().decode("utf-8"))
        print(f"R2から読み込み完了: {key}")
    except s3.exceptions.NoSuchKey:
        print(f"{key} がR2に存在しないため、新規作成として扱います。")
        return make_empty_cache(key)
    except Exception as e:
        # ClientError などでオブジェクトが見つからない場合
        if "NoSuchKey" in str(e) or "404" in str(e):
            print(f"{key} がR2に存在しないため、新規作成として扱います。")
            return make_empty_cache(key)
        raise

    # すでに新フォーマットっぽい
    if isinstance(raw, dict) and "meta" in raw and "daily" in raw:
        # hourly が無ければ足しておく
        raw.setdefault("hourly", {})
        return raw

    # 古いフラット形式と見なしてマイグレーション
    print(f"{key} は旧フォーマットとみなされるため、新フォーマットへ変換します。")
    return migrate_old_flat_format(raw, key)


def save_cache(key: str, cache: dict) -> None:
    """
    キャッシュを日付順・日時順にソートしてR2に保存。
    cache は新フォーマット前提:
      { "meta": {...}, "daily": {...}, "hourly": {...} }
    
    key: R2上のオブジェクトキー (例: "ledger_cache_2025.json")
    """
    meta = cache.get("meta", {})
    daily = cache.get("daily", {})
    hourly = cache.get("hourly", {})

    # daily はキー (YYYY-MM-DD) でソート
    daily_sorted = {k: daily[k] for k in sorted(daily.keys())}

    # hourly は ISO文字列キーでソート
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


def get_ledger_index_by_date(dt: datetime, max_iter: int = 5) -> tuple[int, datetime]:
    """
    指定日時に最も近い ledger index をラフに推定する。

    - dt が最新レジャーの close_time より未来なら FutureLedgerError を投げる
    - それ以外は「誤差1時間以内」のラフ値を返す（後段で補正前提）
    """
    latest = client.request(Ledger(ledger_index="validated", expand=True)).result
    latest_index = int(latest["ledger_index"])
    latest_time = ripple_time_to_datetime(latest["ledger"]["close_time"])

    # 🟡 ここで「未来日付」を判定
    if dt > latest_time:
        raise FutureLedgerError(
            f"target datetime {dt.isoformat()} is newer than latest ledger close_time {latest_time.isoformat()}"
        )

    # 初期推定: 4秒/ledger と仮定
    delta = (latest_time - dt).total_seconds()
    guess_index = latest_index - int(delta / 4)

    close_time = latest_time  # fallback

    for i in range(max_iter):
        res = client.request(Ledger(ledger_index=guess_index, expand=True)).result
        close_time = ripple_time_to_datetime(res["ledger"]["close_time"])

        diff = (close_time - dt).total_seconds()
        print(f"  ↳ iter {i+1}: ledger={guess_index}, close_time={close_time}, diff={diff/3600:.2f}h")

        if abs(diff) < 3600:
            return guess_index, close_time

        guess_index -= int(diff / 4)
        if guess_index < 1:
            guess_index = 1

        time.sleep(0.3)

    return guess_index, close_time


def get_ledger_index_by_date_binary(dt: datetime,
                                    tol_seconds: int = 3600,
                                    max_iter: int = 40) -> tuple[int, datetime]:
    target_date = dt.date()

    # 最新レジャー
    latest = client.request(Ledger(ledger_index="validated", expand=True)).result
    hi_idx = int(latest["ledger_index"])
    hi_time = ripple_time_to_datetime(latest["ledger"]["close_time"])

    # 未来チェック
    if dt > hi_time:
        raise FutureLedgerError(
            f"target datetime {dt.isoformat()} is newer than latest ledger close_time {hi_time.isoformat()}"
        )

    # GENESIS レジャー
    genesis = client.request(Ledger(ledger_index=GENESIS_INDEX, expand=True)).result
    lo_idx = GENESIS_INDEX
    lo_time = ripple_time_to_datetime(genesis["ledger"]["close_time"])

    # GENESIS より前をどう扱うか：ここは仕様次第
    if dt <= lo_time:
        # 1) 強制的に GENESIS を返す
        return GENESIS_INDEX, lo_time

    best_idx = lo_idx
    best_time = lo_time

    for i in range(max_iter):
        mid = (lo_idx + hi_idx) // 2
        res = client.request(Ledger(ledger_index=mid, expand=True)).result
        mid_time = ripple_time_to_datetime(res["ledger"]["close_time"])
        mid_date = mid_time.date()

        # 一番近いものを記憶しておく
        if abs((mid_time - dt).total_seconds()) < abs((best_time - dt).total_seconds()):
            best_idx = mid
            best_time = mid_time

        diff = (mid_time - dt).total_seconds()
        print(f"  ↳ iter {i+1}: ledger={mid}, close_time={mid_time}, diff={diff/3600:.2f}h")

        if mid_date == target_date:
            return mid, mid_time

        if mid_time < dt:
            lo_idx = mid + 1
        else:
            hi_idx = mid - 1

        if lo_idx > hi_idx:
            break

        time.sleep(0.3)

    return best_idx, best_time


def append_rough_ledger_cache(key: str, dt_start: datetime, dt_end: datetime) -> None:
    """
    R2上の key で指定された JSON キャッシュ（新フォーマット）に対して、
    [dt_start, dt_end] の日付範囲で「存在しない daily だけ」ラフ値を追加する。
    変更があった場合のみ、最後に1回保存する。
    """
    cache = load_cache(key)
    daily: dict = cache.setdefault("daily", {})
    cache.setdefault("hourly", {})  # まだ使わないが念のため確保

    cur = dt_start
    total_days = (dt_end.date() - dt_start.date()).days + 1
    processed = 0
    added = 0

    print(f"{dt_start.date()} ～ {dt_end.date()} の不足分を {key} の daily に追記します。")

    while cur <= dt_end:
        processed += 1
        date_key = cur.strftime("%Y-%m-%d")

        if date_key in daily:
            print(f"[{processed}/{total_days}] {date_key}: daily 既存エントリあり → スキップ")
        else:
            print(f"[{processed}/{total_days}] {date_key}: daily 追加処理開始")
            try:
                idx, close_time = get_ledger_index_by_date(cur)
                daily[date_key] = {
                    "ledger_index": idx,
                    "close_time": close_time.isoformat().replace("+00:00", "Z"),
                }
                added += 1
                print(f"   ↳ 追加: ledger={idx}, close_time={close_time}")
                time.sleep(1)  # 成功時だけウェイト
            except FutureLedgerError as e:
                # まだレジャーが存在しない（未来日付）の場合は「正常スキップ」とみなす
                print(f"   ⏭ 未来日付のためスキップ: {e}")
                # この日以降は全部未来確定なのでループ終了でよい
                break
            except Exception as e:
                print(f"   追加失敗（別原因）: {e}")
                time.sleep(3)

        cur += timedelta(days=1)

    print(f"\n 処理完了: {processed} 日中 {added} 日を daily に追加")
    
    # 変更があった場合のみ保存
    if added > 0:
        save_cache(key, cache)
    else:
        print("変更なしのため保存スキップ")


def parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("使い方:")
        print("  python append_rough_ledger_cache.py <r2_key> <start_date> <end_date>")
        print("例:")
        print("  python append_rough_ledger_cache.py ledger_cache_2025.json 2025-01-01 2025-12-31")
        print()
        print("環境変数:")
        print("  R2_ACCOUNT_ID       - CloudflareアカウントID")
        print("  R2_ACCESS_KEY_ID    - R2のアクセスキーID")
        print("  R2_SECRET_ACCESS_KEY - R2のシークレットアクセスキー")
        print("  R2_BUCKET_NAME      - バケット名")
        sys.exit(1)

    r2_key = sys.argv[1]
    start_dt = parse_date(sys.argv[2])
    end_dt = parse_date(sys.argv[3])

    append_rough_ledger_cache(r2_key, start_dt, end_dt)
