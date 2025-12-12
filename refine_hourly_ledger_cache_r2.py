#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
hourly の精緻化スクリプト（daily + 直前 hourly を下限・上限として使う精緻版）
Cloudflare R2 対応版

指定した ledger_cache_YYYY.json（新フォーマット）について、
指定した日付範囲の 1時間ごとの ledger_index を XRPL から引いて
hourly セクションを埋める。

仕様のポイント:
- daily は「ラフな1日1本のレジャー」であり、その日の最終レジャーとは限らない
- ただし「前日の daily と翌日の daily の間にある」ことは保証される前提とする
- 各時間の hourly は、
    - 下限 = max(前日の daily ledger_index, 直前の hourly ledger_index)
    - 上限 = 翌日の daily ledger_index（または十分先）
  とするインデックス範囲内で二分探索し、
  「close_time >= target_dt」を満たす最小の ledger_index とする
- 精緻条件は「target_dt と同時刻のレジャー」か「その直後の最初のレジャー」のみに限定
- 変更があった場合のみ、最後に1回保存

環境変数:
  R2_ACCOUNT_ID: CloudflareアカウントID
  R2_ACCESS_KEY_ID: R2のアクセスキーID
  R2_SECRET_ACCESS_KEY: R2のシークレットアクセスキー
  R2_BUCKET_NAME: バケット名

使い方:
  python refine_hourly_ledger_cache3.py ledger_cache_2025.json 2025-01-01 2025-12-31
"""

import sys
import time
from datetime import datetime, timedelta, timezone

from xrpl.models.requests import Ledger

# R2対応版の append_rough_ledger_cache.py から共通処理をインポート
from append_rough_ledger_cache_r2 import (
    load_cache,
    save_cache,
    FutureLedgerError,
    ripple_time_to_datetime,
    client,
)

# XRPL のジェネシスレジャー（これより前には遡れない）
GENESIS_INDEX = 32570


def get_ledger_time(index: int) -> datetime:
    """指定 index の ledger close_time を datetime で返すヘルパ。"""
    res = client.request(Ledger(ledger_index=index, expand=True)).result
    return ripple_time_to_datetime(res["ledger"]["close_time"])


def find_ledger_between(
    target_dt: datetime,
    lo_index: int,
    hi_index: int,
) -> tuple[int, datetime]:
    """
    [lo_index, hi_index] 範囲内で二分探索し、

      - close_time >= target_dt

    を満たす「最小の ledger_index」を返す。
    """

    # 最新レジャー情報（未来チェックと上限補正用）
    latest = client.request(Ledger(ledger_index="validated", expand=True)).result
    latest_index = int(latest["ledger_index"])
    latest_time = ripple_time_to_datetime(latest["ledger"]["close_time"])

    if target_dt > latest_time:
        raise FutureLedgerError(
            f"target datetime {target_dt.isoformat()} is newer than latest ledger close_time {latest_time.isoformat()}"
        )

    # 範囲をクランプ
    lo_index = max(lo_index, GENESIS_INDEX)
    hi_index = min(hi_index, latest_index)

    # ---- 両端の通信（lo / hi） ----
    lo_time = get_ledger_time(lo_index)
    diff_lo = (lo_time - target_dt).total_seconds()
    print(f"    ↳ init: ledger={lo_index}, close_time={lo_time}, diff={diff_lo:.1f}s")

    hi_time = get_ledger_time(hi_index)
    diff_hi = (hi_time - target_dt).total_seconds()
    print(f"    ↳ init: ledger={hi_index}, close_time={hi_time}, diff={diff_hi:.1f}s")

    # target が範囲外ならエラー
    if target_dt < lo_time or target_dt > hi_time:
        raise RuntimeError(
            f"target {target_dt.isoformat()} is not bracketed by "
            f"lo({lo_index}, {lo_time.isoformat()}) and hi({hi_index}, {hi_time.isoformat()})"
        )

    iter_count = 0

    # ---- 二分探索 ----
    while lo_index + 1 < hi_index:
        mid = (lo_index + hi_index) // 2
        mid_time = get_ledger_time(mid)
        diff_mid = (mid_time - target_dt).total_seconds()

        iter_count += 1
        print(f"    ↳ iter {iter_count}: ledger={mid}, close_time={mid_time}, diff={diff_mid:.1f}s")

        if mid_time >= target_dt:
            hi_index, hi_time = mid, mid_time
        else:
            lo_index, lo_time = mid, mid_time

        time.sleep(0.05)

    # hi_index が「条件を満たす最小 index」
    if hi_time < target_dt:
        raise RuntimeError(
            f"binary search ended with hi_time < target_dt: "
            f"hi_index={hi_index}, hi_time={hi_time.isoformat()}, target={target_dt.isoformat()}"
        )

    # 最終ログ
    final_diff = (hi_time - target_dt).total_seconds()
    print(f"    ↳ result: ledger={hi_index}, close_time={hi_time}, diff={final_diff:.1f}s")

    return hi_index, hi_time


def refine_hourly_for_range(key: str, dt_start: datetime, dt_end: datetime) -> None:
    """
    R2上の key で指定された JSON キャッシュに対して、
    [dt_start, dt_end] の 1時間ごとのエントリを hourly に追加（なければ）する。

    - daily のラフデータをアンカーとし、「前日の daily」と「翌日の daily」の
      ledger_index を探索の基本下限・上限にする
    - さらに、直前の hourly の ledger_index を下限に取り込むことで検索範囲を狭める
    - 精緻条件は「target_dt と同時刻のレジャー」か「その直後の最初のレジャー」に限定
    - 変更があった場合のみ、最後に1回保存
    """
    cache = load_cache(key)
    daily: dict = cache.setdefault("daily", {})
    hourly: dict = cache.setdefault("hourly", {})

    cur = dt_start
    total_hours = int(((dt_end - dt_start).total_seconds() // 3600) + 1)
    processed = 0
    added = 0

    prev_date = None

    print(f"{dt_start} ～ {dt_end} の hourly を {key} に追記します。")

    # 1日ごとに「その日の daily ベースの lo_index / hi_index」
    day_lo_index = None
    day_hi_index = None

    # 直近で確定している hourly の ledger_index（前日も含めてグローバルに単調増加）
    last_hourly_index = None

    while cur <= dt_end:
        processed += 1
        current_date = cur.date()
        date_key = current_date.strftime("%Y-%m-%d")

        # 日付が変わったら daily を元に探索範囲を更新
        if prev_date is None or current_date != prev_date:
            today_entry = daily.get(date_key)
            if not today_entry:
                print(f"{date_key}: daily アンカーなし → この日の hourly を全スキップ")
                # 次の日の 0:00 に飛ばす
                cur = datetime.combine(
                    current_date + timedelta(days=1),
                    datetime.min.time(),
                    tzinfo=timezone.utc,
                )
                prev_date = current_date
                day_lo_index = None
                day_hi_index = None
                continue

            # 前日の daily
            prev_date_obj = current_date - timedelta(days=1)
            prev_key = prev_date_obj.strftime("%Y-%m-%d")
            prev_entry = daily.get(prev_key)

            # 翌日の daily
            next_date_obj = current_date + timedelta(days=1)
            next_key = next_date_obj.strftime("%Y-%m-%d")
            next_entry = daily.get(next_key)

            if prev_entry:
                day_lo_index = int(prev_entry["ledger_index"])
            else:
                day_lo_index = GENESIS_INDEX

            if next_entry:
                day_hi_index = int(next_entry["ledger_index"])
            else:
                # 翌日 daily がなければ、とりあえず「今日の daily より十分先」
                # 実際には find_ledger_between 側で validated を上限にクランプされる
                day_hi_index = int(today_entry["ledger_index"]) + 200_000

            print(
                f"{date_key}: base探索範囲 lo={day_lo_index}, hi={day_hi_index} "
                f"(prev_daily={prev_key if prev_entry else 'GENESIS'}, "
                f"next_daily={next_key if next_entry else 'validated付近'})"
            )

            prev_date = current_date

        # hourly のキー: 2025-12-01T00:00:00Z 形式
        key_iso = cur.replace(minute=0, second=0, microsecond=0).isoformat().replace("+00:00", "Z")

        # すでに hourly が存在する場合は、それを last_hourly として活用してスキップ
        if key_iso in hourly:
            existing = hourly[key_iso]
            try:
                existing_idx = int(existing.get("ledger_index"))
                if last_hourly_index is None or existing_idx > last_hourly_index:
                    last_hourly_index = existing_idx
            except Exception:
                pass

            print(f"[{processed}/{total_hours}] {key_iso}: hourly 既存 → スキップ")
            cur += timedelta(hours=1)
            continue

        if day_lo_index is None or day_hi_index is None:
            print(f"[{processed}/{total_hours}] {key_iso}: 探索範囲が未設定 → スキップ")
            cur += timedelta(hours=1)
            continue

        target_dt = cur

        # 直前の hourly を考慮して下限を狭める
        lo_index = day_lo_index
        if last_hourly_index is not None and last_hourly_index > lo_index:
            lo_index = last_hourly_index

        hi_index = day_hi_index

        print(f"[{processed}/{total_hours}] {key_iso}: {lo_index}～{hi_index} で探索")

        try:
            idx, close_time = find_ledger_between(target_dt, lo_index, hi_index)
            hourly[key_iso] = {
                "ledger_index": idx,
                "close_time": close_time.isoformat().replace("+00:00", "Z"),
            }
            added += 1
            last_hourly_index = idx  # 直前 hourly として更新
            print(f"   ↳ hourly 追加: ledger={idx}, close_time={close_time}")
            time.sleep(0.3)
        except FutureLedgerError as e:
            print(f"   ⏭ 未来の時間帯のためスキップ: {e}")
            break
        except Exception as e:
            # 条件を満たせなかった／その他エラー → hourly は書かず次へ
            print(f"   ⚠ hourly 取得失敗: {e}")
            time.sleep(0.5)

        cur += timedelta(hours=1)

    print(f"\n✅ hourly 精緻化完了: {processed} 時間中 {added} 本を追加")
    
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
        print("  python refine_hourly_ledger_cache3.py <r2_key> <start_date> <end_date>")
        print("例:")
        print("  python refine_hourly_ledger_cache3.py ledger_cache_2025.json 2025-01-01 2025-12-31")
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

    refine_hourly_for_range(r2_key, start_dt, end_dt)
