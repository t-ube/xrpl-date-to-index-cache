#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
XRPL Ledger Cache 更新スクリプト（Clio版）

GitHub Actions から呼び出され、当年と前年の2年分のデータを更新します。

Clio API を使用するため、従来の二分探索版と比べて：
- 高速（1時間あたり1回のAPI呼び出し）
- シンプル（dailyデータ不要）
- 正確（Clio保証の値）

環境変数:
  R2_ACCOUNT_ID: CloudflareアカウントID
  R2_ACCESS_KEY_ID: R2のアクセスキーID
  R2_SECRET_ACCESS_KEY: R2のシークレットアクセスキー
  R2_BUCKET_NAME: バケット名
"""

import subprocess
import sys
from datetime import datetime, timezone


def get_current_year() -> int:
    """現在のUTC年を取得"""
    return datetime.now(timezone.utc).year


def run_command(args: list[str]) -> bool:
    """コマンドを実行し、成功したかどうかを返す"""
    print(f"\n{'='*60}")
    print(f"実行: {' '.join(args)}")
    print('='*60)
    
    try:
        result = subprocess.run(args, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"⚠ コマンド失敗 (exit code {e.returncode}): {' '.join(args)}")
        return False
    except Exception as e:
        print(f"⚠ エラー: {e}")
        return False


def main():
    current_year = get_current_year()
    prev_year = current_year - 1

    print(f"XRPL Ledger Cache 更新開始（Clio版）")
    print(f"現在年: {current_year}")
    print(f"処理対象: {prev_year}年, {current_year}年")

    success_count = 0
    total_count = 0

    # ========================================
    # 前年の hourly 生成
    # ========================================
    total_count += 1
    if run_command([
        "python", "generate_hourly_clio.py",
        f"ledger_cache_{prev_year}.json",
        f"{prev_year}-01-01",
        f"{prev_year}-12-31"
    ]):
        success_count += 1

    # ========================================
    # 当年の hourly 生成
    # ========================================
    total_count += 1
    if run_command([
        "python", "generate_hourly_clio.py",
        f"ledger_cache_{current_year}.json",
        f"{current_year}-01-01",
        f"{current_year}-12-31"
    ]):
        success_count += 1

    # ========================================
    # 結果サマリー
    # ========================================
    print(f"\n\n{'='*60}")
    print("処理完了")
    print('='*60)
    print(f"成功: {success_count}/{total_count}")

    if success_count == total_count:
        print("✅ すべての処理が正常に完了しました")
        sys.exit(0)
    else:
        print("⚠ 一部の処理が失敗しました")
        sys.exit(1)


if __name__ == "__main__":
    main()
