#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
infer_bus_mapping.py
=====================
チャネルごとに観測されたメッセージID集合と、バス名ごとに期待される
メッセージID集合(送信元ECUの物理接続バス情報から導出)を比較し、
「集合類似度マッチング問題」としてチャネル↔バス名の対応を推論する。

入力CSV(3種類、いずれもUTF-8想定)
----------------------------------------------------------------
1) ecu_bus.csv          ECUとその物理接続バスの対応
   カラム: ecu, bus_name
   例:
     ecu,bus_name
     ECU_ENGINE,Powertrain
     ECU_GATEWAY,Powertrain
     ECU_GATEWAY,Body
     ECU_BCM,Body

   ※ ゲートウェイのように複数バスに接続するECUは複数行に分けて記載する。

2) message_sender.csv   メッセージIDとその送信元ECUの対応
   カラム: message_id, sender_ecu
   例:
     message_id,sender_ecu
     0x100,ECU_ENGINE
     0x200,ECU_BCM

   ※ 中継(ゲートウェイ)されたメッセージも「本来の送信元ECU」を書く。
      中継ECU自身を送信元にしない(=ネイティブ集合が汚れるのを防ぐ)。

3) channel_observed.csv 各チャネルで実際に観測されたメッセージID
   カラム: channel, message_id[, count]
   例:
     channel,message_id,count
     CH1,0x100,15234
     CH1,0x101,15230
     CH1,0x200,320      <- 中継されて紛れ込んだ他バスのID(あってよい)

   ※ countは任意。将来的な頻度ベースの重み付けに使える(現状は集合演算のみ使用)。

出力
----------------------------------------------------------------
- 標準出力にスコア行列とハンガリアン法による最終割当を表示
- --out で指定したCSVに最終割当を保存
  (channel, matched_bus, score, recall, precision, note)

スコアの考え方
----------------------------------------------------------------
S_B = バスBに接続されたECUが送信するメッセージIDの集合(設計上の期待値)
O_C = チャネルCで実際に観測されたメッセージIDの集合(実測)

  recall    = |O_C ∩ S_B| / |S_B|   … 本来あるべきIDをどれだけ観測できたか
  precision = |O_C ∩ S_B| / |O_C|   … 観測したIDのうちどれだけ本来のものか

中継によってprecisionは下がりやすいため、recallをより重視した
F-beta score (beta=2) を最終スコアとして採用している。
  F_beta = (1+beta^2) * precision * recall / (beta^2 * precision + recall)

割当はこのスコアを重みとしたハンガリアン法(最大重み一対一マッチング)で決定する。
チャネル数とバス数が異なる場合はダミー行/列(スコア0)を挿入して対応する。
"""

import argparse
import csv
import sys
from collections import defaultdict

import numpy as np
from scipy.optimize import linear_sum_assignment

# ----------------------------------------------------------------------
# データ読み込み
# ----------------------------------------------------------------------


def load_ecu_bus(path):
    """ecu_bus.csv を読み、 bus_name -> set(ecu) の辞書を返す"""
    bus_to_ecus = defaultdict(set)
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        _require_columns(reader.fieldnames, ["ecu", "bus_name"], path)
        for row in reader:
            ecu = row["ecu"].strip()
            bus = row["bus_name"].strip()
            if ecu and bus:
                bus_to_ecus[bus].add(ecu)
    return bus_to_ecus


def load_message_sender(path):
    """message_sender.csv を読み、 sender_ecu -> set(message_id) の辞書を返す"""
    ecu_to_msgs = defaultdict(set)
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        _require_columns(reader.fieldnames, ["message_id", "sender_ecu"], path)
        for row in reader:
            mid = _normalize_id(row["message_id"])
            ecu = row["sender_ecu"].strip()
            if mid and ecu:
                ecu_to_msgs[ecu].add(mid)
    return ecu_to_msgs


def load_channel_observed(path):
    """channel_observed.csv を読み、 channel -> set(message_id) の辞書を返す"""
    ch_to_msgs = defaultdict(set)
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        _require_columns(reader.fieldnames, ["channel", "message_id"], path)
        for row in reader:
            ch = row["channel"].strip()
            mid = _normalize_id(row["message_id"])
            if ch and mid:
                ch_to_msgs[ch].add(mid)
    return ch_to_msgs


def _require_columns(fieldnames, required, path):
    fieldnames = fieldnames or []
    missing = [c for c in required if c not in fieldnames]
    if missing:
        sys.exit(f"エラー: {path} に必要な列がありません: {missing} (実際の列: {fieldnames})")


def _normalize_id(raw):
    """メッセージIDの表記ゆれ(0x100 / 100 / 256 など)を正規化する。
    16進数は小文字 '0x' 付きの文字列に統一する。10進で書かれていても
    そのまま数値として扱いたい場合はここを調整すること。"""
    s = raw.strip()
    if not s:
        return None
    try:
        if s.lower().startswith("0x"):
            val = int(s, 16)
        else:
            # 10進として読めるならそちらを優先、ダメなら16進とみなす
            try:
                val = int(s, 10)
            except ValueError:
                val = int(s, 16)
    except ValueError:
        return s  # 数値化できない特殊IDはそのまま文字列キーとして扱う
    return f"0x{val:X}"


# ----------------------------------------------------------------------
# 集合構築
# ----------------------------------------------------------------------


def build_bus_native_sets(bus_to_ecus, ecu_to_msgs):
    """バスごとの『ネイティブ集合』S_B を構築する"""
    bus_to_msgs = {}
    for bus, ecus in bus_to_ecus.items():
        msgs = set()
        for ecu in ecus:
            msgs |= ecu_to_msgs.get(ecu, set())
        bus_to_msgs[bus] = msgs
    return bus_to_msgs


# ----------------------------------------------------------------------
# スコアリング
# ----------------------------------------------------------------------


def score_pair(observed, expected, beta=2.0):
    """観測集合(observed)と期待集合(expected)の類似度を返す。
    戻り値: (f_beta_score, recall, precision, jaccard)"""
    if not expected or not observed:
        return 0.0, 0.0, 0.0, 0.0
    inter = observed & expected
    union = observed | expected
    recall = len(inter) / len(expected)
    precision = len(inter) / len(observed)
    jaccard = len(inter) / len(union) if union else 0.0
    if precision + recall == 0:
        f_beta = 0.0
    else:
        f_beta = (1 + beta**2) * precision * recall / (beta**2 * precision + recall)
    return f_beta, recall, precision, jaccard


def build_score_matrix(channels, buses, ch_to_msgs, bus_to_msgs, beta=2.0):
    """channels x buses のスコア行列と詳細情報を返す"""
    n, m = len(channels), len(buses)
    score_mat = np.zeros((n, m))
    detail = {}
    for i, ch in enumerate(channels):
        for j, bus in enumerate(buses):
            f_beta, recall, precision, jaccard = score_pair(
                ch_to_msgs.get(ch, set()), bus_to_msgs.get(bus, set()), beta=beta
            )
            score_mat[i, j] = f_beta
            detail[(ch, bus)] = dict(score=f_beta, recall=recall, precision=precision, jaccard=jaccard)
    return score_mat, detail


# ----------------------------------------------------------------------
# 最適割当(ハンガリアン法)
# ----------------------------------------------------------------------


def solve_assignment(score_mat):
    """スコア最大化の一対一割当を解く。
    行数・列数が異なる場合はscipyが自動的に短い方に合わせて割り当てる
    (全部は割り当てられない側が出る)。"""
    cost_mat = -score_mat  # 最大化 → 最小化に変換
    row_ind, col_ind = linear_sum_assignment(cost_mat)
    return row_ind, col_ind


# ----------------------------------------------------------------------
# メイン処理
# ----------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="CAN/Ethernetチャネルとバス名を自動でひも付ける")
    parser.add_argument("--ecu-bus", required=True, help="ecu_bus.csv のパス")
    parser.add_argument("--message-sender", required=True, help="message_sender.csv のパス")
    parser.add_argument("--channel-observed", required=True, help="channel_observed.csv のパス")
    parser.add_argument("--out", default="channel_bus_mapping.csv", help="出力CSVのパス")
    parser.add_argument(
        "--beta", type=float, default=2.0, help="F-betaスコアのbeta値(大きいほどrecallを重視。デフォルト2.0)"
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=0.3,
        help="この値未満のスコアで割り当てられた組は '要確認' として警告する(デフォルト0.3)",
    )
    args = parser.parse_args()

    bus_to_ecus = load_ecu_bus(args.ecu_bus)
    ecu_to_msgs = load_message_sender(args.message_sender)
    ch_to_msgs = load_channel_observed(args.channel_observed)

    bus_to_msgs = build_bus_native_sets(bus_to_ecus, ecu_to_msgs)

    channels = sorted(ch_to_msgs.keys())
    buses = sorted(bus_to_msgs.keys())

    if not channels:
        sys.exit("エラー: channel_observed.csv から観測チャネルが取得できませんでした。")
    if not buses:
        sys.exit("エラー: ecu_bus.csv から期待バス集合を構築できませんでした。")

    score_mat, detail = build_score_matrix(channels, buses, ch_to_msgs, bus_to_msgs, beta=args.beta)

    # --- スコア行列の表示 ---
    print("\n=== スコア行列 (F%.1f score) ===" % args.beta)
    header = "        " + "".join(f"{b:>14s}" for b in buses)
    print(header)
    for i, ch in enumerate(channels):
        row = "".join(f"{score_mat[i, j]:14.3f}" for j in range(len(buses)))
        print(f"{ch:>8s}{row}")

    # --- 最適割当 ---
    row_ind, col_ind = solve_assignment(score_mat)

    results = []
    assigned_channels = set()
    for r, c in zip(row_ind, col_ind):
        ch = channels[r]
        bus = buses[c]
        d = detail[(ch, bus)]
        note = ""
        if d["score"] < args.min_score:
            note = "要確認(スコアが低い)"
        results.append(dict(channel=ch, matched_bus=bus, **d, note=note))
        assigned_channels.add(ch)

    # ハンガリアン法で割り当てられなかったチャネル(チャネル数>バス数の場合)
    for ch in channels:
        if ch not in assigned_channels:
            results.append(
                dict(
                    channel=ch,
                    matched_bus="(割当なし)",
                    score=0,
                    recall=0,
                    precision=0,
                    jaccard=0,
                    note="対応するバス候補が不足",
                )
            )

    results.sort(key=lambda r: r["channel"])

    print("\n=== 推論結果(ハンガリアン法による最適割当) ===")
    print(f"{'channel':<10}{'matched_bus':<18}{'score':>8}{'recall':>8}{'precision':>10}{'note'}")
    for r in results:
        print(
            f"{r['channel']:<10}{r['matched_bus']:<18}"
            f"{r['score']:>8.3f}{r['recall']:>8.3f}{r['precision']:>10.3f}  {r['note']}"
        )

    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f, fieldnames=["channel", "matched_bus", "score", "recall", "precision", "jaccard", "note"]
        )
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    print(f"\n結果を {args.out} に保存しました。")


if __name__ == "__main__":
    main()
