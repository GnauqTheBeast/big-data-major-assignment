#!/usr/bin/env python3
"""
Gen 100M dòng từ 100k từ distinct.
- Nếu có vocab file (1 từ/dòng) thì dùng nó, không thì tạo 100k từ synthetic kw000000..kw099999
- Phân bố Zipf (s=1.08) để Top-K có ý nghĩa (một số từ rất hot), có flag --uniform để chia đều
- Stream theo batch để không tốn RAM, ~800MB cho 100M dòng

Usage:
  python3 top-k/generate-topk-input.py                          # 100k vocab synthetic -> 100M dòng -> top-k/input/keywords-100M.txt
  python3 top-k/generate-topk-input.py --vocab /path/vocab.txt  # dùng vocab sẵn có
  python3 top-k/generate-topk-input.py --lines 10000000 --uniform
"""
import argparse, random, sys, os

parser = argparse.ArgumentParser()
parser.add_argument("--vocab", default=None, help="đường dẫn file vocab (1 từ/dòng), nếu thiếu sẽ tự tạo 100k từ")
parser.add_argument("--vocab-size", type=int, default=100_000)
parser.add_argument("--lines", type=int, default=100_000_000, help="số dòng cần gen")
parser.add_argument("--output", default="top-k/input/keywords-100M.txt")
parser.add_argument("--uniform", action="store_true", help="chia đều thay vì Zipf")
parser.add_argument("--zipf-s", type=float, default=1.08, help="exponent Zipf, càng lớn càng lệch")
parser.add_argument("--seed", type=int, default=42)
parser.add_argument("--batch", type=int, default=1_000_000, help="ghi theo batch")
args = parser.parse_args()

random.seed(args.seed)

# 1. Load / tạo vocab
if args.vocab and os.path.exists(args.vocab):
    with open(args.vocab, encoding="utf-8") as f:
        vocab = [line.strip() for line in f if line.strip()]
    print(f"Loaded vocab: {len(vocab)} từ từ {args.vocab}")
    if len(vocab) != args.vocab_size:
        print(f"Cảnh báo: vocab có {len(vocab)} từ, khác --vocab-size={args.vocab_size} (vẫn dùng toàn bộ)")
else:
    if args.vocab:
        print(f"Không thấy {args.vocab}, tự tạo {args.vocab_size} từ synthetic.")
    vocab = [f"kw{i:06d}" for i in range(args.vocab_size)]
    # lưu lại để tái sử dụng
    vocab_path = "top-k/input/vocab-100k.txt"
    os.makedirs(os.path.dirname(vocab_path), exist_ok=True)
    with open(vocab_path, "w", encoding="utf-8") as f:
        f.write("\n".join(vocab) + "\n")
    print(f"Đã tạo vocab synthetic {len(vocab)} từ -> {vocab_path}")

n = len(vocab)

# 2. Chuẩn bị weights
try:
    import numpy as np
    has_np = True
except ImportError:
    has_np = False

if args.uniform:
    weights = None
    print("Phân bố: uniform")
else:
    # Zipf: w_i = 1 / (i+1)^s
    if has_np:
        ranks = np.arange(1, n + 1, dtype=np.float64)
        weights = 1.0 / np.power(ranks, args.zipf_s)
        weights /= weights.sum()
        print(f"Phân bố: Zipf s={args.zipf_s} (numpy)")
    else:
        weights = [1.0 / ((i + 1) ** args.zipf_s) for i in range(n)]
        s = sum(weights)
        weights = [w / s for w in weights]
        print(f"Phân bố: Zipf s={args.zipf_s} (pure python)")

os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)

# 3. Gen stream
print(f"Gen {args.lines:,} dòng -> {args.output}  (batch={args.batch:,}) ...")
written = 0
with open(args.output, "w", encoding="utf-8", buffering=1024 * 1024) as out:
    if has_np and weights is not None:
        # numpy batch
        for start in range(0, args.lines, args.batch):
            cur = min(args.batch, args.lines - start)
            idx = np.random.choice(n, size=cur, p=weights)
            # map idx -> word, join nhanh
            out.write("\n".join(vocab[i] for i in idx) + "\n")
            written += cur
            if written % (10_000_000) == 0:
                print(f"  ... {written:,}/{args.lines:,}")
    elif has_np and weights is None:
        for start in range(0, args.lines, args.batch):
            cur = min(args.batch, args.lines - start)
            idx = np.random.randint(0, n, size=cur)
            out.write("\n".join(vocab[i] for i in idx) + "\n")
            written += cur
            if written % 10_000_000 == 0:
                print(f"  ... {written:,}/{args.lines:,}")
    else:
        # pure python — random.choices đã tối ưu C, vẫn nhanh
        import bisect
        # random.choices với weights
        for start in range(0, args.lines, args.batch):
            cur = min(args.batch, args.lines - start)
            picks = random.choices(vocab, weights=weights, k=cur) if weights else random.choices(vocab, k=cur)
            out.write("\n".join(picks) + "\n")
            written += cur
            if written % 10_000_000 == 0:
                print(f"  ... {written:,}/{args.lines:,}")

size = os.path.getsize(args.output)
print(f"Xong: {written:,} dòng, {size/1024/1024:.1f} MiB")
print(f"Preview top:")
with open(args.output) as f:
    for _ in range(5):
        print(" ", f.readline().rstrip())
