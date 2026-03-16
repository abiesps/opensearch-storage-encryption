#!/usr/bin/env python3
"""Generate bar charts for JMH benchmark results."""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import os

# Raw benchmark data: (directoryType, disableL1Cache, score, error)
benchmarks = {
    "randomReadBulkBytesFromClone": [
        ("bufferpool", "false", 5329.043, 258.512),
        ("bufferpool", "true", 987.357, 347.366),
        ("bufferpoolv0", "false", 4.730, 0.536),
        ("bufferpoolv0", "true", 4.211, 0.367),
        ("mmap", "false", 5631.180, 104.786),
        ("mmap", "true", 5563.446, 522.365),
        ("mmap_single", "false", 6194.311, 243.433),
        ("mmap_single", "true", 5909.227, 2153.868),
        ("directbufferpool", "false", 4962.427, 611.357),
        ("directbufferpool", "true", 4006.979, 254.414),
    ],
    "randomReadByteFromClone": [
        ("bufferpool", "false", 28083.582, 2011.499),
        ("bufferpool", "true", 1011.628, 185.908),
        ("bufferpoolv0", "false", 3742.916, 544.001),
        ("bufferpoolv0", "true", 12178.994, 150.201),
        ("mmap", "false", 36112.875, 1171.202),
        ("mmap", "true", 34879.980, 3915.627),
        ("mmap_single", "false", 37748.477, 3836.588),
        ("mmap_single", "true", 37466.077, 4374.063),
        ("directbufferpool", "false", 28995.739, 1624.387),
        ("directbufferpool", "true", 13477.281, 760.787),
    ],
    "sequentialReadBulkBytesFromClone": [
        ("bufferpool", "false", 5780.284, 131.774),
        ("bufferpool", "true", 936.085, 192.497),
        ("bufferpoolv0", "false", 3001.758, 10.488),
        ("bufferpoolv0", "true", 4115.150, 758.067),
        ("mmap", "false", 6246.319, 164.598),
        ("mmap", "true", 6180.808, 185.906),
        ("mmap_single", "false", 6337.289, 166.227),
        ("mmap_single", "true", 6360.763, 102.767),
        ("directbufferpool", "false", 5916.996, 148.921),
        ("directbufferpool", "true", 4517.969, 162.753),
    ],
    "sequentialReadBytesFromClone": [
        ("bufferpool", "false", 131.873, 8.194),
        ("bufferpool", "true", 209.436, 12.178),
        ("bufferpoolv0", "false", 8.013, 1.349),
        ("bufferpoolv0", "true", 7.981, 1.331),
        ("mmap", "false", 1194.473, 80.895),
        ("mmap", "true", 1179.796, 35.964),
        ("mmap_single", "false", 1158.550, 49.271),
        ("mmap_single", "true", 1156.757, 46.992),
        ("directbufferpool", "false", 173.449, 178.978),
        ("directbufferpool", "true", 189.977, 80.478),
    ],
}

# Colors: L1 enabled (solid), L1 disabled (hatched)
COLORS = {
    "bufferpool":       "#2196F3",  # blue
    "bufferpoolv0":     "#FF9800",  # orange
    "mmap":             "#4CAF50",  # green
    "mmap_single":      "#9C27B0",  # purple
    "directbufferpool": "#F44336",  # red
}

DIR_ORDER = ["bufferpool", "bufferpoolv0", "mmap", "mmap_single", "directbufferpool"]
LABELS = {
    "bufferpool": "BufferPool V1",
    "bufferpoolv0": "BufferPool V0",
    "mmap": "MMap (chunked)",
    "mmap_single": "MMap (single)",
    "directbufferpool": "DirectBufferPool V2",
}

os.makedirs("benchmark_charts", exist_ok=True)

for bench_name, data in benchmarks.items():
    fig, ax = plt.subplots(figsize=(14, 7))

    # Group by directory type
    x_labels = []
    scores_l1_on = []
    errors_l1_on = []
    scores_l1_off = []
    errors_l1_off = []
    colors = []

    for d in DIR_ORDER:
        x_labels.append(LABELS[d])
        colors.append(COLORS[d])
        for dtype, l1, score, err in data:
            if dtype == d and l1 == "false":
                scores_l1_on.append(score)
                errors_l1_on.append(err)
            elif dtype == d and l1 == "true":
                scores_l1_off.append(score)
                errors_l1_off.append(err)

    x = np.arange(len(x_labels))
    width = 0.35

    bars1 = ax.bar(x - width/2, scores_l1_on, width, yerr=errors_l1_on,
                   label='L1 Cache ON', color=colors, alpha=0.9,
                   edgecolor='black', linewidth=0.5, capsize=4)
    bars2 = ax.bar(x + width/2, scores_l1_off, width, yerr=errors_l1_off,
                   label='L1 Cache OFF', color=colors, alpha=0.45,
                   edgecolor='black', linewidth=0.5, capsize=4,
                   hatch='///')

    ax.set_xlabel('Directory Type', fontsize=13, fontweight='bold')
    ax.set_ylabel('Throughput (ops/s)', fontsize=13, fontweight='bold')
    ax.set_title(f'HotPathReadBenchmarks.{bench_name}\n'
                 f'8KB blocks · 1MB file · 32 threads · 5 iterations',
                 fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, fontsize=11)
    ax.legend(fontsize=12, loc='upper right')
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)

    # Add value labels on bars
    def add_labels(bars, scores):
        for bar, score in zip(bars, scores):
            height = bar.get_height()
            if score >= 1000:
                label = f'{score:,.0f}'
            else:
                label = f'{score:.1f}'
            ax.annotate(label,
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 5), textcoords="offset points",
                        ha='center', va='bottom', fontsize=8, fontweight='bold')

    add_labels(bars1, scores_l1_on)
    add_labels(bars2, scores_l1_off)

    plt.tight_layout()
    filename = f"benchmark_charts/{bench_name}.png"
    fig.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved: {filename}")

print("\nAll charts generated in benchmark_charts/")
