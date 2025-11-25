#!/usr/bin/env python3
import csv
import sys
import os
import math
from statistics import mean
from typing import List, Dict

import matplotlib.pyplot as plt


RESULTS_DIR = "results"

# Mapeia nome do arquivo -> rótulo bonito
LABELS: Dict[str, str] = {
    "metrics1.csv": "PUT 10k (20 goroutines)",
    "metrics2.csv": "GET 10k (40 goroutines)",
    "metrics3.csv": "MIX 20k (40 goroutines)",
    "out_put_50k.csv": "PUT 50k (80 goroutines)",
}

# Mapeia nome do arquivo -> concorrência (para estimar throughput)
CONCURRENCY: Dict[str, int] = {
    "metrics1.csv": 20,
    "metrics2.csv": 40,
    "metrics3.csv": 40,
    "out_put_50k.csv": 80,
}



CONCURRENCY.update({
    "get_S1_R2.csv": 20,
    "get_S1_R3.csv": 20,
    "get_S1_R4.csv": 20,

    "put_S1_R2.csv": 20,
    "put_S1_R3.csv": 20,
    "put_S1_R4.csv": 20,

    "get_S1_R1.csv": 150,
    "get_S2_R1.csv": 150,
    "get_S4_R1.csv": 150,
    "get_S8_R1.csv": 150,

    "put_S1_R1.csv": 100,
    "put_S2_R1.csv": 100,
    "put_S4_R1.csv": 100,
    "put_S8_R1.csv": 100,
})


def load_latencies_ms(csv_path: str, only_success: bool = True) -> List[float]:
    lat_ms: List[float] = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ok_str = row.get("ok", "true").lower()
            ok = ok_str == "true"
            if only_success and not ok:
                continue
            us = int(row["latency_us"])
            lat_ms.append(us / 1000.0)
    return lat_ms


def percentile(sorted_vals: List[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    if p <= 0:
        return sorted_vals[0]
    if p >= 1:
        return sorted_vals[-1]
    k = (len(sorted_vals) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)
    return d0 + d1


def compute_stats(lat_ms: List[float]) -> Dict[str, float]:
    if not lat_ms:
        return {
            "count": 0,
            "min": 0.0,
            "mean": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "max": 0.0,
        }

    lat_sorted = sorted(lat_ms)
    return {
        "count": len(lat_sorted),
        "min": lat_sorted[0],
        "mean": mean(lat_sorted),
        "p50": percentile(lat_sorted, 0.50),
        "p95": percentile(lat_sorted, 0.95),
        "p99": percentile(lat_sorted, 0.99),
        "max": lat_sorted[-1],
    }


def pretty_print_stats(name: str, stats: Dict[str, float]):
    print(f"\n=== Estatísticas para {name} ===")
    print(f"Samples : {int(stats['count'])}")
    print(f"min (ms): {stats['min']:.3f}")
    print(f"média   : {stats['mean']:.3f}")
    print(f"p50     : {stats['p50']:.3f}")
    print(f"p95     : {stats['p95']:.3f}")
    print(f"p99     : {stats['p99']:.3f}")
    print(f"max (ms): {stats['max']:.3f}")
    print("====================================")


def estimate_throughput_ops_s(stats: Dict[str, float], conc: int) -> float:
    """
    Estima throughput aproximado:
      throughput ≈ N / ( (sum(latências)/conc) / 1000 )
                 ≈ N * conc * 1000 / (mean_ms * N)
                 ≈ conc * 1000 / mean_ms
    """
    if stats["mean"] <= 0 or conc <= 0:
        return 0.0
    return conc * 1000.0 / stats["mean"]


def plot_histogram(lat_ms: List[float], title: str, out_path: str, bins: int = 60):
    if not lat_ms:
        print(f"[WARN] Sem dados para histograma em {title}")
        return

    plt.figure()
    plt.hist(lat_ms, bins=bins)
    plt.xlabel("Latência (ms)")
    plt.ylabel("Frequência")
    plt.title(f"Histograma de Latência - {title}")
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()
    print(f"[OK] Histograma salvo em {out_path}")


def plot_cdf(lat_ms: List[float], title: str, out_path: str):
    if not lat_ms:
        print(f"[WARN] Sem dados para CDF em {title}")
        return

    lat_sorted = sorted(lat_ms)
    n = len(lat_sorted)
    ys = [(i + 1) / n for i in range(n)]

    plt.figure()
    plt.plot(lat_sorted, ys)
    plt.xlabel("Latência (ms)")
    plt.ylabel("F(x)")
    plt.title(f"CDF de Latência - {title}")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()
    print(f"[OK] CDF salva em {out_path}")


def plot_summary_p95(labels: List[str], p95_vals: List[float], out_path: str):
    if not labels:
        return
    plt.figure()
    x = range(len(labels))
    plt.bar(x, p95_vals)
    plt.xticks(x, labels, rotation=20, ha="right")
    plt.ylabel("Latência p95 (ms)")
    plt.title("Comparação de Latência p95 entre Testes")
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()
    print(f"[OK] Gráfico de p95 salvo em {out_path}")


def plot_summary_throughput(labels: List[str], thr_vals: List[float], out_path: str):
    if not labels:
        return
    plt.figure()
    x = range(len(labels))
    plt.bar(x, thr_vals)
    plt.xticks(x, labels, rotation=20, ha="right")
    plt.ylabel("Throughput estimado (ops/s)")
    plt.title("Comparação de Throughput entre Testes")
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()
    print(f"[OK] Gráfico de throughput salvo em {out_path}")


def discover_csvs_from_results() -> List[str]:
    if not os.path.isdir(RESULTS_DIR):
        print(f"[ERRO] Diretório '{RESULTS_DIR}' não existe.")
        return []
    files = []
    for name in os.listdir(RESULTS_DIR):
        if name.lower().endswith(".csv"):
            files.append(os.path.join(RESULTS_DIR, name))
    files.sort()
    return files


def main():
    # Se o usuário passar arquivos na linha de comando, usa eles.
    # Caso contrário, carrega todos os CSVs dentro de ./results
    if len(sys.argv) > 1:
        csv_paths = sys.argv[1:]
    else:
        csv_paths = discover_csvs_from_results()

    if not csv_paths:
        print("Nenhum CSV encontrado. Use:")
        print("  python3 plots.py results/metrics1.csv results/metrics2.csv ...")
        print("ou coloque os CSVs em ./results para detecção automática.")
        sys.exit(1)

    summary_labels: List[str] = []
    summary_p95: List[float] = []
    summary_thr: List[float] = []

    os.makedirs(RESULTS_DIR, exist_ok=True)

    for path in csv_paths:
        base = os.path.basename(path)
        label = LABELS.get(base, base)

        print(f"\n[INFO] Processando {path} ({label}) ...")
        lat_ms = load_latencies_ms(path, only_success=True)
        stats = compute_stats(lat_ms)
        pretty_print_stats(label, stats)

        # Histograma e CDF
        base_no_ext = os.path.splitext(base)[0]
        hist_out = os.path.join(RESULTS_DIR, f"{base_no_ext}_hist.png")
        cdf_out = os.path.join(RESULTS_DIR, f"{base_no_ext}_cdf.png")

        plot_histogram(lat_ms, label, hist_out)
        plot_cdf(lat_ms, label, cdf_out)

        # Summary p95 + throughput
        summary_labels.append(label)
        summary_p95.append(stats["p95"])

        conc = CONCURRENCY.get(base, 0)
        thr = estimate_throughput_ops_s(stats, conc)
        summary_thr.append(thr)

    # Gráficos de comparação
    if len(summary_labels) >= 1:
        p95_out = os.path.join(RESULTS_DIR, "summary_p95.png")
        thr_out = os.path.join(RESULTS_DIR, "summary_throughput.png")
        plot_summary_p95(summary_labels, summary_p95, p95_out)
        plot_summary_throughput(summary_labels, summary_thr, thr_out)

    print("\n[FINISHED] Geração de gráficos concluída.")


if __name__ == "__main__":
    main()
