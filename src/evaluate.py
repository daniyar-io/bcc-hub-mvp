# src/evaluate.py
import os
import re
import pandas as pd
from pathlib import Path

INPUT = "data/processed/push_results.csv"
OUT_METRICS = "data/processed/scores_metrics.csv"
REPORT = Path("reports/evaluation.md")
REPORT.parent.mkdir(parents=True, exist_ok=True)

CTAS = ["оформ", "посмотр", "узна", "открыт", "открыть"]

def has_cta(text):
    if not isinstance(text, str): return False
    return any(c in text.lower() for c in CTAS)

def caps_ok(text):
    # разрешаем 1 полностью заглавное слово максимум
    caps = sum(1 for w in text.split() if w.isalpha() and w.isupper())
    return caps <= 1

def emojis_count(text):
    if not isinstance(text, str): return 0
    return len(re.findall(r"[\U0001F300-\U0001F6FF\U0001F600-\U0001F64F]", text))

def run_evaluation():
    if not os.path.exists(INPUT):
        print("❌ Нет push_results.csv — сначала запустите generate_push.py")
        return
    df = pd.read_csv(INPUT, dtype={"client_code": object})
    # basic checks
    df["push"] = df["push"].fillna("")
    df["len_ok"] = df["push"].apply(lambda t: len(str(t)) <= 200)
    df["cta_ok"] = df["push"].apply(has_cta)
    df["caps_ok"] = df["push"].apply(caps_ok)
    df["emoji_count"] = df["push"].apply(emojis_count)
    df["emoji_ok"] = df["emoji_count"] <= 1
    df["not_empty"] = df["push"].apply(lambda t: len(str(t).strip()) > 0)

    # uniqueness: are all pushes identical?
    unique_pushes = df["push"].nunique()
    total = len(df)

    # product distribution
    dist = df["product"].value_counts().to_dict()

    # top1 / top4 metrics if target_product exists
    if "target_product" in df.columns:
        df["hit_top1"] = (df["rec_1"].astype(str) == df["target_product"].astype(str)).astype(int)
        df["hit_top4"] = df[["rec_1","rec_2","rec_3","rec_4"]].apply(
            lambda row: int(str(row["rec_1"])==str(row.name) or str(row["rec_2"])==str(row.name) or False),
            axis=1
        )
        # safer calc: use row-wise function
        def hit_top4_row(row):
            tsh = str(row["target_product"])
            for c in ["rec_1","rec_2","rec_3","rec_4"]:
                if str(row.get(c,"")) == tsh:
                    return 1
            return 0
        df["hit_top4"] = df.apply(hit_top4_row, axis=1)
        # save per-client metrics
        df[["client_code","hit_top1","hit_top4"]].to_csv(OUT_METRICS, index=False, encoding="utf-8-sig")
        top1_rate = df["hit_top1"].mean()
        top4_rate = df["hit_top4"].mean()
    else:
        top1_rate = None
        top4_rate = None

    # summary stats
    summary = {
        "total_clients": total,
        "unique_push_texts": unique_pushes,
        "pushes_non_empty_pct": df["not_empty"].mean(),
        "len_ok_pct": df["len_ok"].mean(),
        "cta_ok_pct": df["cta_ok"].mean(),
        "caps_ok_pct": df["caps_ok"].mean(),
        "emoji_ok_pct": df["emoji_ok"].mean(),
        "top1_rate": top1_rate,
        "top4_rate": top4_rate,
        "product_distribution": dist
    }

    # write evaluation.md
    with open(REPORT, "w", encoding="utf-8") as f:
        f.write("# Evaluation Report\n\n")
        f.write("## Summary\n\n")
        for k,v in summary.items():
            f.write(f"- *{k}*: {v}\n")
        f.write("\n## Failing examples (up to 20)\n\n")
        fails = df[(~df["len_ok"]) | (~df["cta_ok"]) | (~df["caps_ok"]) | (~df["emoji_ok"]) | (~df["not_empty"])]
        if fails.empty:
            f.write("All pushes passed basic checks.\n")
        else:
            for _, row in fails.head(20).iterrows():
                f.write(f"- client_code: {row['client_code']}, push: \"{row['push']}\", len:{len(row['push'])}, cta_ok:{row['cta_ok']}, caps_ok:{row['caps_ok']}, emoji_count:{row['emoji_count']}\n")

    print("✅ Evaluation finished.")
    print("Summary:")
    for k,v in summary.items():
        print(f"- {k}: {v}")

if __name__ == "__main__":
    run_evaluation()