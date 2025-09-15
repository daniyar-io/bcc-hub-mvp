# src/generate_push.py
import os
import re
import math
import json
from pathlib import Path
import pandas as pd

# Пути
CLIENTS_FULL = "data/processed/clients_full.csv"
SCORES_ALL = "data/processed/scores.csv"        # опционально: все продукты (scoring.py)
SCORES_TOP1 = "data/processed/scores_top1.csv"  # опционально: топ-1 (scoring.py)
OUT = "data/processed/push_results.csv"
REPORTS_DIR = Path("reports/pushes")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Шаблоны (детерминированно выбираем первый шаблон)
TEMPLATES = {
    "travel_card": "{name}, за 3 месяца вы потратили {amount} на поездки. С тревел-картой вы могли бы получить ≈{benefit}. Посмотреть.",
    "taxi_card": "{name}, расходы на такси {amount}. Карта для поездок даст вам ≈{benefit}. Посмотреть.",
    "restaurants_card": "{name}, траты в кафе {amount}. Карта с кешбэком вернёт ≈{benefit}. Оформить.",
    "supermarket_card": "{name}, расходы на продукты {amount}. Карта с кешбэком даст ≈{benefit}. Посмотреть.",
    "premium_card": "{name}, средний баланс {balance}. Премиальный пакет даст сервисы и выгоду ≈{benefit}. Узнать.",
    "deposit": "{name}, разместив {balance} на депозите (примерно), вы получите ≈{benefit} в месяц. Открыть депозит.",
    "credit_offer": "{name}, по вашему профилю доступен кредит с преимуществами — ориентировочная польза ≈{benefit}. Посмотреть.",
    "fx_offer": "{name}, у вас есть переводы — можно снизить комиссии и сэкономить ≈{benefit}. Узнать.",
    "investment_offer": "{name}, с текущим балансом доступны инвестиции — ориентировая доходность ≈{benefit}. Посмотреть.",
    "gold_offer": "{name}, есть покупки ювелирных — рассмотрите накопления в золоте, ориентировая польза ≈{benefit}. Узнать.",
    # fallback
    "default": "{name}, у вас персональное предложение: {product}. Посмотреть."
}

FALLBACK_PRODUCTS = ["deposit", "credit_offer", "premium_card", "supermarket_card", "travel_card", "taxi_card", "restaurants_card", "investment_offer", "fx_offer", "gold_offer"]

CTAS = ["оформ", "посмотр", "узна", "открыт", "открыть"]

# --- вспомогательные функции ---
def safe_float(x):
    try:
        v = float(x)
        if not math.isfinite(v):
            return 0.0
        return v
    except:
        return 0.0

def format_money_kzt(x):
    v = safe_float(x)
    s = f"{int(round(v)):,}".replace(",", " ")
    return f"{s} ₸"

def enforce_text(text):
    # 1) не CAPS (title-case для слов >1 буквы в CAPS)
    text = re.sub(r"\b([A-ZА-ЯЁ]{2,})\b", lambda m: m.group(1).title(), text)
    # 2) убрать лишние эмодзи (простая версия)
    text = re.sub(r"[\U0001F300-\U0001F6FF\U0001F600-\U0001F64F\U0001F680-\U0001F6FF]", "", text)
    # 3) максимум один '!'
    if text.count("!") > 1:
        text = text.replace("!", ".")
    # 4) длина <=200
    if len(text) > 200:
        text = text[:197].rsplit(" ", 1)[0] + "…"
    # 5) ensure CTA
    if not any(c in text.lower() for c in CTAS):
        text = text.rstrip(".") + ". Посмотреть."
    return text.strip()

def get_top4_by_scores(client_code, scores_all_df):
    # deterministic: сортировка по benefit_est_KZT desc, tie-breaker product name
    df = scores_all_df[scores_all_df["client_code"].astype(str) == str(client_code)]
    if df.empty:
        return []
    df_sorted = df.sort_values(["benefit_est_KZT", "product"], ascending=[False, True])
    return list(df_sorted["product"].astype(str).tolist())[:4]

# --- основное ---
def generate_pushes():
    if not os.path.exists(CLIENTS_FULL):
        print("❌ Нет clients_full.csv. Сначала запустите merge_data.py")
        return

    clients = pd.read_csv(CLIENTS_FULL, dtype={"client_code": object})
    # нормализуем числовые колонки, чтобы не было NaN
    num_cols = [c for c in clients.columns if c.startswith("spent_")] + ["total_spent","avg_transaction","transfers_in","transfers_out","avg_monthly_balance_KZT"]
    for c in num_cols:
        if c in clients.columns:
            clients[c] = pd.to_numeric(clients[c], errors="coerce").fillna(0)

    scores_all = None
    if os.path.exists(SCORES_ALL):
        scores_all = pd.read_csv(SCORES_ALL, dtype={"client_code": object})
        scores_all["benefit_est_KZT"] = pd.to_numeric(scores_all["benefit_est_KZT"], errors="coerce").fillna(0)

    out_rows = []
    for _, r in clients.iterrows():
        client_code = str(r.get("client_code", r.name))
        name = r.get("name", "Клиент")
        balance = safe_float(r.get("avg_monthly_balance_KZT", 0))
        total_spent = safe_float(r.get("total_spent", 0))

        # получаем топ-4 продуктов детерминированно:
        recs = []
        if scores_all is not None:
            recs = get_top4_by_scores(client_code, scores_all)
        # если нет или меньше 4, дополняем fallback в порядке списка (детерминир.)
        for p in FALLBACK_PRODUCTS:
            if p not in recs:
                recs.append(p)
            if len(recs) >= 4:
                break
        rec_1, rec_2, rec_3, rec_4 = recs[:4]

        # benefit: берем из scores_all (первый найден) или 0
        benefit_val = 0.0
        if scores_all is not None:
            srow = scores_all[(scores_all["client_code"].astype(str) == client_code) & (scores_all["product"].astype(str) == rec_1)]
            if not srow.empty:
                benefit_val = safe_float(srow["benefit_est_KZT"].iloc[0])

        # текст пуша: берем шаблон по rec_1, подставляем переменные
        template = TEMPLATES.get(rec_1, TEMPLATES["default"])
        push_raw = template.format(
            name=name,
            amount=format_money_kzt(total_spent),
            balance=format_money_kzt(balance),
            benefit=format_money_kzt(benefit_val),
            product=rec_1
        )
        push = enforce_text(push_raw)

        # сохранить per-client отчет (md)
        with open(REPORTS_DIR / f"client_{client_code}_push.md", "w", encoding="utf-8") as f:
            f.write(f"# Push — client {client_code}\n\n")
            f.write(f"Name: {name}\n\n")
            f.write(f"rec_1: {rec_1}\nrec_2: {rec_2}\nrec_3: {rec_3}\nrec_4: {rec_4}\n\n")
            f.write(f"benefit_est_KZT: {benefit_val}\n\n")
            f.write("Push text:\n\n")
            f.write(push + "\n")

        out_rows.append({
            "client_code": client_code,
            "name": name,
            "product": rec_1,
            "push": push,
            "rec_1": rec_1,
            "rec_2": rec_2,
            "rec_3": rec_3,
            "rec_4": rec_4
        })

    df_out = pd.DataFrame(out_rows)
    df_out.to_csv(OUT, index=False, encoding="utf-8-sig")
    print(f"✅ push_results сохранён: {OUT}")
    print(f"✅ per-client отчёты: {REPORTS_DIR}")

if __name__ == "__main__":
    generate_pushes()