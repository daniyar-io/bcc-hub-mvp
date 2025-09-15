# src/scoring.py
import math
import pandas as pd
import os
from pathlib import Path

INPUT = "data/processed/clients_full.csv"
OUT_SCORES = "data/processed/scores.csv"         # все продукты для всех клиентов
OUT_TOP1 = "data/processed/scores_top1.csv"     # топ-1 продукт для каждого клиента

os.makedirs("data/processed", exist_ok=True)

# --- helper: найти сумму по категории, допускаем пробелы/подчёрки в названиях ---
def get_spent(client_row, name):
    keys = [f"spent_{name}", f"spent_{name.replace(' ', '_')}"]
    for k in keys:
        if k in client_row:
            return float(client_row.get(k) or 0.0)
    return 0.0

def fmt_kzt(x):
    """Форматирует число в строку с пробелами и ₽-подобной меткой '₸'.
    Возвращает '0 ₸' для None/NaN/нечисел."""
    try:
        val = float(x)
        if not math.isfinite(val):
            return "0 ₸"
        s = f"{int(round(val)):,}".replace(",", " ")
        return f"{s} ₸"
    except Exception:
        return "0 ₸"
    
# --- базовые коэффициенты и лимиты (тюнинг-параметры) ---
PARAMS = {
    "travel_cashback_pct": 0.04,        # 4% travel
    "travel_cashback_cap": 20000,       # макс кешбэк
    "taxi_pct": 0.03,
    "restaurants_pct": 0.02,
    "supermarket_pct": 0.03,
    "premium_balance_threshold": 300000, # KZT
    "premium_base_benefit": 2000,       # пример экономии/бонусов
    "deposit_annual_rate": 0.12,        # 12% годовых (пример)
    "deposit_min_balance": 50000,
    "credit_pct_est": 0.01,             # пример: 1% от оборота как выгода от кредитного продукта
    "fx_pct": 0.005,                    # экономия на FX/комиссиях
    "investment_min_balance": 200000,
    "investment_pct": 0.03,             # годовая возможная выгода (оценка)
}

# --- scoring functions (возвращают (benefit_est_KZT, reason_code, explain)) ---
def score_travel(client):
    travel = get_spent(client, "Путешествия")
    hotels = get_spent(client, "Отели")
    taxi = get_spent(client, "Такси")
    travel_volume = travel + hotels + taxi
    pct = PARAMS["travel_cashback_pct"]
    est = travel_volume * pct
    est = min(est, PARAMS["travel_cashback_cap"])
    reason = []
    if travel_volume > 0:
        reason.append("HIGH_TRAVEL_SPEND")
    if taxi > 0:
        reason.append("TAXI_PRESENT")
    explain = f"Траты на поездки: {fmt_kzt(travel_volume)}. Оценимый кешбэк ≈ {fmt_kzt(est)}"
    return round(est, 2), "|".join(reason) or "NO_SIGNAL", explain

def score_taxicard(client):
    taxi = get_spent(client, "Такси")
    est = taxi * PARAMS["taxi_pct"]
    reason = ["HIGH_TAXI"] if taxi > 20000 else []
    explain = f"По такси: {fmt_kzt(taxi)} → выгода ≈ {fmt_kzt(est)}"
    return round(est,2), "|".join(reason) or "NO_SIGNAL", explain

def score_restaurants(client):
    rest = get_spent(client, "Кафе и рестораны")
    est = rest * PARAMS["restaurants_pct"]
    reason = ["HIGH_RESTAURANTS"] if rest > 30000 else []
    explain = f"Траты в ресторанах: {fmt_kzt(rest)} → выгода ≈ {fmt_kzt(est)}"
    return round(est,2), "|".join(reason) or "NO_SIGNAL", explain

def score_supermarket(client):
    sup = get_spent(client, "Продукты питания")
    est = sup * PARAMS["supermarket_pct"]
    reason = ["HIGH_SUPERMARKET"] if sup > 50000 else []
    explain = f"Траты на продукты: {fmt_kzt(sup)} → выгода ≈ {fmt_kzt(est)}"
    return round(est,2), "|".join(reason) or "NO_SIGNAL", explain

def score_premium_card(client):
    bal = client.get("avg_monthly_balance_KZT") or client.get("avg_monthly_balance", 0)
    try:
        bal = float(bal)
    except:
        bal = 0.0
    est = 0.0
    reason = []
    if bal >= PARAMS["premium_balance_threshold"]:
        est = PARAMS["premium_base_benefit"] + 0.001 * bal  # пример: базовая + пропорция от баланса
        reason.append("HIGH_BALANCE")
    explain = f"Средний баланс: {fmt_kzt(bal)} → оценка выгоды ≈ {fmt_kzt(est)}"
    return round(est,2), "|".join(reason) or "NO_SIGNAL", explain

def score_deposit(client):
    bal = client.get("avg_monthly_balance_KZT") or client.get("avg_monthly_balance", 0)
    try:
        bal = float(bal)
    except:
        bal = 0.0
    if bal < PARAMS["deposit_min_balance"]:
        return 0.0, "LOW_BALANCE", "Недостаточный баланс для выгодного депозита"
    est = bal * PARAMS["deposit_annual_rate"] / 12.0  # месячная оценка
    explain = f"Если положить {fmt_kzt(bal)} на депозит (12% годовых), месячная выручка ≈ {fmt_kzt(est)}"
    return round(est,2), "DEPOSIT_OPPORTUNITY", explain

def score_credit_offer(client):
    total = client.get("total_spent") or 0
    avg_tx = client.get("avg_transaction") or 0
    try:
        total = float(total)
        avg_tx = float(avg_tx)
    except:
        total, avg_tx = 0.0, 0.0
    est = 0.0
    reason = []
    if avg_tx > 20000 or total > 300000:
        est = total * PARAMS["credit_pct_est"]
        reason.append("LARGE_PAYMENTS")
    explain = f"Оборот: {fmt_kzt(total)}, средний чек: {fmt_kzt(avg_tx)} → ожидаемая выгода от кредитного продукта ≈ {fmt_kzt(est)}"
    return round(est,2), "|".join(reason) or "NO_SIGNAL", explain

def score_fx(client):
    fx_volume = (client.get("transfers_in") or 0) + (client.get("transfers_out") or 0)
    est = fx_volume * PARAMS["fx_pct"]
    reason = ["FX_ACTIVITY"] if fx_volume > 0 else []
    explain = f"FX/переводы: {fmt_kzt(fx_volume)} → потенциальная экономия на комиссиях ≈ {fmt_kzt(est)}"
    return round(est,2), "|".join(reason) or "NO_SIGNAL", explain

def score_investments(client):
    bal = client.get("avg_monthly_balance_KZT") or client.get("avg_monthly_balance", 0)
    try:
        bal = float(bal)
    except:
        bal = 0.0
    if bal < PARAMS["investment_min_balance"]:
        return 0.0, "LOW_BALANCE", "Слишком мал баланс для инвестиционных продуктов"
    est = bal * PARAMS["investment_pct"] / 12.0
    explain = f"Средний баланс: {fmt_kzt(bal)} → месячная оценочная доходность инвестиций ≈ {fmt_kzt(est)}"
    return round(est,2), "INVEST_OPPORTUNITY", explain

def score_gold(client):
    jew = get_spent(client, "Ювелирные украшения") or 0.0
    try:
        jew = float(jew)
    except:
        jew = 0.0
    if not math.isfinite(jew) or jew <= 0:
        return 0.0, "NO_SIGNAL", "Нет трат на ювелирку"
    est = jew * 0.02
    explain = f"Траты на ювелирку: {fmt_kzt(jew)} → выгодна накопительная программа/золото ≈ {fmt_kzt(est)}"
    return round(est,2), "GOLD_INTEREST", explain

# список продуктов (имена в output)
PRODUCT_FUNCTIONS = {
    "travel_card": score_travel,
    "taxi_card": score_taxicard,
    "restaurants_card": score_restaurants,
    "supermarket_card": score_supermarket,
    "premium_card": score_premium_card,
    "deposit": score_deposit,
    "credit_offer": score_credit_offer,
    "fx_offer": score_fx,
    "investment_offer": score_investments,
    "gold_offer": score_gold,
}

def run_scoring():
    if not os.path.exists(INPUT):
        print(f"❌ Входной файл не найден: {INPUT}")
        return

    df = pd.read_csv(INPUT)

    # --- нормализуем числовые поля чтобы избежать NaN при расчетах ---
    num_cols = [c for c in df.columns if c.startswith("spent_")]
    for c in ["total_spent","avg_transaction","num_transactions","transfers_in","transfers_out","avg_monthly_balance_KZT"]:
        if c in df.columns and c not in num_cols:
            num_cols.append(c)

    if num_cols:
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    rows = []
    for _, r in df.iterrows():
        client = r.to_dict()
        client_code = client.get("client_code") or client.get("client_id") or client.get("client")
        for product, func in PRODUCT_FUNCTIONS.items():
            benefit, reason, explain = func(client)
            rows.append({
                "client_code": client_code,
                "product": product,
                "benefit_est_KZT": benefit,
                "reason_code": reason,
                "explain": explain
            })

    out_df = pd.DataFrame(rows)
    out_df.sort_values(["client_code", "benefit_est_KZT"], ascending=[True, False], inplace=True)
    # save all scores
    out_df.to_csv(OUT_SCORES, index=False, encoding="utf-8")
    # save top1 per client
    top1 = out_df.groupby("client_code").first().reset_index()
    top1.to_csv(OUT_TOP1, index=False, encoding="utf-8")

    print(f"✅ Сохранено: {OUT_SCORES}")
    print(f"✅ Топ-1 продукт для каждого клиента: {OUT_TOP1}")

if __name__ == "__main__":
    run_scoring()