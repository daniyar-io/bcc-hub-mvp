import pandas as pd
import os

RAW_PATH = "data/raw/"
PROCESSED_PATH = "data/processed/"

def load_transactions():
    df = pd.read_csv(os.path.join(RAW_PATH, "client_1_transactions_3m.csv"))
    print("Transactions loaded:", df.shape)
    return df

def load_transfers():
    df = pd.read_csv(os.path.join(RAW_PATH, "client_1_transfers_3m.csv"))
    print("Transfers loaded:", df.shape)
    return df

def clean_transactions(df):
    # Удаляем строки без категории или суммы
    if "category" in df.columns and "amount" in df.columns:
        df = df.dropna(subset=["category", "amount"])
    # Приводим дату к формату YYYY-MM-DD
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df

def run_etl():
    # Загрузка
    transactions = load_transactions()
    transfers = load_transfers()

    # Очистка
    transactions = clean_transactions(transactions)

    # Сохранение
    transactions.to_csv(os.path.join(PROCESSED_PATH, "transactions_clean.csv"), index=False)
    transfers.to_csv(os.path.join(PROCESSED_PATH, "transfers_clean.csv"), index=False)

    print("✅ ETL completed. Clean files saved to:", PROCESSED_PATH)

# потом идёт твоя функция
def load_and_clean(transactions_path, transfers_path):
    """Загрузка и базовая очистка данных клиента"""
    print(f"📂 Загружаем транзакции из {transactions_path}")
    transactions = pd.read_csv(transactions_path)

    print(f"📂 Загружаем переводы из {transfers_path}")
    transfers = pd.read_csv(transfers_path)

    # Приведение даты к формату datetime
    if "date" in transactions.columns:
        transactions["date"] = pd.to_datetime(transactions["date"], errors="coerce")
    if "date" in transfers.columns:
        transfers["date"] = pd.to_datetime(transfers["date"], errors="coerce")

    print("✅ Данные загружены и приведены к нужному формату")
    return transactions, transfers



if __name__ == "__main__":
    run_etl()
