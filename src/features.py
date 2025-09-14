import pandas as pd
import os
import glob
from datetime import datetime

RAW_PATH = "data/raw/"
PROCESSED_PATH = "data/processed/"

def load_client_data(client_id):
    """Загружаем транзакции и переводы для клиента"""
    transactions_file = os.path.join(RAW_PATH, f"client_{client_id}_transactions_3m.csv")
    transfers_file = os.path.join(RAW_PATH, f"client_{client_id}_transfers_3m.csv")

    if not os.path.exists(transactions_file) or not os.path.exists(transfers_file):
        print(f"⚠️ Нет данных для клиента {client_id}")
        return None, None

    transactions = pd.read_csv(transactions_file)
    transfers = pd.read_csv(transfers_file)
    return transactions, transfers


def extract_features(client_id, transactions, transfers):
    """Извлекаем признаки клиента"""
    features = {"client_id": client_id}

    # Общие расходы
    features["total_spent"] = transactions["amount"].sum()

    # Средний чек
    features["avg_transaction"] = transactions["amount"].mean()

    # Количество транзакций
    features["num_transactions"] = transactions.shape[0]

    # Расходы по категориям (чистим названия колонок)
    cat_sum = transactions.groupby("category")["amount"].sum().to_dict()
    for cat, value in cat_sum.items():
        col_name = f"spent_{cat.replace(' ', '_')}"
        features[col_name] = value

    # Переводы
    if "direction" in transfers.columns:
        transfers["direction"] = transfers["direction"].str.lower()
        incoming = transfers[transfers["direction"] == "in"]["amount"].sum()
        outgoing = transfers[transfers["direction"] == "out"]["amount"].sum()
        features["transfers_in"] = incoming
        features["transfers_out"] = outgoing

    return features


def run_features():
    print("🚀 Извлечение признаков для всех клиентов...")

    # ищем все транзакционные файлы
    transaction_files = glob.glob(os.path.join(RAW_PATH, "client_*_transactions_3m.csv"))
    client_ids = [os.path.basename(f).split("_")[1] for f in transaction_files]

    all_features = []

    for client_id in client_ids:
        transactions, transfers = load_client_data(client_id)
        if transactions is not None and transfers is not None:
            features = extract_features(client_id, transactions, transfers)
            all_features.append(features)
            print(f"✅ Клиент {client_id} обработан")

    # сохраняем в общий файл
    if all_features:
        df = pd.DataFrame(all_features)
        os.makedirs(PROCESSED_PATH, exist_ok=True)

        # основной файл
        output_file = os.path.join(PROCESSED_PATH, "clients_features.csv")

        # архивная копия с датой
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_file = os.path.join(PROCESSED_PATH, f"clients_features_{timestamp}.csv")

        # если файл существует — удаляем перед записью
        if os.path.exists(output_file):
            os.remove(output_file)

        df.to_csv(output_file, index=False)
        df.to_csv(archive_file, index=False)

        print(f"📄 Файл clients_features.csv сохранён: {output_file}")
        print(f"🗄 Архивная версия сохранена: {archive_file}")
    else:
        print("⚠️ Не найдено клиентов для обработки")


if __name__ == "__main__":
    run_features()
