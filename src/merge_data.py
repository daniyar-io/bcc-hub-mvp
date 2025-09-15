import pandas as pd
import os
from datetime import datetime

RAW_PATH = "data/raw/clients.csv"
PROCESSED_PATH = "data/processed/clients_features.csv"
OUTPUT_PATH = "data/processed/clients_full.csv"

def merge_data():
    # Загружаем данные
    clients = pd.read_csv(RAW_PATH)
    features = pd.read_csv(PROCESSED_PATH)

    print("Колонки в clients.csv:", clients.columns.tolist())
    print("Колонки в clients_features.csv:", features.columns.tolist())

    # Определяем ключи для объединения
    if "client_id" in clients.columns and "client_id" in features.columns:
        key_clients, key_features = "client_id", "client_id"
    elif "client_code" in clients.columns and "client_id" in features.columns:
        key_clients, key_features = "client_code", "client_id"
    elif "id" in clients.columns and "client_id" in features.columns:
        key_clients, key_features = "id", "client_id"
    else:
        raise KeyError("Не найден общий ключ для объединения. Проверь названия колонок.")

    # Объединяем
    df = pd.merge(clients, features, left_on=key_clients, right_on=key_features, how="inner")

    os.makedirs("data/processed", exist_ok=True)

    # Архивная версия
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_file = f"data/processed/clients_full_{timestamp}.csv"

    # Если файл существует — удаляем
    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)

    # Сохраняем обе версии
    df.to_csv(OUTPUT_PATH, index=False)
    df.to_csv(archive_file, index=False)

    print(f"✅ Итоговый файл сохранён: {OUTPUT_PATH}")
    print(f"🗄 Архивная версия сохранена: {archive_file}")


if __name__ == "__main__":
    merge_data()
