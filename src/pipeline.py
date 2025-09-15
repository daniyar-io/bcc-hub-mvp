import os
import pandas as pd

from data_ingest import load_and_clean
from features import run_features
from merge_data import merge_data
from recommender import run_recommender

PROCESSED_FULL = "data/processed/clients_full.csv"

def run_pipeline():
    print("🚀 Запуск пайплайна...")

    # 1. Формирование признаков для всех клиентов
    print("\n🧮 Шаг 1. Формирование признаков...")
    run_features()

    # 2. Объединение с профилями клиентов
    print("\n🔗 Шаг 2. Объединение с профилями...")
    merge_data()

    # 3. Генерация рекомендаций для всех клиентов
    print("\n🤖 Шаг 3. Генерация рекомендаций...")
    if os.path.exists(PROCESSED_FULL):
        df = pd.read_csv(PROCESSED_FULL)
        for client_code in df["client_code"].unique():
            run_recommender(client_code)
    else:
        print("❌ Нет файла clients_full.csv — сначала запусти merge_data.py")

    print("\n✅ Пайплайн выполнен!")


if __name__ == "__main__":
    run_pipeline()
