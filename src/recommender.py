import pandas as pd
import os
import sys

PROCESSED_PATH = "data/processed/clients_full.csv"
REPORTS_DIR = "reports/"

def load_data():
    """Загружаем объединённые данные по всем клиентам"""
    if not os.path.exists(PROCESSED_PATH):
        raise FileNotFoundError(f"❌ Файл {PROCESSED_PATH} не найден. Сначала запусти merge_data.py")
    return pd.read_csv(PROCESSED_PATH)


def define_segment(features: dict):
    """Определяем сегмент клиента"""
    if features.get("spent_Путешествия", 0) > 100000:
        return "Путешественник"
    elif features.get("spent_Кафе и рестораны", 0) > 80000:
        return "Гурман"
    elif features.get("spent_Такси", 0) > 40000:
        return "Активный горожанин"
    elif features.get("spent_Продукты питания", 0) > 100000:
        return "Домосед"
    else:
        return "Базовый клиент"


def generate_recommendations(segment: str):
    """Рекомендации по сегменту"""
    recs = []

    if segment == "Путешественник":
        recs.append("Travel-карта с кешбэком на билеты и отели")
        recs.append("Страховка для путешествий")
        recs.append("Карта для выгодных конвертаций валют")

    elif segment == "Гурман":
        recs.append("Карта с кешбэком на рестораны и кафе")
        recs.append("Участие в программе лояльности с ресторанами-партнёрами")

    elif segment == "Активный горожанин":
        recs.append("Карта с кешбэком на такси и транспорт")
        recs.append("Специальные предложения на городские сервисы")

    elif segment == "Домосед":
        recs.append("Карта с кешбэком на супермаркеты и онлайн-покупки")
        recs.append("Программы бонусов для подписок (кино, игры)")

    else:
        recs.append("Базовый пакет услуг")
        recs.append("Персональные предложения будут доступны при активности")

    return recs


def save_report(client_code, segment, recs):
    """Сохраняем рекомендации в markdown-файл для конкретного клиента"""
    os.makedirs(REPORTS_DIR, exist_ok=True)
    file_path = os.path.join(REPORTS_DIR, f"client_{client_code}_recs.md")

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(f"# Recommendations Report — Client {client_code}\n\n")
        f.write(f"**Сегмент клиента:** {segment}\n\n")
        f.write("**Рекомендации:**\n")
        for i, r in enumerate(recs, 1):
            f.write(f"{i}. {r}\n")

    print(f"📄 Рекомендации сохранены в {file_path}")


def run_recommender(client_code):
    df = load_data()

    if client_code not in df["client_code"].values:
        print(f"❌ Клиент {client_code} не найден в данных")
        return

    client = df[df["client_code"] == client_code].iloc[0].to_dict()
    segment = define_segment(client)
    recs = generate_recommendations(segment)

    print("✅ Рекомендации для клиента:")
    print(f"Имя: {client.get('name', '-')}, Город: {client.get('city', '-')}")
    print(f"Сегмент: {segment}")
    for i, r in enumerate(recs, 1):
        print(f"{i}. {r}")

    save_report(client_code, segment, recs)


if __name__ == "__main__":
    # Проверяем, передан ли аргумент в командной строке
    if len(sys.argv) > 1:
        try:
            client_code = int(sys.argv[1])
            run_recommender(client_code)
        except ValueError:
            print("❌ Ошибка: client_code должен быть числом")
    else:
        print("⚠️ Использование: python src/recommender.py <client_code>")
        print("Пример: python src/recommender.py 1")
