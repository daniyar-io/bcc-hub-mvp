# 🏦 BCC Hub MVP
*Персонализированные предложения для клиентов банка*

---

## 📌 Цель проекта
Создать MVP-систему персонализации, которая:

- анализирует поведение клиентов (транзакции, переводы),
- формирует их профиль (признаки и потребности),
- оценивает выгоды по продуктам банка,
- генерирует персональные push-уведомления,
- проверяет их качество по чеклисту.

---

## 📁 Структура проекта

bcc-hub-mvp/
 ├─ data/raw/            # client_<id>_transactions_3m.csv, client_<id>_transfers_3m.csv, clients.csv
 ├─ data/processed/      # обработанные данные и результаты
 ├─ reports/             
 │   ├─ pushes/           # push по каждому клиенту
 │   └─ evaluation.md     # отчёт по качеству push
 └─ src/                 # исходный код системы


---

## ⚙ Установка
bash
pip install pandas python-pptx


---

## 🚀 Пошаговый запуск

### 1) Подготовить данные
Положить файлы:
- client_<id>_transactions_3m.csv
- client_<id>_transfers_3m.csv
в data/raw/, и clients.csv в data/raw/.

### 2) Создать признаки
bash
python src/features.py

➡ Результат: data/processed/clients_features.csv

### 3) Объединить с анкетой клиентов
bash
python src/merge_data.py

➡ Результат: data/processed/clients_full.csv

### 4) Рассчитать скоринг (опционально)
bash
python src/scoring.py

➡ Результат: data/processed/scores.csv

### 5) Сгенерировать push-уведомления
bash
python src/generate_push.py

➡ Результаты:
- data/processed/push_results.csv
- reports/pushes/client_<id>_push.md

### 6) Проверить качество
bash
python src/evaluate.py

➡ Результаты:
- reports/evaluation.md
- data/processed/scores_metrics.csv (если есть target_product)

---

## 📊 Что проверяется в evaluate.py
- Персонализация и уместность
- Наличие CTA (призыв к действию)
- Ясность и краткость (<200 символов)
- Уникальность сообщений
- Точность рекомендаций (Top-1 / Top-4)

---

## ✅ Чек-лист готовности к демо

- [ ] clients_full.csv создан
- [ ] scores.csv создан (если применимо)
- [ ] push_results.csv содержит корректные push
- [ ] В reports/pushes/ есть примеры
- [ ] evaluation.md создан и без ошибок
- [ ] Запуск generate_push.py работает стабильно
- [ ] Презентация presentation.pptx готова

---

## ⚡ Быстрый запуск пайплайна
bash
python src/features.py
python src/merge_data.py
python src/scoring.py
python src/generate_push.py
python src/evaluate.py


---

## 📌 Команда
*BCC Hub | Hackathon MVP*