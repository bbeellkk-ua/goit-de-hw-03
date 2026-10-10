# GOIT DE HW 03 — аналіз даних у PySpark

Проєкт містить Docker-середовище з Java 17 і PySpark 3.5.5 та реалізацію
домашнього завдання в `src/homework.py`.

## Структура

```text
.
├── data/
│   ├── products.csv
│   ├── purchases.csv
│   └── users.csv
├── src/
│   └── homework.py
├── compose.yaml
├── Dockerfile
└── requirements.txt
```

## Запуск середовища

Зібрати образ і запустити контейнер:

```bash
docker compose up --build -d
```

Репозиторій примонтовано в контейнер як `/workspace`, тому зміни зберігаються
на локальному комп'ютері. Spark UI доступний на <http://localhost:4040> під час
виконання Spark job.

Переглянути логи або зупинити середовище:

```bash
docker compose logs -f spark
docker compose down
```

## Запуск домашнього завдання

У вже запущеному контейнері:

```bash
docker compose exec spark spark-submit src/homework.py --data-dir data
```

Або одноразово, без попереднього запуску контейнера:

```bash
docker compose run --rm spark spark-submit src/homework.py --data-dir data
```

Скрипт:

1. читає три CSV з явними схемами;
2. видаляє рядки, які містять хоча б одне пропущене значення;
3. обчислює суму `price * quantity` за кожною категорією;
4. повторює обчислення для користувачів віком від 18 до 25 років включно;
5. обчислює відсоток кожної категорії від усіх витрат цієї вікової групи;
6. виводить три категорії з найбільшим відсотком, округленим до двох знаків.

## Корисні команди

Відкрити shell усередині контейнера:

```bash
docker compose exec spark bash
```

Запустити PySpark REPL:

```bash
docker compose exec spark pyspark
```