# Synthetic Data Generator for DEV

Генерирует синтетические CSV-данные с использованием PySpark для тестового окружения (`DEV`).
Имена и города берутся из реалистичных внешних библиотек.

## 🛠️ Technologies

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.4+-f77f00?logo=apachespark&logoColor=white)
![Make](https://img.shields.io/badge/Makefile-Build%20Automation-lightgrey?logo=gnubash&logoColor=black)
![names](https://img.shields.io/badge/names-библиотека-blueviolet)
![geonamescache](https://img.shields.io/badge/geonamescache-библиотека-forestgreen)

## 📦 Data structure

- `id`: Уникальный идентификатор
- `name`: Случайное имя из библиотеки `names`
- `email`: Адрес электронной почты, составленный из имени + домен `.ru` или `.com`
- `city`: Название города из `geonamescache` (минимум 7 символов)
- `age`: Возраст (от 18 до 85 лет)
- `salary`: Зарплата (от 30,000 до 250,000, всегда заканчивается на `.00`)
- `registration_date`: Дата регистрации (не раньше чем возраст \* 365 дней назад)
- До 5% значений в каждом столбце могут быть `NULL` (имитация пропущенных данных).

## 📁 Project structure

```
synthetic_data_generator_dev/
├── src/
│   ├── generator.py          # Логика генерации синтетических данных
│   ├── utils.py              # Вспомогательные функции: имена, города, UDF
│
├── data/                     # Каталог с выходными CSV-файлами
│   └── 2025-05-15-dev.csv    # Пример сгенерированного файла
│
├── scripts/
│   ├── validate.py           # Скрипт валидации данных (NULL, границы, типы)
│   └── ...                   # Другие скрипты
│
├── Makefile                 # Автоматизация запуска (make run / validate / all)
├── main.py                  # Главный скрипт генерации (точка входа)
├── requirements.txt         # Python-зависимости
└── README.md                # Документация проекта
```

## 📄 Example of generated data

| id  | name    | email               | city        | age | salary    | registration_date |
| --- | ------- | ------------------- | ----------- | --- | --------- | ----------------- |
| 1   | John    | John@example.ru     | Amsterdam   | 23  | 35000.00  | 2020-04-10        |
| 2   | Alice   | Alice@example.com   | Barcelona   | 41  | 250000.00 | 2011-02-25        |
| 3   | NULL    | NULL                | SanAntonio  | 65  | NULL      | 1990-10-19        |
| 4   | Michael | Michael@example.ru  | NULL        | 30  | 78000.00  | 2017-03-12        |
| 5   | Jessica | Jessica@example.com | NewYorkCity | 29  | 50000.00  | 2018-08-01        |

> 📝 **Note:**
>
> - Поля `name`, `email`, `city`, `age`, `salary` могут содержать значения NULL не более чем в 5% строк.
> - `registration_date` зависит от возраста и не может быть раньше, чем `current_date - age * 365 дней`.

## 🚀 🛠️ Запуск через Makefile

🔹 Генерация данных:

```bash
make run
```

🔹 Валидация сгенерированных данных:

```bash
make validate
```

🔹 Полный цикл: генерация + валидация:

```bash
make all
```
