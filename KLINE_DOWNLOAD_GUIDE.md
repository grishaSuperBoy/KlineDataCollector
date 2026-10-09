# Инструкция и Спецификация Скачивания 1m Klines и Funding Rates (KlineDataCollector)

> **Назначение документа**: Данный документ является полным техническим руководством для разработчиков и AI-агентов по работе с проектом `KlineDataCollector`: формирование списков монет, REST API 10 криптовалютных бирж, оркестрация параллельной загрузки, сбор ставок финансирования (funding rate), и последующая нормализация данных.

---

## 1. Архитектура Проекта

1. **Цель**: Собрать единый, стандартизированный архив минутных свечей (1m OHLCV) и исторических ставок финансирования (Funding Rates) по всем ликвидным альткоинам для кросс-биржевого анализа дислокаций спреда, ордербуков (L2/L3), маркет-мейкинга и бэктестинга арбитражных стратегий.
2. **Временное окно**: Задаётся динамически в начале `crypto_alts_multi_exchange.json`:
   - `collectFrom`: дата начала сбора (например, `"2026-07-25 00:00:00"`).
   - `collectTo`: дата окончания (например, `"present"` для скачивания до текущего момента или конкретная дата `"2026-10-08 00:00:00"`).
3. **Хранилище**:
   - Минутные свечи: `tf_1min_kline_data/<exchange>/`
   - Ставки финансирования (сырые): `funding_rate_data/<exchange>/`
   - Нормализованный фандинг: `funding/<exchange>/`

---

## 2. Структура Конфигурации (`crypto_alts_multi_exchange.json`)

Файл конфигурации монет и диапазона дат расположен в корне проекта:
`crypto_alts_multi_exchange.json`.

Формат файла:
```json
{
  "collectFrom": "2026-07-25 00:00:00",
  "collectTo": "present",
  "exchanges": [
    {
      "exchange": "binance",
      "coins": ["AAVE", "ADA", "APT", "ARB", "AVAX", "DOGE", "SOL", "..."]
    },
    {
      "exchange": "bybit",
      "coins": ["AAVE", "ADA", "APT", "ARB", "AVAX", "DOGE", "SOL", "..."]
    },
    {
      "exchange": "okx",
      "coins": ["AAVE", "ADA", "APT", "ARB", "AVAX", "DOGE", "SOL", "..."]
    }
  ]
}
```

### Поддерживаемые форматы дат:
- Строка: `"YYYY-MM-DD HH:MM:SS"`, `"YYYY-MM-DD"`, `"YYYY-MM-DDTHH:MM:SSZ"`
- Ключевые слова: `"present"`, `"now"`, `"latest"` (означает текущее UTC время)
- Unix Timestamp в миллисекундах или секундах (например, `1784937600000`).

---

## 3. Критерии Формирования Списка Монет

Любой бот, обновляющий или создающий этот список, **ОБЯЗАН** строго соблюдать следующие фильтры:

### 3.1. Критерии Фильтрации
1. **Только чистые крипто-альткоины (Pure Crypto Altcoins)**:
   - Допускаются только нативные криптовалюты и DeFi/Web3/Meme токены (SOL, XRP, DOGE, AVAX, SUI, PEPE, NEAR, APT, etc.).
2. **СТРОГИЙ ЗАПРЕТ на Главные Монеты (No BTC, No ETH)**:
   - `BTC` (Bitcoin) и `ETH` (Ethereum) исключены. Они собираются отдельно с другими весами ликвидности.
3. **СТРОГИЙ ЗАПРЕТ на TradFi / Традиционные Акции / Товары / Индексы / Фонды**:
   - Деривативные биржи (Gate.io, BingX, MEXC) часто листят синтетические контракты на акции США, товары и индексы.
   - **Черный список TradFi**:
     - Акции: `AAPL`, `TSLA`, `NVDA`, `AMZN`, `MSFT`, `GOOGL`, `META`, `AMD`, `NFLX`, `BABA`, `COIN`, `PLTR`, `MSTR`, etc.
     - Индексы: `SPX`, `SPY`, `QQQ`, `DJI`, `NDX`, `VIX`, etc.
     - Товары: `XAU`, `XAG`, `WTI`, `BRENT`, `COPPER`, `NATGAS`, etc.
4. **Кросс-биржевой критерий (Cross-Exchange Presence > 1)**:
   - Монета обязана торговаться минимум на 2 биржах из списка поддерживаемых. Одиночные неликвидные инструменты отсеиваются.

---

## 4. Запуск Сбора Данных

### 4.1. Оркестратор Сбора Klines (10 бирж)
```bash
# Запуск параллельного сбора по очереди с контролем нагрузки (2 параллельных процесса)
python pipeline_orchestrator.py --max-concurrent 2

# Запуск только выбранных бирж
python pipeline_orchestrator.py --exchanges binance,bybit,okx

# Переопределение дат из командной строки
python pipeline_orchestrator.py --from "2026-08-01 00:00:00" --to "2026-10-01 00:00:00"

# Скачивание klines и сразу funding rates
python pipeline_orchestrator.py --with-funding
```

### 4.2. Точечный Запуск Отдельных Бирж
Каждый скрипт скачивания биржи (`download_<exchange>.py`) поддерживает независимый запуск:
```bash
python download_binance.py
python download_bybit.py
python download_okx.py
python download_gateio.py
python download_bitget.py
python download_kucoin.py
python download_bingx.py
python download_mexc.py
python download_hyperliquid.py
python download_asterdex.py
```

### 4.3. Сбор Funding Rates
```bash
# Скачивание funding rates по всем 10 биржам
python download_funding.py --exchange all

# Скачивание для конкретной биржи
python download_funding.py --exchange binance

# Принудительная перезагрузка
python download_funding.py --exchange bybit --force
```

---

## 5. Нормализация Данных

### 5.1. Нормализация 1-min Klines (`normalize_klines.py`)
Приводит файлы к стандартизированному формату и генерирует отчет покрытия `coverage.csv`:
```bash
python normalize_klines.py --data-dir tf_1min_kline_data --workers 4
```

Схема выходного Parquet:
- `timestamp` (int64, ms, UTC)
- `date` (datetime64[ns, UTC])
- `exchange` (str)
- `symbol_base` (str)
- `symbol_original` (str)
- `open`, `high`, `low`, `close` (float64)
- `volume` (float64, в базовой монете)
- `quote_volume` (float64, volume * close, в USDT)

### 5.2. Нормализация Funding Rates (`normalize_funding.py`)
Приводит ставки к долям (0.0001 = 0.01%), определяет шаг начисления `interval_hours` (8h, 4h, 1h) и генерирует `funding_coverage.csv`:
```bash
python normalize_funding.py --raw-dir funding_rate_data --out-dir funding --workers 4
```

Схема выходного Parquet:
- `timestamp` (int64, ms, UTC)
- `date` (datetime64[ns, UTC])
- `exchange` (str)
- `symbol_base` (str)
- `symbol_original` (str)
- `funding_rate` (float64, в долях)
- `funding_rate_pct` (float64, в процентах)
- `interval_hours` (int, 8, 4 или 1)
