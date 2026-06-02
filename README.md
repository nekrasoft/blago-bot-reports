# bot-reports

Бот для отчётов водителя мусоровоза. Один launcher запускает Telegram- или
MAX-версию. Основные сценарии: отчёт о вывозе контейнеров, заявка на
опустошение бункеров, учёт ходок с путевыми листами. В MAX дополнительно есть
учёт рабочего времени водителя.

Этот README предназначен в первую очередь для агентов, которые впервые
открывают проект. Перед изменениями также обязательно прочитать
[`AGENTS.md`](AGENTS.md).

## Быстрый контекст

- Точка входа: [`run.py`](run.py). Переменная `BOT_PLATFORM` выбирает
  `telegram` (по умолчанию) или `max`.
- Telegram: [`bot.py`](bot.py), диалоги вынесены в
  [`bunker_report.py`](bunker_report.py) и [`trip_report.py`](trip_report.py).
- MAX: [`max_bot.py`](max_bot.py). Здесь продублированы платформенные версии
  диалогов и добавлена команда `/v`.
- API карты: [`map_client.py`](map_client.py). Источник бункеров и контрагентов,
  а также запись изменения заполненности и даты вывоза.
- Google Sheets: [`sheets_client.py`](sheets_client.py). Финальное хранилище
  отчётов.
- MySQL: [`waybill_files_db.py`](waybill_files_db.py) и
  [`driver_work_time_db.py`](driver_work_time_db.py). Файлы путевых листов и
  рабочее время.
- [`parser.py`](parser.py) содержит OpenAI-парсер текстовых сообщений, но
  текущие точки входа его не вызывают. Не считайте его частью активного
  runtime без отдельного подключения.

## Активные команды

| Команда | Telegram | MAX | Назначение |
| --- | --- | --- | --- |
| `/bunker`, `/b` | да | да | Вывоз контейнеров: выбрать бункеры со `fillLevel == 100` |
| `/zayavka`, `/z` | да | да | Заявка на опустошение: выбрать бункеры со `fillLevel < 100` |
| `/h` | да | да | Ходки: контрагент, количество, объём, опциональный путевой лист |
| `/v` | нет | да | Рабочее время водителя за текущий день |
| `/cancel` | в диалоге `/h` | текстом в активном диалоге | Отмена |

В комментариях к старому коду встречается `/report`, но handler для этой
команды сейчас не зарегистрирован.

## Основные потоки

### Вывоз контейнеров: `/bunker`

1. Бот получает бункеры через `GET /api/bunkers`.
2. В списке остаются только бункеры со `fillLevel == 100`.
3. При каждом выборе бот сразу вызывает `PUT /api/bunkers/{id}`:
   устанавливает `lastPickupDate` и `fillLevel = 0`.
4. После кнопки `Готово` выбранные бункеры группируются по контрагенту и
   примечанию, затем строки записываются в Google Sheets.
5. Номера бункеров добавляются в примечание в формате `# 2,4,5`.

Важно: обновление карты происходит до записи в Google Sheets. При ошибке
Sheets автоматического rollback нет.

### Заявка на опустошение: `/zayavka`

1. Бот получает бункеры через `GET /api/bunkers`.
2. В списке остаются только бункеры со `fillLevel < 100`.
3. При каждом выборе бот сразу вызывает
   `POST /api/bunkers/{id}/mark-filled`.
4. В Google Sheets заявка не записывается.

### Ходки: `/h`

1. Контрагенты загружаются через `GET /api/counterparties`.
2. Показываются только записи с `operation_type=trip_removal`.
3. Пользователь выбирает контрагента, вводит число ходок и объём. Для кнопок
   кузова `30 м3` и `36 м3` объём умножается на количество ходок.
4. Для контрагента `Частник` бот запрашивает наличку и сразу пишет отчёт.
5. Для остальных контрагентов можно приложить фото или PDF путевого листа
   либо пропустить файл.
6. Путевой лист сохраняется в MySQL `works_files`, его token добавляется в
   примечание Sheets как `[ПЛ:wb_...]`.
7. Итоговая строка добавляется в Google Sheets.

Лимит файла задаётся через `WAYBILL_MAX_FILE_SIZE_BYTES`, по умолчанию 10 МБ.

### Рабочее время: `/v`

Сценарий доступен только в MAX. Бот сохраняет начало, окончание и длительность
работы в таблицу MySQL `driver_work_time`. Для одного пользователя допускается
одна запись за день; повторная команда `/v` запускает подтверждение замены.

## Интеграции и конфигурация

Скопируйте [`.env.example`](.env.example) в `.env` и задайте нужные значения.
Секреты и credentials не коммитить.

| Переменная | Для чего нужна |
| --- | --- |
| `BOT_PLATFORM` | `telegram` или `max` |
| `TELEGRAM_BOT_TOKEN` | token Telegram-бота |
| `MAX_BOT_TOKEN` | token MAX-бота |
| `ALLOWED_CHAT_IDS` | whitelist ID групп через запятую; пустое значение снимает ограничение |
| `MAP_SERVICE_URL` | базовый URL API карты |
| `MAP_BOT_API_KEY` | ключ записи в API карты |
| `MAP_BOT_READ_API_KEY` | отдельный ключ чтения; если пусто, используется `MAP_BOT_API_KEY` |
| `GOOGLE_SHEET_URL` | URL Google-таблицы, желательно вместе с `gid` листа |
| `GOOGLE_CREDENTIALS_PATH` | JSON сервисного аккаунта Google |
| `MYSQL_HOST`, `MYSQL_PORT` | адрес MySQL |
| `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE` | подключение к MySQL |
| `WAYBILL_MAX_FILE_SIZE_BYTES` | максимальный размер путевого листа |
| `SQL_ECHO` | SQLAlchemy logging при `1`, `true` или `yes` |
| `OPENAI_API_KEY`, `OPENAI_MODEL` | нужны только для не подключённого сейчас `parser.py` |

JSON сервисного аккаунта Google по умолчанию ожидается по пути
`credentials/google_service_account.json`. Каталог `credentials/` и файл
`.env` исключены из Git.

## Данные и схема

| Файл | Назначение |
| --- | --- |
| [`config/schema.json`](config/schema.json) | Колонки Sheets, переносимые формулы, fallback URL таблицы |
| [`data/operations.json`](data/operations.json) | Реквизиты операций; активно используется для ходок и OpenAI-парсера |
| [`data/counterparties.json`](data/counterparties.json) | Справочник для OpenAI-парсера, не для интерактивных команд |
| [`data/parsing_rules.md`](data/parsing_rules.md) | Документация правил OpenAI-парсера |

Интерактивная команда `/h` берёт контрагентов из API карты, а не из
`data/counterparties.json`.

`sheets_client.append_rows()` ищет последнюю заполненную строку по колонке
`Дата`, переносит формулы из предыдущих строк и обновляет только необходимые
диапазоны. При изменении колонок синхронно проверяйте `google_sheet_columns`,
`fill_columns` и `formula_columns`.

### Таблицы MySQL

- `works_files` должна существовать до запуска. Код не создаёт её миграцией.
  Для ожидаемой семантики upsert проверьте unique index по `file_token`.
- `driver_work_time` создаётся через SQLAlchemy при первом обращении.
  Unique constraint `uq_driver_work_time_source_user_date` покрывает условия
  чтения и upsert: `source`, `source_user_id`, `work_date`.

При любых изменениях SQL или условий `WHERE` обязательно проверяйте индексы.

## Карта файлов

| Файл | Когда менять |
| --- | --- |
| [`run.py`](run.py) | Выбор платформы при запуске |
| [`bot.py`](bot.py) | Telegram bootstrap, whitelist групп, общий error handler |
| [`bunker_report.py`](bunker_report.py) | Telegram UI выбора бункеров и общие helpers форматирования |
| [`trip_report.py`](trip_report.py) | Telegram-сценарий ходок и загрузки путевых листов |
| [`max_bot.py`](max_bot.py) | MAX bootstrap и MAX-версии всех диалогов |
| [`map_client.py`](map_client.py) | Контракт API карты и сборка строк контейнерных отчётов |
| [`sheets_client.py`](sheets_client.py) | Запись строк и формул в Sheets |
| [`waybill_files_db.py`](waybill_files_db.py) | Подключение MySQL и upsert путевых листов |
| [`driver_work_time_db.py`](driver_work_time_db.py) | Учёт рабочего времени MAX |
| [`waybill_notes.py`](waybill_notes.py) | Генерация и разбор token путевого листа |
| [`parser.py`](parser.py) | Не подключённый OpenAI-парсер свободного текста |

## Запуск

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python run.py
```

Для прямой диагностики можно запускать `python bot.py` или `python max_bot.py`,
но штатная точка входа — `python run.py`.

## Проверки

Базовая проверка после любых изменений:

```bash
TMPDIR=/tmp TEMP=/tmp TMP=/tmp python -m compileall .
```

[`test_map_integration.py`](test_map_integration.py) — ручной интеграционный
smoke test. Он делает `PUT` для первого бункера и затем пытается восстановить
исходный `fillLevel`, поэтому не запускайте его как безвредный unit test:

```bash
python test_map_integration.py
```

Автоматизированных unit tests в репозитории сейчас нет.

## Известные риски и tech debt

- `ALLOWED_CHAT_IDS` проверяется при добавлении бота в группу, но не перед
  каждой командой. Пустой whitelist означает доступ без ограничений. Для
  security by design стоит централизовать авторизацию входящих команд.
- Синхронные вызовы `httpx`, `gspread` и SQLAlchemy выполняются внутри async
  handlers. При росте нагрузки вынести blocking I/O в worker/thread и
  батчить обращения к Google Sheets.
- Telegram- и MAX-реализации `/h` заметно дублируют бизнес-логику. При
  следующем функциональном изменении сначала оценить выделение общих helpers,
  чтобы платформы не разошлись по поведению.
- MAX загружает вложение по URL из payload события. Для защиты от SSRF стоит
  проверить allowlist доменов MAX, запрет приватных адресов и redirect policy.
- Операции с картой и Sheets не образуют транзакцию. Для гарантированной
  согласованности нужен retry/outbox либо компенсирующее обновление карты.
- `.env.example` пока не содержит `MAP_BOT_READ_API_KEY`, хотя код его
  поддерживает.

Не исправляйте эти пункты попутно: сначала согласуйте scope конкретной задачи.
