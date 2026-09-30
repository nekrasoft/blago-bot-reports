# MAX Bot API: требования и диагностика доставки команд

Проверено по официальной документации MAX 30 сентября 2026 года.

## Что могло сломать уже работающего бота

1. **Миграция API с 19 июля 2026 года.** Запросы нужно отправлять на `https://platform-api2.max.ru`, а в trust store клиента добавить сертификат Минцифры. Старый `platform-api.max.ru` больше не является корректным endpoint. [Обзор API](https://dev.max.ru/docs-api), [история изменений](https://dev.max.ru/docs-api/changelog-api).
2. **Изменение авторизации.** Токен в query-параметре больше не поддерживается. Он должен передаваться заголовком `Authorization: <access_token>` (без опубликования токена в логах и диагностических отчётах). [Обзор API](https://dev.max.ru/docs-api).
3. **Webhook и Long Polling взаимоисключающие.** При наличии Webhook-подписки `GET /updates` не работает. Текущий `max_bot.py` запускает Long Polling, поэтому случайно оставшаяся подписка — одна из первых проверок. MAX считает Long Polling средством разработки/тестирования; для production предписан Webhook. [Уведомления о событиях](https://dev.max.ru/docs-api/use-cases/event-notifications), [GET `/updates`](https://dev.max.ru/docs-api/methods/GET/updates).
4. **Webhook мог быть автоматически удалён.** Endpoint должен вернуть ровно HTTP `200` за 30 секунд. После неудачной доставки MAX делает до 10 повторов; если успешного ответа нет 8 часов, бот автоматически отписывается. [POST `/subscriptions`](https://dev.max.ru/docs-api/methods/POST/subscriptions).
5. **Неверный фильтр событий.** Текстовые команды приходят как `message_created`, нажатия кнопок — `message_callback`, начало/возобновление общения — `bot_started`. Если `update_types` не включает нужный тип, обработчик его не увидит. В групповых чатах и каналах бот должен быть администратором, чтобы получать события. [Объект `Update`](https://dev.max.ru/docs-api/objects/Update), [GET `/updates`](https://dev.max.ru/docs-api/methods/GET/updates).

`PATCH /me/commands` лишь управляет подсказками команд в UI; он не заменяет получение и обработку `message_created`. [Команды чат-бота](https://dev.max.ru/docs-api/use-cases/chatbot-commands), [PATCH `/me/commands`](https://dev.max.ru/docs-api/methods/PATCH/me/commands).

## Актуальные требования

### Исходящие запросы к MAX

- только HTTPS к `platform-api2.max.ru`;
- сертификат Минцифры должен быть доверенным в системном/Python trust store;
- `Authorization: <access_token>`, не query-параметр и не задокументированная схема `Bearer`;
- не более 30 запросов в секунду к `platform-api2.max.ru` для стабильной работы;
- типовые ответы: `401` — неверный/недействительный токен, `429` — превышение лимита, `503` — недоступность сервиса.

Источники: [обзор API](https://dev.max.ru/docs-api), [уведомления о событиях](https://dev.max.ru/docs-api/use-cases/event-notifications).

### Webhook

- публичный HTTPS URL только на порту `443`; порт в URL не указывается;
- сертификат от доверенного УЦ или сертификат Минцифры; self-signed и HTTP не поддерживаются с 25 мая 2026 года;
- домен URL должен совпадать с CN/SAN, сервер обязан отдавать полную цепочку;
- ответ HTTP `200` не позднее 30 секунд;
- рекомендуется задать `secret` (5–256 символов из `A-Z`, `a-z`, `0-9`, `_`, `-`) и сверять `X-Max-Bot-Api-Secret`;
- `POST /subscriptions` создаёт/обновляет подписку, `GET /subscriptions` показывает текущие, `DELETE /subscriptions?url=...` удаляет конкретную подписку.

Источник: [POST `/subscriptions`](https://dev.max.ru/docs-api/methods/POST/subscriptions), [GET `/subscriptions`](https://dev.max.ru/docs-api/methods/GET/subscriptions), [DELETE `/subscriptions`](https://dev.max.ru/docs-api/methods/DELETE/subscriptions).

### Long Polling

- допустим только без Webhook-подписки и официально не предназначен для production;
- `GET /updates`: `limit` 1–1000 (по умолчанию 100), `timeout` 0–90 секунд (по умолчанию 30), фильтр `types`;
- нужно сохранять и передавать следующий `marker`; без `marker`/с `null` возвращается только последнее обновление, более ранние можно потерять.

Источник: [GET `/updates`](https://dev.max.ru/docs-api/methods/GET/updates).

## Безопасный порядок диагностики

Не печатать токен, `.env`, заголовки запросов или полный вывод с секретами.

1. Проверить TLS к новому API без токена: `curl -v https://platform-api2.max.ru/me`. Ошибка проверки сертификата указывает на отсутствующий/устаревший trust store; HTTP `401` означает, что TLS и маршрут уже работают.
2. Проверить токен вызовом `GET https://platform-api2.max.ru/me` с заголовком `Authorization`. `200` подтверждает домен, TLS и токен; `401` — проверить актуальность токена в настройках бота, при подозрении на компрометацию обновить его. [GET `/me`](https://dev.max.ru/docs-api/methods/GET/me), [управление токеном](https://dev.max.ru/docs/chatbots/bots-create/manage).
3. Вызвать `GET https://platform-api2.max.ru/subscriptions`. Для текущего Long Polling список должен быть пуст. Если подписка нужна, проверить её URL и `update_types`, прежде всего `message_created` и `message_callback`.
4. Если выбран Long Polling, сделать одиночный `GET /updates?timeout=0&limit=10&types=message_created,message_callback`, затем отправить тестовую команду и повторить запрос с возвращённым `marker`. Не запускать одновременно второй экземпляр polling-процесса.
5. Если выбран Webhook, проверить доступность endpoint снаружи на 443, срок действия, CN/SAN и полную цепочку сертификата; сопоставить access-логи с временем тестовой команды и убедиться в ответе `200` быстрее 30 секунд.
6. Для группового чата проверить, что бот остаётся участником и администратором; для личного диалога — что пользователь не остановил бота (`bot_stopped`).
7. В логах различать `401`, `429`, `503`, ошибки DNS/connect и TLS verification. Для июльской миграции особенно искать обращения к `platform-api.max.ru` и ошибки доверия цепочке `platform-api2.max.ru`.

## Изменения API, не связанные напрямую с приёмом команд

- `GET /chats` не поддерживается с июня 2026 года; MAX предлагает собирать `chat_id` из событий и хранить самостоятельно.
- `POST /chats/{chatId}/members` удалён 30 сентября 2026 года.

Они не объясняют отсутствие всех входящих команд, если бот не вызывает эти методы на пути polling/webhook, но могут создавать отдельные ошибки. Источник: [история изменений API](https://dev.max.ru/docs-api/changelog-api).
