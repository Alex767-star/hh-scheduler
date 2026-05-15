# hh-scheduler — Планировщик сбора и анализа вакансий

Внутрипроцессный планировщик на tokio. Собирает вакансии с hh.ru по расписанию, сохраняет в БД, считает ключевые навыки.

## Архитектура
src/
├── main.rs        # Инициализация, scheduler, graceful shutdown, pipeline
├── storage.rs     # Trait Storage + SqliteStorage
├── api.rs         # HH API клиент, пагинация, retry, тестовые данные
├── analyzer.rs    # Подсчёт 45+ ключевых слов
└── monitoring.rs  # Prometheus метрики, health check


## Стек

- tokio — асинхронный рантайм, signal handling
- tokio-cron-scheduler — cron-задачи внутри процесса
- reqwest 0.13 — HTTP клиент с rustls
- sqlx 0.8 — SQLite, compile-time проверка запросов
- tracing + tracing-subscriber — структурированное логирование
- prometheus — метрики для Grafana
- tiny_http — health/metrics HTTP сервер (порт 3000)
- chrono — временные метки, RFC3339
- serde_json — парсинг HH API
- clap — CLI аргументы

## Как работает

### Старт
1. Инициализирует SQLite, создаёт таблицы vacancies, skill_stats, pipeline_state
2. Проверяет время последнего запуска из pipeline_state
3. Если пропущены тики — выводит предупреждение с количеством
4. Запускает полный цикл пайплайна
5. Добавляет cron-задачу, запускает планировщик
6. Поднимает HTTP сервер на порту 3000 (/health, /metrics)
7. Ждёт Ctrl+C

### Пайплайн (каждый тик)
1. Запрашивает вакансии через HH API с пагинацией по всем страницам
2. Сохраняет в БД с UPSERT, возвращает (saved, updated) раздельно
3. Достаёт описания за 24 часа
4. Подсчитывает 45+ ключевых слов: Rust, Tokio, Actix, Axum, Docker, Kubernetes, PostgreSQL, MySQL, MongoDB, Redis, gRPC, GraphQL, REST, WebSocket, AWS, GCP, Azure, CI/CD, GitLab, GitHub Actions, Python, Go, Java, Kotlin, TypeScript, JavaScript, React, Vue, Angular, Svelte, Kafka, RabbitMQ, NATS, Linux, Bash, Terraform, Ansible, Microservices, DDD, TDD, Agile, Scrum
5. Сохраняет агрегацию в skill_stats
6. Удаляет вакансии старше N дней (по умолчанию 90)
7. Фиксирует время завершения тика в pipeline_state
8. Обновляет Prometheus метрики

### Завершение
SIGINT → ожидание завершения текущего тика (с таймаутом) → scheduler.shutdown() → закрытие БД → выход 0

## Идемпотентность

- vacancies: INSERT ON CONFLICT(id) DO UPDATE — обновляет поля существующих
- skill_stats: INSERT ON CONFLICT(date, keyword) DO UPDATE count
- pipeline_state: INSERT ON CONFLICT(id) DO UPDATE last_run_at
- Повторный запуск задачи не создаёт дубликатов
- retry с backoff при ошибках API (4 попытки, базовая задержка 1с)
- Обработка 429 с парсингом Retry-After
- Rate limit: 100ms между запросами к HH API

## CLI
--query "Rust+developer"    # Поисковый запрос (по умолчанию "Rust+developer")
--area "113"                # Регион HH (по умолчанию "113" — Россия)
--cron "0 0 1/6 * * *"      # Cron-выражение (по умолчанию каждые 6 часов)
--retention-days 90         # Хранение вакансий в днях (по умолчанию 90)
--pipeline-timeout-secs 300 # Таймаут при shutdown (по умолчанию 300с)


## Метрики Prometheus (GET /metrics)

- pipeline_runs_total — всего запусков пайплайна
- pipeline_errors_total — всего ошибок
- vacancies_saved_total — новых вакансий
- vacancies_updated_total — обновлённых вакансий
- keywords_found — найдено ключевых слов
- pipeline_duration_seconds — гистограмма времени выполнения
- zero_updates_streak — тиков подряд без обновлений
- missed_ticks — пропущенных тиков с последнего запуска

## Health Check (GET /health)
{"status": "ok", "service": "hh-scheduler"}


## Логирование

RUST_LOG через переменную окружения:
- error — только ошибки
- info — старт/стоп пайплайна, метрики тика, пропущенные тики
- debug — содержимое ответов API, топ-10 навыков

## Тестовые данные

Если HH API возвращает пустой массив — подставляются 5 тестовых вакансий с полным набором ключевых слов для проверки пайплайна.

## Структура БД

- vacancies — сырые вакансии (id TEXT PK, UPSERT, индекс по published_at)
- skill_stats — агрегация по дням (date + keyword UNIQUE, индекс по date)
- pipeline_state — время последнего тика (id=1)

## Тесты

Интеграционные тесты на in-memory SQLite:
- UPSERT: новые/обновлённые вакансии
- Идемпотентность skill_stats
- Pipeline state: запись/чтение
- get_recent_descriptions: возвращает свежие данные
