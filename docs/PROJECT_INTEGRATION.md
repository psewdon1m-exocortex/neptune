# Подключение проекта к Neptune Linux

Neptune запускается один раз на Linux-хосте и обслуживает несколько проектов.
Каждый deployment проекта регистрируется отдельно и получает собственные
control/export/Saturn producer tokens. Секреты не хранятся в Kernel Register.

## Контракт проекта

Проект должен использовать один общий backup builder для трёх операций:

- ручное скачивание ZIP;
- внутренний экспорт ZIP для Neptune;
- ручное восстановление полученного ZIP.

Внутренний loopback endpoint принимает `POST`, проверяет отдельный Bearer export
token и возвращает неизменённые байты `application/zip`. Neptune не распаковывает
и не перепаковывает архив.

Backend проекта предоставляет авторизованному оператору facade:

```text
GET  /api/.../neptune/status
PUT  /api/.../neptune/schedule  { enabled, interval_hours }
POST /api/.../neptune/runs
POST /api/.../neptune/update/check
POST /api/.../neptune/update/install
```

Settings сохраняет существующие Download/Restore и рядом показывает состояние,
интервал в целых часах, переключатель, запуск в Saturn и версию Neptune.

## Регистрация deployment

Создайте три случайных секрета с правами чтения только для runtime-аккаунтов:

```text
/etc/neptune/secrets/<deployment>-control.token
/etc/neptune/secrets/<deployment>-export.token
/etc/neptune/secrets/<deployment>-saturn.token
```

Затем подготовьте root-only env-файл:

```text
NEPTUNE_BACKUP_EXPORT_URL=http://127.0.0.1:<port>/api/.../internal/neptune/backup
NEPTUNE_CONTROL_TOKEN_FILE=/etc/neptune/secrets/<deployment>-control.token
NEPTUNE_EXPORT_TOKEN_FILE=/etc/neptune/secrets/<deployment>-export.token
NEPTUNE_SATURN_TOKEN_FILE=/etc/neptune/secrets/<deployment>-saturn.token
NEPTUNE_SATURN_SLUG_REGISTER_KEY=services.<project>.backup.saturn_slug
NEPTUNE_BACKUP_ENABLED=false
NEPTUNE_BACKUP_INTERVAL_HOURS=24
```

Зарегистрируйте deployment без запуска второго daemon:

```text
neptuned register-project <deployment-id> <env-file>
```

`deployment-id` уникален внутри хоста. Для одного проекта на нескольких серверах
используйте разные producer identities/tokens и при необходимости разные Register
keys. Каждый daemon сохраняет собственный `client_instance_id`; вместе с project
и run ID он входит в idempotency key, поэтому параллельные клиенты не смешивают
повторы. Saturn namespace определяется отдельным producer slug.

## Container runtime

Контейнеру проекта нужны bind mounts Unix sockets Neptune и Updater, control и
export token files, а также supplemental GID обоих sockets. Внутренний exporter
публикуется только на loopback хоста. Browser никогда не получает локальные или
Saturn tokens.

Адрес Saturn, пути `backups`/`sync`/`sync_preferences`, producer slug и URL репозитория Neptune агент
получает из проверенного Kernel Register. Updater повторно читает Register,
проверяет manifest/SHA-256, атомарно заменяет binary и откатывает его при неуспешном
health check.

## Готовые адаптеры

В workspace подключены Kernel, Chronos, Laboratory, Perimetr, Saturn и Volt. Для
нового проекта копируется только facade/auth wiring; его backup builder остаётся
единственным владельцем формата архива.
