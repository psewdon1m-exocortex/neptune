# Подключение проекта к Neptune Linux

Neptune запускается один раз на Linux-хосте и обслуживает несколько проектов.
Каждый deployment получает собственные control/export/Saturn tokens. Общие
координаты Saturn и репозитория читаются из Kernel Register; секреты в Register
не хранятся.

## Два независимых Linux pipeline

### Recovery archives

Проект использует один backup builder для трёх операций:

- ручное скачивание ZIP;
- внутренний экспорт ZIP для Neptune;
- ручное восстановление любого из этих ZIP.

Loopback endpoint принимает `POST`, проверяет отдельный Bearer export token и
возвращает неизменённые байты `application/zip`. Neptune не распаковывает и не
перепаковывает recovery archive. Каждый успешный запуск создаёт новый файл в
`backups/<project-namespace>/<server-id>/`; старый архив не перезаписывается, а
stored quota не позволяет новому запуску превысить выделенный объём.

### Dedicated mirror

Опциональный mirror имеет отдельный loopback endpoint и отдельный Saturn WebDAV
token. Он выполняется параллельно с archive worker и никогда не подменяет ZIP:

- Volt возвращает raw snapshot `personal.volt`; Neptune зеркалирует только этот
  файл в корень `volt`;
- Mastermind возвращает bounded ZIP с полным Obsidian vault; Neptune безопасно
  распаковывает его во временную директорию и зеркалирует дерево в `mastermind`;
- изменённый файл передаётся целиком, неизменённый пропускается по SHA-256/ETag,
  отсутствующий в источнике удаляется после mass-deletion guard.

Exporter не должен включать symlink/reparse point, traversal path или данные,
не принадлежащие выделенному набору. Полный Volt recovery ZIP по-прежнему
содержит настройки и секреты, а mirror содержит только `personal.volt`.

## Управление из Saturn

Ручные Download/Restore, локальный статус и Initialize/Repair остаются в
Settings. Автоматические ZIP archives, выделенные mirrors, Windows sync clients,
интервалы, явный запуск и fleet update Neptune находятся в верхнеуровневой
вкладке Saturn `Synchronization`.

Каждый daemon сам обращается к Saturn по исходящему HTTPS и получает desired
state с монотонной revision и очередь команд. Поэтому из Saturn можно управлять
Neptune на других серверах без входящего порта, VPN или SSH:

```text
neptuned -> POST /api/v1/neptune/agent/check-in
          Authorization: Bearer <producer-token>
          status + applied revision + completed commands

Saturn   -> desired archive/mirror schedules + pending commands
```

Локальный Unix socket остаётся диагностическим/совместимым интерфейсом, но не
является control plane и не требует совместного размещения Saturn и Neptune.

## Рекомендуемая регистрация deployment

Оператор в Saturn → Synchronization создаёт identity с namespace, server ID и
ролью (`archive`, `volt` или `mastermind`). Полученный setup code действует 15
минут и используется один раз. Если Neptune уже установлен, основной сценарий
— Settings → Backup → **Initialize Neptune** в подключаемом сервисе: UI передаёт
код локальному Updater и дожидается terminal job status. Для установки
отсутствующего агента, repair или аварийной работы используется CLI:

```text
sudo <project>-install backup
```

Готовые команды: `chronos-install backup`, `volt-install backup`,
`kernel-install backup` и `saturn-install backup`. Для Volt один setup code и
одна команда одновременно подключают recovery ZIP и mirror `personal.volt`;
частоты этих процессов после регистрации всё равно задаются независимо.

Installer делегирует Updater проверенную установку единственного host-wide
daemon, обменивает code на producer token, генерирует локальные control/export
tokens, регистрирует deployment и перезапускает только нужный сервис.

Для `volt`/`mastermind` обмен также выдаёт Device token, ограниченный ровно
корнем `volt` или `mastermind`. Новый setup code заменяет прежний mirror token;
отзыв identity отзывает оба полномочия. Producer token не работает через
WebDAV, Device token не может создавать recovery archives.

Разные серверы используют разные identities и tokens. Stable
`client_instance_id`, project ID и run ID входят в idempotency key, поэтому
одновременные клиенты не смешивают повторы. Несколько Windows клиентов также
получают отдельные Device tokens и занимают уникальные `sync/<folder>`.

## Ручная аварийная регистрация

Основной env-файл:

```text
NEPTUNE_BACKUP_EXPORT_URL=http://127.0.0.1:<port>/api/.../internal/neptune/backup
NEPTUNE_CONTROL_TOKEN_FILE=/etc/neptune/clients/<deployment>.control.token
NEPTUNE_EXPORT_TOKEN_FILE=/etc/neptune/clients/<deployment>.export.token
NEPTUNE_SATURN_TOKEN_FILE=/etc/neptune/clients/<deployment>.saturn.token
NEPTUNE_SATURN_SLUG=<producer-slug>
NEPTUNE_BACKUP_ENABLED=false
NEPTUNE_BACKUP_INTERVAL_HOURS=24
```

Для выделенного mirror:

```text
NEPTUNE_MIRROR_EXPORT_URL=http://127.0.0.1:<port>/api/.../internal/neptune/mirror
NEPTUNE_MIRROR_ROOT=volt|mastermind
NEPTUNE_MIRROR_TOKEN_FILE=/etc/neptune/clients/<deployment>.mirror.token
NEPTUNE_MIRROR_MODE=single-file|zip-tree
NEPTUNE_MIRROR_TARGET_FILENAME=personal.volt
NEPTUNE_MIRROR_ENABLED=false
NEPTUNE_MIRROR_INTERVAL_MINUTES=5
```

`NEPTUNE_MIRROR_TARGET_FILENAME` задаётся только для `single-file`. Затем:

```text
sudo neptunectl register-project <deployment-id> <env-file>
```

## Текущие адаптеры

Volt уже реализует raw mirror exporter. Репозитория Mastermind в workspace пока
нет: при его добавлении нужен описанный bounded ZIP-tree endpoint; универсальный
worker, безопасная распаковка и точное WebDAV-зеркалирование уже реализованы в
Neptune. Backup builder каждого проекта остаётся единственным владельцем
формата recovery archive.
