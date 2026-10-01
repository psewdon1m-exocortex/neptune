# Подключение проекта к Neptune Linux

This document specializes [Part 09 — service agents deployment and lifecycle](https://github.com/psewdon1m-exocortex/general/blob/main/PART_09_SERVICE_AGENTS_DEPLOYMENT_AND_LIFECYCLE.md); that central contract remains authoritative.

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

## Разделение управления

В Settings подключённого сервиса находятся ручные Download/Restore, локальный
статус, Initialize/Repair, **Unlink Neptune agent** и управление автоматическим
расписанием. Для Kernel, Chronos, Saturn и Laboratory это одна политика ZIP
archive. Для Volt и Mastermind один переключатель и часовой интервал атомарно
обновляют политики archive и mirror через `schedule-all`; сами workers, их
состояния, повторы и полномочия остаются раздельными. Новый профиль выключен
и имеет интервал 24 часа. Ручного запуска удалённого backup из GUI нет.

Saturn `Synchronization` управляет identities, setup codes, quotas, Windows
sync clients и наблюдением за состоянием и результатами. Редактор расписаний
сервисов и fleet update Neptune там отсутствуют. Проверка и установка релиза
общего Neptune выполняются на каждом хосте через `sudo updater tui`.

Каждый daemon сам обращается к Saturn по исходящему HTTPS и получает desired
state с монотонной revision и допустимые команды. Из Saturn можно наблюдать за
Neptune на других серверах и передавать подтверждённую политику без входящего
порта, VPN или SSH:

```text
neptuned -> POST /api/v1/neptune/agent/check-in
          Authorization: Bearer <producer-token>
          status + applied revision + completed commands

Saturn   -> desired archive/mirror schedules + pending commands
```

Локальный Unix socket обслуживает scoped-операции Updater и диагностику.
Совместное размещение Saturn и Neptune не требуется.

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

Готовые команды включают `chronos-install backup`, `volt-install backup`,
`kernel-install backup` и `saturn-install backup`. Для Volt и Mastermind один
setup code подключает recovery ZIP и выделенное зеркало. Их расписания
согласованно задаются одним контролом в Settings владельца.

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
NEPTUNE_MIRROR_INTERVAL_MINUTES=1440
```

`NEPTUNE_MIRROR_TARGET_FILENAME` задаётся только для `single-file`. Эти env
значения — аварийный bootstrap профиля; после подключения расписанием управляет
только версия политики из Settings владельца, сохранённая в Saturn. Затем:

```text
sudo neptunectl register-project <deployment-id> <env-file>
```

## Отключение и повторная привязка

**Unlink Neptune agent** запускает через локальный Updater долговечное задание
только для данного проекта. Neptune приостанавливает его политики и дожидается
безопасной границы активных передач; Saturn отзывает его producer и mirror
credentials и незадействованные setup codes, сохраняя уже загруженные архивы.
При неудаче удалённого отзыва проект остаётся в `unlinking` и допускает повтор.
После подтверждения Neptune удаляет только этот профиль, а Updater удаляет его
локальные credentials. Другие проекты и общий daemon продолжают работу. Для
нового подключения нужен новый setup code.

## Текущие экспортёры

Volt реализует raw mirror exporter `personal.volt`, а Mastermind — bounded
ZIP-tree exporter своего Vault. Универсальный worker Neptune безопасно
распаковывает дерево и зеркалирует его в выделенный root. Backup builder
каждого проекта остаётся владельцем формата recovery archive.
