# CHAT-B — Neptune: implementation handoff, 2026-10-14

Статус: **IMPLEMENTED / UNVALIDATED**, кроме C01, где диагностика внедрена, а фактическая причина исторической задержки остаётся **OPEN**. Это описание исходников, а не результат исполнения.

Основание: [инвентаризация C01–C23](../../saturn/docs/implementation/IMPLEMENTATION_CHANGE_INVENTORY_2026-10-14.md), [K1 r2 / K2 r2 / K3 r2 / K4 r1](../../saturn/docs/implementation/PARALLEL_SYNC_CONTRACTS_2026-10-14.md). Neptune принимает K1 r2 с совместимостью r1; K4 напрямую не использует. [Статус CHAT-B](../../saturn/docs/implementation/parallel-2026-10-14/CHAT-B.md), [журнал работы](../../.history/2026-10-14-parallel-neptune.md).

По поручению оператора **не запускались tests, lint, build, typecheck, изменённый продукт, native/installer checks, миграции реальных profiles, службы, containers, deployment или 6h**. Не выполнялись установка пакетов и изменение пользовательского source tree. Существующие originals/receipts не очищались. Рабочее дерево изначально содержало tracked/untracked изменения; исходный HEAD не является baseline этой работы.

## Реализация по требованиям

| C-ID | Реализованный путь Neptune | Основные исходники |
|---|---|---|
| C01 | Fixed phase timings/progress/SQL counts, backlog/USN lag; успешный schema preflight переиспользуется до изменения `schema_version`. Ни timeout, ни hardlink proof не ослаблены. Причина прежних 300 секунд не доказана. | `SyncDiagnostics`, `WindowsLocalCoherenceCoordinator*`, `NeptuneStateStore.LocalSource` |
| C04 | Durable hints с generation CAS, quiet 750 ms/max age 5 s, bounded channel/inbox, durable gap и full proof перед его ACK. Подписки roots имеют отдельные cursors/owners. Volume/mode reader общий в процессе; named mutex и восьмистраничный cache согласуют чтения разных profile processes того же пользователя/session. Cached pages повторно проверяются по journal bounds и SHA. | `WindowsLocalChangeWatcher`, `NeptuneStateStore.EventInbox`, `WindowsVolumeJournalSubscription`, `WindowsVolumeJournalCache` |
| C05 | Negotiated long-poll с cancellation, отдельными wait slots и повторным чтением durable feed. Expired cursor/gap идёт в существующее staged refresh. Polling fallback сохраняется. | `SaturnGuardedCycleTransport.Extensions`, `WindowsGuardedMappingSession`, engine |
| C06 | Durable directory IDs из текущего R/B; per-directory singleflight, bounded positive/negative cache. Ordinary ensures собираются в 500-item/4 MiB batch. Родители разрешаются durable topological waves до claim детей; Neptune не посылает неразрешённый `parentRef`. Individual fallback сохраняет прежние IDs/input. | `SaturnGuardedCycleTransport.Folders`, `NeptuneStateStore.LocalCycleWork`, engine |
| C09 | Stable-ID rename/move planner, native incarnation/full membership proof, expected versions и original recovery. Derived descendant paths меняются без повторной передачи чистых bytes. Неоднозначные halves/hardlinks/type changes сохраняют конфликт. | `NeptuneStateStore.NamespaceOperations`, `ParentReconcileInstall`, `WindowsFreshSyncEngine.Namespace` |
| C10 | Baseline-managed proven absence, full descendant coverage и postorder quarantine. UI показывает точный page/digest. K1 r2 server approval и receipt сохраняются до effect. ACK относится к original g; g+1 остаётся dirty. Later remote state сохраняет historical publication и видимый конфликт. | `NeptuneStateStore.FullParentProof`, `DeletePlan`, `NamespaceOperations`, `MainWindow.MappingReview` |
| C11 | Explicit excludes без скрытых `.git`/`node_modules`/`.audit`; version/hash, persisted preview и continuation по 250 записей. Exclude сохраняет серверную копию; re-include инвалидирует нужные строки. Pending originals блокируют несовместимый переход. | `MappingSyncPolicy`, `NeptuneStateStore.MappingPolicy`, Review UI |
| C12 | `legacy`/`portable_v1`, NFC validation без автонормализации, ASCII-fold key, collision preview. Existing binding меняется только через reviewed server adoption artifact и отдельный новый client scope. Старые configurations/originals не перепривязываются. | `Sync/Protocol/SyncExtensions`, `NeptuneStateStore.BindingAdoption`, profile CLI |
| C13 | Per-mapping ordinary opt-in; отключение не мешает recovery принятых originals. Missing-only aggregate получает новый batch ID, сохраняя individual IDs; accepted partial uploads продолжаются отдельно. Monotonic per-item outcomes при нескольких aggregates. | `WindowsFreshSyncEngine.SmallBatches`, `WindowsMappingAnchorRemote`, preferences/Review |
| C14 | Create-empty и metadata-patch через durable namespace originals и metadata batch. Metadata-only требует непрерывного USN proof, incarnation/native handles, прежнего content hash и текущего remote version; иначе остаётся обычный capture. | `WindowsNativeCycleCapture.Metadata`, `NeptuneStateStore.MetadataOnly`, engine Metadata/ContentMutation |
| C15 | Early 4 MiB chunks, bounded ring spool, immutable source witness/generation, per-chunk ACK и whole SHA seal. Restart перечитывает только доказанно ту же source version для whole hash, не отправляя ACKed chunks повторно. Sealed old uploads остаются на прежнем protocol. | `WindowsNativeCycleCapture.Stream`, `WindowsFreshSyncEngine.Stream`, `NeptuneStateStore.Stream`, stream transport/contracts |
| C16 | Durable operation deadlines/states, status-first ambiguity recovery, jitter/Retry-After, rare checks после восьми неудач. Shared process-wide reader=1/CPU=2/data<=4/large=2, byte queues live/recovery/bootstrap с aging/borrowing, published-throughput/latency cooldown. Retained/disabled mapping spools входят в общий 1 GiB/64-file admission; более строгие mapping bounds сохраняются. | `NeptuneStateStore.OperationSchedule`, `SyncRetryController`, `SyncResourceScheduler`, `WindowsCycleSharedBudget`, engine Scheduling |
| C17 | Negotiated JSON gzip свыше 16 KiB с 4 MiB metadata bounds и общим CPU gate. File chunks не сжимаются; canonical originals не зависят от wire encoding. | `SaturnGuardedCycleTransport`, extension negotiation |
| C18 | Offline inspect/pause/checkpoint/backup/migrate-plan/adopt/resume/diagnostics; binding-plan/adopt и extended historical recovery. WAL-aware SQLite Backup API, SHA manifest, encrypted enrollment/settings/spools, owner locks, durable progress. Exact schema/minimum-writer fence не позволяет старому binary игнорировать новые originals. | `WindowsProfileCommands`, `NeptuneStateStore.Compatibility`, `ClientEvolutionMigration`, `BindingAdoption`, `ExtendedRecovery` |
| C19 | Signed bundle variant/compatibility, отдельная OS trust проверка MSI, verified previous MSI. Portable staged directory swap и persistent recovery journal; installed mode использует installer, rollback через прежний verified package. Pause/drain/checkpoint/backup, проверка других profile processes, mandatory read-only health перед writer. | `WindowsUpdateService`, `WindowsUpdateTransaction`, `WindowsInstallerTrust`, `UpdateApplyMode`, `windows_release.py` |
| C20 | Live per-phase diagnostics, journal lag, SQL/progress, retries, oldest scheduled pending, shared resource wait и mapping state. Captured/received/published разделены. Backend written/verified явно unknown, если wire не сообщает их. Export не содержит credentials/content/source paths в metric labels. | `SyncDiagnostics`, `WindowsSyncService*`, `MainWindow.MappingReview` |
| C21 | Authorized lookup/clone только по уже известному sealed hash; immutable content reference и target CAS. Неподдержка/известный безопасный отказ допускают прежний upload; неоднозначный clone восстанавливается по original ID. | `WindowsFreshSyncEngine.ContentMutation`, namespace transport/state |

## Контракты и lifecycle

Клиент регистрирует `saturn.delete-plan.v1` через K1 r2 и сохраняет `saturn.delete-plan-approved.v1` до отправки quarantine mutations. Потеря ACK повторяет тот же plan ID/digest/items. r1 сервер не получает bulk-delete без server approval. Metadata batch возвращает независимые outcomes; успех контейнера не объявляет успех каждого original.

Для C12 используется предоставленный CHAT-A `saturn.sync-adoption.v1`: старые root/server epoch/device/backend должны совпасть, binding должен измениться. `binding-adopt` копирует L/B в отдельный scope по 250 строк с continuation; исходный scope сохраняется. Обычный запуск не обходит `COPYING`. После него проверяются HTTPS binding, namespace policy и qualification receipt, затем complete fresh feed постранично квалифицирует baseline. Автоматического запуска server/client или переименования имён нет. PostgreSQL legacy `lower(name)` может дополнительно отказать некоторым Unicode-парам: серверный collision/adoption report остаётся обязательным.

Client evolution format v1 устанавливается additive с отдельным SQLite backup и exact writer fence. Ни одна его редакция в этой работе не применялась к реальному профилю. Unsupported/partial schema отвергается до записи. Новые namespace/stream originals и binding migration history учитываются в retirement; automated purge этой истории не добавлен.

Обычный operation retry сохраняет `RETRY_WAIT`, `SOURCE_BUSY`, `WAIT_CONNECTIVITY` или `DEGRADED`. После восьми неудач следующая проверка откладывается на 5–6 минут, с учётом большего Retry-After. `AUTH_REQUIRED` останавливается до явного offline `resume` после восстановления доступа. Срок хранится в SQLite, поэтому restart его не сбрасывает. Native handles stream удерживаются до seal, но общий reader освобождается между чтением chunk и сетевой отправкой.

Временное ожидание при сохранённом anchor не уничтожает Windows mapping session: отдельный deadline wake-up продолжает работу. Неисправный local proof/source gap не маскируется этим состоянием. Общие scheduler/CPU/spool лимиты действуют внутри одного client process и его mappings; USN cache отдельно согласован между процессами профилей. Измеренная межпроцессная resource/fairness qualification остаётся открытой.

## Offline команды профиля

Это примеры будущего запуска подходящего собранного binary; **в этой работе команды не выполнялись**. Требуется закрытая GUI-сессия указанного профиля: CLI получает тот же per-profile mutex. Каждый вызов требует `--output`; результат либо typed JSON, либо `status: failed`, процесс возвращает ненулевой exit code при отказе.

```powershell
Neptune.Windows.exe --profile work --profile-command inspect --output C:\review\inspect.json
Neptune.Windows.exe --profile work --profile-command pause --output C:\review\pause.json
Neptune.Windows.exe --profile work --profile-command checkpoint --output C:\review\checkpoint.json
Neptune.Windows.exe --profile work --profile-command backup --destination C:\review\profile-backup --output C:\review\backup-result.json
Neptune.Windows.exe --profile work --profile-command migrate-plan --output C:\review\migration-plan.json
Neptune.Windows.exe --profile work --profile-command adopt --plan C:\review\migration-plan.json --destination C:\review\adoption-backup --output C:\review\adoption.json
Neptune.Windows.exe --profile work --profile-command diagnostics --output C:\review\diagnostics.json
Neptune.Windows.exe --profile work --profile-command resume --output C:\review\resume.json
```

`--destination` должен быть новым каталогом вне активного профиля. `adopt` требует paused profile, совпадения inventory/evidence digest и отсутствия UNKNOWN. Повторный вызов с прежними plan/destination/output продолжает сохранённый `.journal`; backup сверяется по SHA до продолжения. Это profile schema adoption, не автоматическая серверная activation.

Смена server binding/codec требует отдельного server-reviewed artifact. `--database` — относительный путь существующего `cycle.db` из inspect, например `guarded-sync\<namespace>\<mapping>\cycle.db`:

```powershell
Neptune.Windows.exe --profile work --profile-command binding-plan --database "guarded-sync\<namespace>\<mapping>\cycle.db" --server-adoption C:\review\saturn-adoption.json --output C:\review\binding-plan.json
Neptune.Windows.exe --profile work --profile-command binding-adopt --plan C:\review\binding-plan.json --destination C:\review\binding-backup --output C:\review\binding-result.json
Neptune.Windows.exe --profile work --profile-command recover-extensions --database "guarded-sync\<namespace>\<mapping>\cycle.db" --output C:\review\recovery.json
```

`recover-extensions` требует paused profile, прежних enrollment credentials, того же native root и server binding; optional `--scope` выбирает retained scope. Команда отменяет только заведомо неотправленные namespace originals, читает statuses остальных и может завершить уже sealed stream с прежними IDs. Она сохраняет historical outcome без current B/dirty ACK. Stream без final native seal, UNKNOWN или несовместимая source version остаётся deferred; это не разрешение recapture с новой identity. До разрешения такого original journal replacement/binding adoption блокируется.

## Открытые gates и сохранность дерева

- **C01:** динамическая диагностика на сохранённом профиле для установления причины 300-second timeout. Schema caching устраняет видимую повторную работу, но не доказывает историческую причину или ускорение.
- Contract/state/crash/resume/migration и composed Neptune/Saturn interoperability на одной согласованной версии; никакого runtime PASS сейчас нет.
- Windows native identity, gap/full membership/hardlinks, multi-profile USN, cancellation, shared resource/fairness, source mutation и UNKNOWN recovery.
- Signed portable/MSI native install/update/rollback, OS trust, interruption recovery, health на существующих profiles и binary compatibility. Profile backup не является разрешением откатить уже опубликованные server effects.
- Throughput/p95/CPU/RAM/temp budgets и benefit gates. Automatic small-file batching остаётся OFF; opt-in — явное решение оператора. SFTP window tuning и новые hardware limits не менялись.

Baseline и fingerprints находятся в `.audit/2026-10-14/chat-b-neptune/`. `current-changes.json` сравнивает всё дерево с исходным snapshot и включает параллельные изменения, поэтому не является авторским списком CHAT-B. `LocalUploadSnapshot`, `WindowsLocalSnapshot`, `WebDavSyncClient` и новые concurrency/snapshot test sources изменялись другим владельцем и сохранены. В test project этой работой добавлены только четыре `Compile` links для новых Windows production dependencies; существующие test cases не менялись и не запускались.

Продуктовые изменения CHAT-B ограничены Neptune; общие контракты, Saturn/Pluto/host code не редактировались. Внешний файл изменён только собственный status CHAT-B. Коммиты, PR и публикация не выполнялись.
