# CHAT-B — результаты целевой приёмки Neptune

Дата среза: 2026-10-14, 20:38 +03:00. План: [TARGETED_TEST_PLAN_CHAT_A_B_C_2026-10-14.md](C:/.projects/exocortex/saturn/docs/implementation/TARGETED_TEST_PLAN_CHAT_A_B_C_2026-10-14.md).

**83 уникальных test cases: 83 PASS, 0 FAIL, 0 SKIP в итоговой матрице адресных повторов. Полная приёмка группы B не закрыта.** Исторический C01, фактическая смена NTFS journal bounds и совместная серверная проверка остаются открытыми; B13/B14 заблокированы отсутствием стенда, что подтвердил оператор. Успех fixture не объявляется успехом реального Saturn или установленного приложения.

Сборки `Neptune.Core.Tests.csproj` и `Neptune.Windows.csproj` прошли без предупреждений и ошибок. Использован SDK из `global.json`, `C:/Users/pc/.dotnet/dotnet.exe`, точные фильтры и `--no-build --no-restore`. Общий suite, endurance, deployment, updater/MSI и рабочие profiles не запускались.

Журнал: [2026-10-14-targeted-tests-chat-b.md](C:/.projects/exocortex/.history/2026-10-14-targeted-tests-chat-b.md). Evidence root: `C:/.projects/exocortex/.audit/2026-10-14/targeted-tests/chat-b/b-193115-8e7b4ea7/` (далее `RUN`).

## Итоговые запуски

Каждый каталог содержит `run.json` с точным фильтром, временем и SHA сборок, `test.log`, TRX и собственные fixture/evidence directories. [Машинная матрица](C:/.projects/exocortex/.audit/2026-10-14/targeted-tests/chat-b/b-193115-8e7b4ea7/final-lanes.json) выбирает последний результат каждого полного test name: 94 исполнения, 83 уникальных случая, 11 повторов. Это адресные результаты затронутых проверок, а не один запуск всего suite: после каждого исправления пересобирался тестовый проект и повторялись affected cases.

| Lane в RUN | Selected / PASS | Состав |
|---|---:|---|
| final-state-isolated | 15 / 15 | B01, B02, B06 paging/pending, B12, durable B11, два exact known-red |
| final-native | 19 / 19 | B03, B04 rename/CAS/gap/directory, B05 approval/negative proof/ACL |
| final-transport-policy | 12 / 12 | Все B07 и B15, включая local wake и policy cache |
| final-content | 24 / 24 | B09 и B10; accepted stream backup assertions |
| final-limits | 3 / 3 | Process limits, weighted scheduling, retained spools |
| final-batch-prepare / data / partial / six | 4 / 4 | Отдельные процессы, accepted batch backup assertions |
| final-review | 2 / 2 | Stale delete digest, native decomposed name preview |
| final-types-receipt | 3 / 3 | Оба type replacements, новый hint до старого delete ACK |
| final-process-crash | 5 / 5 | B02 abrupt process death и повтор четырёх B02/B03 assertions |
| final-old-stream / final-old-batch | 7 / 7 | Повтор шести stream variants и batch-six, включая настоящий predecessor Core |
| **Всего исполнений / уникальных cases** | **94 / 83** | **83 уникальных PASS; 0 failed / 0 skipped** |

Сборки: `RUN/build-crash/old-writer-build.log` (последняя test assembly), `RUN/build-delivery/windows.log`. `RUN/delivery-diff-check.log`: `git diff --check`, exit 0. Последнее дополнение меняло только тесты/helper; затронутые stream/batch/B02/B03 cases повторены, production source SHA неизменны.

## Матрица B00–B15 и пределы доказательства

| ID | Выполненные подварианты и результат | Оставшийся gate / статус |
|---|---|---|
| B00 | Compile link Startup; duplicate alias; Windows guards вместо NoWarn. Обе реальные сборки PASS. | **PASS** |
| B01 | Четыре страницы: 256 data, 41 relevant, 39+200+2, 256/257; новая state instance после каждого commit; ≤256 decoded/≤40 relevant; exact record_after и все retained rows сохранены. Копия historical DB/WAL/SHM: original hashes и protected tables неизменны, trace записан. | **PARTIAL, C01 OPEN**: historical record_after уже равен максимальному retained USN; первый seed потребляет 0. Этот срез не воспроизводит старые 300 секунд и не даёт before/fix comparison. Native scan/import исторического профиля не выполнялись. |
| B02 | 100 hints; 750 ms quiet; 5 s max age; g+1 до ACK; 4096/4097; новый gap во время реального full parent proof; следующий proof закрывает только свою generation. Отдельный native helper после committed enqueue/gap завершён exit23 без finally, до callback; другой процесс восстановил hint/gap, прежний cursor и ownership. PASS. | **PASS**, включая abrupt process death и event-before-subscribe B07. |
| B03 | Реальные NTFS journal pages, два процесса с разными roots и durable cursors; reuse, 8/9 eviction, SHA corruption, смерть владельца mutex; неправильный journal ID отвергнут; независимый cursor не ACKed. Hardlink blockers проверены B04/B05. | **PARTIAL native gate**: реальные reset/trim journal bounds общего host volume не исполнялись. Wrong-ID fault не выдаётся за фактическую смену NTFS journal. |
| B04 | Same-parent, directory с clean descendants, case-only, lost commit reply, destination CAS, g+1, hardlink, rename + injected durable gap, file→directory и directory→file; directory mtime revalidation. IDs/foreign current сохранены, clean descendants не перечитываются. PASS. | **PASS перечисленных client scenarios**. Gap внесён через durable fault point после native rename; физический обрыв половины NTFS rename журналом не имитировался изменением общего volume. |
| B05 | Три уровня; exact preview/digest; lost approval reply и reopen; durable server receipt перед effects; postorder; ACL denied, incomplete membership, root/namespace change during scan, exclude, remote edit, atomic replacement; второй delete меняет digest; новый source hint переживает старый delete receipt. PASS. | **PASS client fixture**. Server authorization/executor — A06/A15, не подменяется FakeRemote. |
| B06 | `.git`, `node_modules`, `.audit` включены; 257 rows, continuation 250+7, reopen, reinclude +6 generations; pending original блокирует apply; native decomposed name даёт collision/refusal, source/original не переименованы. Все 28 shared codec vectors в B15. PASS. | **PASS client fixture**. Реальный межъязыковой adoption — A13/A15. |
| B07 | 501 directory/3 levels, ≤500/4 MiB, parent-before-child; 20 concurrent same-original; immutable input; capability fallback; validated feed/policy invalidates failures и сохраняет original success; native local wake отменяет/join poll; durable event-before-subscribe/restart; expired cursor/journal gap. PASS. | **PASS client fixture**; live API long-poll/cursor acceptance — A05/A15. |
| B08 | Потери prepare/data ACK и partial; reopen при выключенной новой batch admission; шесть originals: 2 published, 2 missing, 1 partial, 1 rejected. Initial admitted aggregates 4+2, новый subset только 2 missing, partial offset32, 5 published+1 NOT_PUBLISHED; originals прежние. PASS. | **PASS client fixture**, automatic enable остаётся OFF до A15 benefit. |
| B09 | Empty без fake upload; proven mtime без нового contentVersion; changed bytes с прежним mtime — capture; clone independent target; no-match/capability off/expected mismatch fallback; ambiguous reply восстанавливает original. PASS. | **PASS client fixture**, live authorization/CAS — A06/A11/A15. |
| B10 | Native 12 MiB+17, первый 4 MiB chunk до whole seal; deny-writer custody; ACK0/1 lost response/restart, отправка только 2/3; whole SHA recovery read учтён; modify/delete/replace, bad seal, quota и пять bad wire ACK/seal variants. PASS. | **PASS client/native fixture**. Реальный серверный streaming — A09/A15. |
| B11 | Exact known-red прошли после контролируемого времени: до due нет нового prepare, после due обязателен progress того же original; late ACK не очищает новые bytes. Restart/degraded8; shared reader1/CPU2/data4/large2; 1 GiB/64 retained spools; weighted 4:2:1 order, aging30s, cooldown и published metrics. PASS. | **PASS targeted assertions**; AUTH_REQUIRED/proxy и полный network retry matrix — отдельный network владелец. Не заявляется throughput benchmark или длительная fairness simulation. |
| B12 | inspect/pause/checkpoint/backup/migrate-plan/adopt; ownership lock; WAL backup; credentials/settings; отдельная восстановленная копия. Accepted batch/stream originals, rows/receipts/ACK maps и spool SHA совпали. Binding251, committed-page fault после250, COPYING fence, new Store continuation, wrong SHA/backend/receipt, fresh-feed conflict, old scope preserved. Настоящий predecessor Core в отдельном процессе отказал с Cycle schema incompatible/unknown objects; DB и committed WAL byte-identical. PASS. | **PASS client lifecycle / old-Core refusal**. Full application binary swap/rollback относится к заблокированным B13/B14. Binding-page interruption использует fault callback после реального commit, затем новый Store. |
| B13 | Native updater, crash stages, trust/health marker, concurrent updater и rollback не запускались. | **BLOCKED**: оператор подтвердил отсутствие disposable Windows VM/user. Unit mock не заменяет gate. |
| B14 | MSI apply/rollback, HKCU Run, новый logon, long recovery command и second-profile bypass не запускались. | **BLOCKED**: нет disposable Windows VM/user и verified previous MSI fixture. Рабочий профиль/registry не менялись. |
| B15 | C# исполнил 28 shared vectors + Unicode/255/256 boundaries; metadata16384/16385/off; canonical gzip bytes; >4 MiB rejection; binary unchanged; captured/received≠published; unknown backend counters null; canary не попал в export. Caller HTTP fixture request/response example сохранён без auth headers. PASS. | **PARTIAL integration**: TS/Go совпадение, server malformed/truncated/expanded gzip refusal и реальные обе client→API wire receipts должны сойтись в A05/A15. Mock HTTP handler не считается live Saturn. |

PASS в таблице означает только прямо перечисленное доказательство. Открытые подварианты не превращены в SKIP или зелёный aggregate gate.

## Исправленные дефекты

Каждое исправление следовало за FAIL; raw red/green logs сохранены. Изменения защитных checks ради получения PASS не применялись.

| Дефект | Исправление / исходники | Доказательство |
|---|---|---|
| B00 не собирался: missing Startup partial, duplicate MessageBox, CA1416 | `Neptune.Core.Tests.csproj`, `MainWindow.MappingReview.cs`, Windows guards в `WindowsProfileCommands.cs` / `WindowsUpdateTransaction.Startup.cs` | B00-before → B00-after; финальные builds |
| Tests делали retry до durable not_before; g+1 ещё не прошёл quiet | Instance `TimeProvider` в state/scheduler, тот же clock в watcher/claims/deadlines; default System; tests проверяют раннее ожидание и обязательный progress после due | B11-red 2 FAIL → state-2, final-state-isolated |
| Namespace planning обращался к ещё не установленному parent module | `NeptuneStateStore.NamespaceOperations.cs`: guard наличия модуля | engine-5 → namespace repeats |
| Move directory оставлял старые descendant paths | `NeptuneStateStore.ParentReconcileScan.cs`: повторное bounded перечисление moved known subtree | namespace-5 → namespace-6 / final-native |
| Новый native ID по старому path попадал в delete candidates | `NamespaceOperations.cs`: отказ при занятом retained path в planning и перед effect | namespace-5 → B05 replacement и final-native |
| Full proof не учитывал root parent; следующий proof упирался в committed pending version | `NeptuneStateStore.FullParentProof.cs`: atomic parents count и promotion завершённых versions | native-proof-2/gap-cache-4 → gap-5 / final-state-isolated |
| Directory metadata hints не сеяли affected parent; own baseline принимался только по replacement lineage | `ParentReconcile.cs` включает DirectoryPolicy в прежний bounded budget; `LocalCycleLifecycle.cs` разрешает exact retained owned baseline с native/current-L/R checks | directory-clone-1 / directory-hint-2 → final-native |
| Offline binding adoption считал WAL reserve по консервативным 65536 bytes до измерения | `NeptuneStateStore.BindingAdoption.cs`: force page-size/WAL inspection перед reserve; internal page-commit callback для fault test | adoption-2 → adoption-3 / final-state-isolated |
| Negative directory cache переживал подтверждённый feed и policy change | `SaturnGuardedCycleTransport.Folders.cs`, `.Extensions.cs`, `.cs`: clear failures, invalidation generation исключает возврат старого in-flight failure; успешные original receipts сохраняются | negative-cache-1 / policy-cache-red → final-transport-policy |
| Local event не отменял remote long-poll; событие до subscription могло ждать poll | `WindowsFreshSyncEngine.cs`: subscribe перед durable inbox read, linked cancel/join; cursor/hints не ACKed | local-wake-red → final-wake-transport / final-transport-policy |

Отдельно исправлены тестовые fixtures: initial baseline/quiet barrier, page-size initialization при direct-state API, bounded batch4+2, корректный ACL restore, управляемый retry clock, cleanup только собственных retained spools. Нет permissive `PendingOrUncertain` вместо обязательного progress.

`final-state` первоначально дал 13 PASS/2 FAIL: параллельный hardlink fault другого lane на том же volume вызвал предусмотренный `HardlinkMembershipUnresolved`. Ошибка изоляции исправлена порядком запусков, protection сохранён; `final-state-isolated` — 15/15. Native fault lane больше не совмещался с native baseline lanes.

## Исходники, изоляция и handoff

- HEAD Neptune до/после: `9eebcfebdc77e7b05605194092928457b8d81579`. Dirty tree нескольких владельцев сохранён; commit/reset/stash/checkout не выполнялись.
- [delivery-inventory.json](C:/.projects/exocortex/.audit/2026-10-14/targeted-tests/chat-b/b-193115-8e7b4ea7/delivery-inventory.json) фиксирует SHA каждого уникального baseline/new source/config path; [source-delta.json](C:/.projects/exocortex/.audit/2026-10-14/targeted-tests/chat-b/b-193115-8e7b4ea7/source-delta.json) включает также параллельные изменения и **не является списком авторства CHAT-B**. Baseline содержит 543 записи с повторяющимися путями; ни один baseline path не пропал.
- Последняя обнаруженная сторонняя правка `WindowsSyncService.cs` включена в финальную сборку; сетевое поведение квалифицирует network-владелец. Собственные тесты расположены в `tests/Neptune.Core.Tests/Stage3ChatBTests*.cs`, helper — `tests/Neptune.Stage3.NativeHelper/`.
- [delivery-binaries.json](C:/.projects/exocortex/.audit/2026-10-14/targeted-tests/chat-b/b-193115-8e7b4ea7/delivery-binaries.json): SHA Core `9EF84D3B57D995ED46537A71EC51DA3D6077F12AB3AC2254A2EA50F54F196E82`, Windows `EAF29DDF86FAB46523AEFAF5320A944E08D214502127D6952AE042BD54BD2E1D`. Per-lane hashes сохранены отдельно, они не заменены задним числом последним build hash.
- Shared `bin/obj` защищён удерживаемым OS `FileShare.None` lock. Проверка в конце показала LOCK_HELD. Все dotnet test/helpers завершены; оставшиеся MSBuild workers не являются тестами. Событие release записывается в журнал после сохранения отчёта.
- K1 r2 / K2 r2 / K3 r2 приняты, новых контрактных блокеров нет. Прочитан журнал A до записи 20:17:52: там выполняются собственные server tests. Их результаты не присвоены CHAT-B. Совместный A15 receipt/benefit отсутствует; automatic batching остаётся OFF.
- Для A15: `RUN/final-transport-policy/evidence/B07-wire-examples.json` — реальные request/response objects **production transport на HttpMessageHandler fixture**, с явным `fixture:true`; `B15-codec.json`, `B15-gzip-*.json`, `B15-diagnostics.json` — client evidence. Для полноценной интеграции нужен отдельный API root владельца A и запуск этих callers против него.
- C01: `RUN/final-state-isolated/evidence/*B01-historical*.json` содержит retained диапазон, counts, phase trace и protected table hashes. Нельзя сбросить cursor в исходном историческом профиле ради искусственного before/fix.
- B12 predecessor: `.audit/2026-10-13/sync-revalidation-095229/OldBatchReader/build/bin/OldBatchReader/debug/Neptune.Core.dll`, SHA `b3c26bf78f3c7a6329f1f97144de804f345fb192f749da9355988f729e273fc5`, совпал с pinned hash исторического OldBatchReader. Новый helper загружает эту неизменённую сборку в отдельный AssemblyLoadContext и вызывает её InitializeAsync только на новой fixture. Evidence: `RUN/final-old-stream/evidence/B12-accepted-backup-stream-*.json`, `RUN/final-old-batch/evidence/B12-accepted-backup-batch-*.json`; текущая сборка не выдаётся за старую.

Следующий необходимый этап: disposable Windows VM/user для B13/B14, изолированный NTFS volume для фактического journal reset/trim, отдельное historical before/fix доказательство C01; согласованный A15 API fixture и codec/gzip receipts. Установленная версия Neptune этим этапом не обновлялась.
