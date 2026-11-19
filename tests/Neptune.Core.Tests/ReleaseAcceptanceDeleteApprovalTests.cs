using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class Stage3ChatBTests
{
    [Theory]
    [InlineData("restored-path")]
    [InlineData("remote-version")]
    [InlineData("excluded")]
    [InlineData("native-source-generation")]
    [InlineData("source-gap")]
    public async Task T04_ApprovedDeleteStillRefusesChangedAbsenceVersionOrPolicy(string change)
    {
        RequireWindows();
        var clock = new Stage3Clock();
        await using var fixture = await WindowsFreshCycleTests.Fixture.Create(clock);
        fixture.Remote.ExtensionsEnabled = true;
        var path = Path.Combine(fixture.Source, "entry");
        File.WriteAllText(path, "original generation");
        await using var engine = await NamespaceEngine(fixture);
        await ManagedBaseline(engine, fixture, clock, 1);
        var remote = fixture.Remote.CurrentFixtureNode("entry");
        var retainedSource = Path.Combine(fixture.Directory, "retained-source");
        if (change == "native-source-generation") File.Move(path, retainedSource);
        else File.Delete(path);
        await AwaitHint(fixture);
        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.Null((await engine.FreshSyncAsync(Ct)).Error);
        var preview = await engine.PreviewDeletesAsync(Ct);
        if (change == "native-source-generation")
        {
            // Native hint timing may expose the now-absent path as an
            // unapproved preview. The safety contract is that moving and later
            // changing the same incarnation never approves a remote deletion.
            Assert.All(preview.Operations,operation=>Assert.False(operation.Approved));
            File.WriteAllText(retainedSource, "later original generation");
            for (var attempt = 0; attempt < 3; attempt++)
            {
                await fixture.WaitDueAsync();
                await engine.FreshSyncAsync(Ct);
            }
            Assert.Empty(fixture.Remote.Approvals);
            Assert.Empty(fixture.Remote.Mutations);
            Assert.Equal(2, fixture.Remote.Count);
            Assert.Equal("later original generation", File.ReadAllText(retainedSource));
            await fixture.Evidence("T04-native-source-change-not-admitted", new
                { preview, fixture.Remote.Approvals, fixture.Remote.Mutations, stage = "before approval", state = await NamespaceState(fixture) });
            return;
        }
        Assert.Single(preview.Operations);
        await engine.ApproveDeletesAsync(preview.Digest, Ct);
        var original = Assert.Single(await Session(engine).ReadNamespaceOperationsAsync(engine.ScopeId, Ct));
        if (change == "source-gap")
        {
            await Session(engine).MarkGapAsync("JournalGap", Ct);
            var stopped = await engine.FreshSyncAsync(Ct);
            Assert.NotNull(stopped.Error);
            Assert.Empty(fixture.Remote.Mutations);
            Assert.Equal(2, fixture.Remote.Count);
            await fixture.Evidence("T04-reviewed-delete-source-gap", new { preview, stopped });
            return;
        }
        if (change == "restored-path") File.WriteAllText(path, "later local generation");
        if (change == "remote-version") fixture.Remote.EditAcceptanceFile(remote.NodeId, "later remote generation");
        if (change == "excluded")
        {
            var originalPolicy = await engine.ReadMappingPolicyAsync(Ct);
            var policy = originalPolicy with { ExcludedPrefixes = ["entry"] };
            var proposed = await engine.PreviewPolicyAsync(policy, Ct);
            var rejected = await Assert.ThrowsAsync<InvalidOperationException>(() => engine.ApplyPolicyAsync(policy, proposed.Digest, Ct));
            Assert.Contains("Recover original operations", rejected.Message);
            Assert.Equal(originalPolicy.Hash, (await engine.ReadMappingPolicyAsync(Ct)).Hash);
            Assert.Equal(System.Text.Json.JsonSerializer.Serialize(original.Request),
                System.Text.Json.JsonSerializer.Serialize(Assert.Single(await Session(engine).ReadNamespaceOperationsAsync(engine.ScopeId, Ct)).Request));
            Assert.Empty(fixture.Remote.Approvals);
            Assert.Empty(fixture.Remote.Mutations);
            await fixture.Evidence("T04-policy-change-blocked-by-reviewed-original", new
                { preview, rejected = rejected.Message, originalRequestPreserved = true, policyUnchanged = true });
            return;
        }
        await Session(engine).SaveEventHintsAsync(["entry"], true, Ct);
        var passes = new List<WindowsCycleResult>();
        for (var attempt = 0; attempt < 5; attempt++)
        {
            await fixture.WaitDueAsync();
            passes.Add(await engine.FreshSyncAsync(Ct));
        }
        await fixture.Evidence("T04-reviewed-delete-refusal-" + change, new
            { preview, passes, fixture.Remote.Approvals, fixture.Remote.Mutations, state = await NamespaceState(fixture) });
        Assert.Empty(fixture.Remote.Mutations);
        Assert.Equal(2, fixture.Remote.Count);
        Assert.Equal(remote.NodeId, fixture.Remote.CurrentFixtureNode("entry").NodeId);
        if (change == "restored-path") Assert.Equal("later local generation", File.ReadAllText(path));
        if (change == "remote-version") Assert.NotEqual(remote.ContentVersionId, fixture.Remote.CurrentFixtureNode("entry").ContentVersionId);
    }

    [Fact]
    public async Task T04_ThirtyTwoFilesRejectStaleApprovalAndKeepLaterGenerationAfterDeleteAckLoss()
    {
        RequireWindows();
        var clock = new Stage3Clock();
        await using var fixture = await WindowsFreshCycleTests.Fixture.Create(clock);
        fixture.Remote.ExtensionsEnabled = true;
        var directory = Path.Combine(fixture.Source, "first", "second", "third");
        Directory.CreateDirectory(directory);
        var names = Enumerable.Range(0, 32).Select(index => $"entry-{index:D2}.bin").ToArray();
        foreach (var name in names) File.WriteAllText(Path.Combine(directory, name), "original " + name);
        var neighbor = Path.Combine(fixture.Directory, "independent-root");
        Directory.CreateDirectory(neighbor);
        var neighborFile = Path.Combine(neighbor, "keep.bin");
        File.WriteAllText(neighborFile, "independent original");
        await using var engine = await NamespaceEngine(fixture);
        await ManagedBaseline(engine, fixture, clock, 35);
        var changedRemote = fixture.Remote.CurrentFixtureNode(names[1]);
        var renamedRemote = fixture.Remote.CurrentFixtureNode(names[2]);
        foreach (var name in names) File.Delete(Path.Combine(directory, name));
        await AwaitHint(fixture);
        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.Null((await engine.FreshSyncAsync(Ct)).Error);
        var reviewed = await engine.PreviewDeletesAsync(Ct);
        Assert.Equal(32, reviewed.Operations.Count);

        File.WriteAllText(Path.Combine(directory, names[0]), "new local generation");
        fixture.Remote.EditAcceptanceFile(changedRemote.NodeId, "new remote generation");
        fixture.Remote.RenameFolder(renamedRemote.NodeId, "renamed-remotely.bin");
        await Session(engine).SaveEventHintsAsync(["first/second/third/" + names[0]], true, Ct);
        var stale = await Record.ExceptionAsync(() => engine.ApproveDeletesAsync(reviewed.Digest, Ct));
        await fixture.Evidence("T04-stale-approval-attempt", new
        {
            errorType = stale?.GetType().FullName, error = stale?.Message, reviewed,
            fixture.Remote.Approvals, fixture.Remote.Mutations, state = await NamespaceState(fixture)
        });
        Assert.True(stale is InvalidOperationException or LocalSourceGapException or BootstrapCaptureDeferredException,
            "Changed source/remote/gap must reject the reviewed deletion before approval effects.");
        Assert.Empty(fixture.Remote.Approvals);
        Assert.Empty(fixture.Remote.Mutations);
        Assert.Equal(36, fixture.Remote.Count);

        WindowsCycleResult? settled = null;
        for (var attempt = 0; attempt < 5; attempt++)
        {
            await fixture.WaitDueAsync();
            settled = await engine.FreshSyncAsync(Ct);
            if (settled.Error is null) break;
            Assert.Equal("SOURCE_BUSY", settled.RetryState);
        }
        Assert.NotNull(settled);
        Assert.Null(settled.Error);
        await Assert.ThrowsAsync<InvalidOperationException>(() => engine.ApproveDeletesAsync(reviewed.Digest, Ct));
        var current = await engine.PreviewDeletesAsync(Ct);
        Assert.Equal(29, current.Operations.Count);
        Assert.NotEqual(reviewed.Digest, current.Digest);
        Assert.DoesNotContain(current.Operations, item => names.Take(3).Contains(item.Path.Split('/').Last()));
        await engine.ApproveDeletesAsync(current.Digest, Ct);
        fixture.Remote.LoseMutationReply = true;
        fixture.Remote.BeforeMutation = async _ =>
        {
            fixture.Remote.BeforeMutation = null;
            await Session(engine).SaveEventHintsAsync(["first/second/third/" + names[0]], false, Ct);
        };
        await engine.FreshSyncAsync(Ct);
        Assert.Single(fixture.Remote.Mutations);
        var later = await Session(engine).ReadEventHintsAsync(Ct);
        Assert.Contains(later.Hints, hint => hint.Path == "first/second/third/" + names[0]);
        await engine.DisposeAsync();
        fixture.Store = new(fixture.Database, clock);
        await using var resumed = await NamespaceEngine(fixture);
        for (var pass = 0; pass < 5 && fixture.Remote.Mutations.Count < 29; pass++)
        {
            await AdvancePastDue(fixture, clock);
            Assert.Null((await resumed.FreshSyncAsync(Ct)).Error);
        }
        Assert.Equal(29, fixture.Remote.Mutations.Count);
        Assert.Equal(29, fixture.Remote.Mutations.Select(operation => operation.OperationId).Distinct().Count());
        Assert.All(fixture.Remote.Mutations, operation => Assert.Contains(current.Operations, item => item.OperationId == operation.OperationId));
        Assert.Equal(7, fixture.Remote.Count);
        Assert.Equal("new local generation", File.ReadAllText(Path.Combine(directory, names[0])));
        Assert.Equal("independent original", File.ReadAllText(neighborFile));
        Assert.Equal(changedRemote.NodeId, fixture.Remote.CurrentFixtureNode(names[1]).NodeId);
        Assert.NotEqual(changedRemote.ContentVersionId, fixture.Remote.CurrentFixtureNode(names[1]).ContentVersionId);
        Assert.Equal(renamedRemote.NodeId, fixture.Remote.CurrentFixtureNode("renamed-remotely.bin").NodeId);
        await fixture.Evidence("T04-32-stale-approval-generation-ack", new
        {
            reviewed, current, later, fixture.Remote.Mutations, fixture.Remote.Approvals,
            neighborPreserved = true, state = await NamespaceState(fixture)
        });
    }

    [Fact]
    public async Task T04_DeleteApprovalLostReplyAndLaterGapRecoverSamePostorderOriginals()
    {
        RequireWindows();
        var clock = new Stage3Clock();
        await using var fixture = await WindowsFreshCycleTests.Fixture.Create(clock);
        fixture.Remote.ExtensionsEnabled = true;
        var parent = Path.Combine(fixture.Source, "parent");
        Directory.CreateDirectory(Path.Combine(parent, "child"));
        File.WriteAllText(Path.Combine(parent, "child", "grandchild"), "retained content");
        await using var engine = await NamespaceEngine(fixture);
        await ManagedBaseline(engine, fixture, clock, 3);
        Directory.Delete(parent, true);
        await AwaitHint(fixture);
        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.Null((await engine.FreshSyncAsync(Ct)).Error);
        var preview = await engine.PreviewDeletesAsync(Ct);
        Assert.Equal(3, preview.Operations.Count);
        await engine.ApproveDeletesAsync(preview.Digest, Ct);
        var originalRows = await OriginalRows();
        fixture.Remote.LoseApprovalReply = true;
        await engine.FreshSyncAsync(Ct);
        Assert.Empty(fixture.Remote.Mutations);
        // Watchers may deliver the same already-proved removal after approval
        // recovery. A later hint must retain its generation without replacing
        // this reviewed set with fresh, unapproved operation IDs.
        await Session(engine).SaveEventHintsAsync(["parent/child/grandchild", "parent/child", "parent"], false, Ct);
        await Session(engine).SaveEventHintsAsync([], true, Ct);
        var laterGap = (await Session(engine).ReadEventHintsAsync(Ct)).GapGeneration;
        await engine.DisposeAsync();
        fixture.Store = new(fixture.Database, clock);
        await AdvancePastDue(fixture, clock);
        await using var resumed = await NamespaceEngine(fixture);
        var passes = new List<WindowsCycleResult>();
        for (var attempt = 0; attempt < 5 && fixture.Remote.Mutations.Count < 3; attempt++)
        {
            await AdvancePastDue(fixture, clock);
            var result = await resumed.FreshSyncAsync(Ct);
            passes.Add(result);
            Assert.Null(result.Error);
        }
        await fixture.Evidence("T04-delete-approval-later-gap", new
        {
            preview, laterGap, passes, fixture.Remote.Approvals, fixture.Remote.Mutations,
            state = await NamespaceState(fixture)
        });
        Assert.Equal(new[] { "grandchild", "child", "parent" }, fixture.Remote.Mutations.Select(
            operation => preview.Operations.Single(item => item.OperationId == operation.OperationId).Path.Split('/').Last()));
        Assert.Equal(2, fixture.Remote.Approvals.Count);
        Assert.Equal(System.Text.Json.JsonSerializer.Serialize(fixture.Remote.Approvals[0]),
            System.Text.Json.JsonSerializer.Serialize(fixture.Remote.Approvals[1]));
        Assert.Equal(1, fixture.Remote.Count);
        Assert.Equal(0L, await fixture.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE baseline IS NOT NULL"));
        Assert.Equal(originalRows, await OriginalRows());
        var settled = new List<WindowsCycleResult>();
        var prepares = fixture.Remote.Prepares;
        for (var attempt = 0; attempt < 5; attempt++)
        {
            await fixture.WaitDueAsync();
            var result = await resumed.FreshSyncAsync(Ct);
            settled.Add(result);
            Assert.Null(result.Error);
            if (result.PendingEntries == 0) break;
        }
        await fixture.Evidence("T04-delete-later-hint-settle", new { settled, fixture.Remote.Prepares, fixture.Remote.Mutations, state = await NamespaceState(fixture) });
        Assert.Equal(0, settled[^1].PendingEntries);
        Assert.Equal(prepares, fixture.Remote.Prepares);
        Assert.Equal(3, fixture.Remote.Mutations.Count);
        Assert.Equal(originalRows, await OriginalRows());

        async Task<string> OriginalRows()
        {
            await using var database = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=" + fixture.Database + ";Pooling=False");
            await database.OpenAsync(Ct);
            await using var query = database.CreateCommand();
            query.CommandText = "SELECT operation_id,request_json,entry_json FROM client_namespace_operations WHERE scope_id=$scope AND kind='quarantine-delete' ORDER BY operation_id";
            query.Parameters.AddWithValue("$scope", engine.ScopeId);
            var rows = new List<string>();
            await using var reader = await query.ExecuteReaderAsync(Ct);
            while (await reader.ReadAsync(Ct)) rows.Add(reader.GetString(0) + "\n" + reader.GetString(1) + "\n" + reader.GetString(2));
            return string.Join("\n", rows);
        }
    }

    [Fact]
    public async Task T04_NewAbsenceProofCannotRetireWatcherGenerationArrivingAfterItsSnapshot()
    {
        RequireWindows();
        var clock = new Stage3Clock();
        await using var fixture = await WindowsFreshCycleTests.Fixture.Create(clock);
        fixture.Remote.ExtensionsEnabled = true;
        var path = Path.Combine(fixture.Source, "entry");
        File.WriteAllText(path, "reviewed original");
        await using var engine = await NamespaceEngine(fixture);
        await ManagedBaseline(engine, fixture, clock, 1);
        File.Delete(path);
        await AwaitHint(fixture);
        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.Null((await engine.FreshSyncAsync(Ct)).Error);
        var preview = await engine.PreviewDeletesAsync(Ct);
        Assert.Single(preview.Operations);
        await engine.ApproveDeletesAsync(preview.Digest, Ct);
        for (var attempt = 0; attempt < 5 && fixture.Remote.Mutations.Count == 0; attempt++)
        {
            await fixture.WaitDueAsync();
            Assert.Null((await engine.FreshSyncAsync(Ct)).Error);
        }
        Assert.Single(fixture.Remote.Mutations);
        var session = Session(engine);
        var originalJson = await fixture.Scalar("SELECT request_json||entry_json FROM client_namespace_operations WHERE kind='quarantine-delete'");
        await session.MarkParentCycleHintsAsync(engine.ScopeId, ["entry"], Ct);
        var checks = await session.ReadDeletedNamespaceRechecksAsync(engine.ScopeId, Ct);
        Assert.Single(checks);
        Assert.True(await session.RequestFullParentProofAsync(engine.ScopeId, Ct));
        var plan = await session.BeginParentReconciliationAsync(engine.ScopeId, Ct);
        var coordinator = (WindowsLocalCoherenceCoordinator)typeof(WindowsFreshSyncEngine).GetField("_coordinator", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(engine)!;
        Assert.Equal(LocalCoherenceStatus.Anchored, (await coordinator.ReconcileParentsAsync(session, engine.ScopeId, plan.CoherenceId, Ct)).Status);
        await session.MarkParentCycleHintsAsync(engine.ScopeId, ["entry"], Ct);
        await session.RetireDeletedNamespaceRechecksAsync(engine.ScopeId, plan.PlanId, checks, Ct);
        Assert.Equal(1L, await fixture.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE phase<4 AND watcher_generation>ack_watcher"));
        var before = await NamespaceState(fixture);
        var prepares = fixture.Remote.Prepares;
        WindowsCycleResult? settled = null;
        for (var attempt = 0; attempt < 5; attempt++)
        {
            await fixture.WaitDueAsync();
            settled = await engine.FreshSyncAsync(Ct);
            Assert.Null(settled.Error);
            if (settled.PendingEntries == 0) break;
        }
        Assert.Equal(0, settled!.PendingEntries);
        Assert.Single(fixture.Remote.Mutations);
        Assert.Equal(prepares, fixture.Remote.Prepares);
        Assert.Equal(originalJson, await fixture.Scalar("SELECT request_json||entry_json FROM client_namespace_operations WHERE kind='quarantine-delete'"));
        await fixture.Evidence("T04-native-proof-later-watcher-pair", new { before, settled, after = await NamespaceState(fixture) });
    }




    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task T04_NewAbsenceProofPreservesCapturedOriginalDespiteUnacceptedPhase(bool deferred)
    {
        RequireWindows();
        var clock = new Stage3Clock();
        await using var fixture = await WindowsFreshCycleTests.Fixture.Create(clock);
        fixture.Remote.ExtensionsEnabled = true;
        var path = Path.Combine(fixture.Source, "entry");
        File.WriteAllText(path, "reviewed original");
        await using var engine = await NamespaceEngine(fixture);
        await ManagedBaseline(engine, fixture, clock, 1);
        var session = Session(engine);
        var operation = (string)(await fixture.Scalar("SELECT operation_id FROM local_cycle_work WHERE capture_proof IS NOT NULL ORDER BY work_no LIMIT 1"))!;
        var captured = await session.ReadCycleWorkAsync(engine.ScopeId, operation, Ct);
        Assert.NotNull(captured.CaptureProof);
        Assert.NotNull(captured.InputJson);
        File.Delete(path);
        await AwaitHint(fixture);
        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.Null((await engine.FreshSyncAsync(Ct)).Error);
        var preview = await engine.PreviewDeletesAsync(Ct);
        Assert.Single(preview.Operations);
        await engine.ApproveDeletesAsync(preview.Digest, Ct);
        for (var attempt = 0; attempt < 5 && fixture.Remote.Mutations.Count == 0; attempt++)
        {
            await fixture.WaitDueAsync();
            Assert.Null((await engine.FreshSyncAsync(Ct)).Error);
        }
        Assert.Single(fixture.Remote.Mutations);
        await session.MarkParentCycleHintsAsync(engine.ScopeId, ["entry"], Ct);
        Assert.True(await session.RequestFullParentProofAsync(engine.ScopeId, Ct));
        var plan = await session.BeginParentReconciliationAsync(engine.ScopeId, Ct);
        var coordinator = (WindowsLocalCoherenceCoordinator)typeof(WindowsFreshSyncEngine).GetField("_coordinator", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(engine)!;
        Assert.Equal(LocalCoherenceStatus.Anchored, (await coordinator.ReconcileParentsAsync(session, engine.ScopeId, plan.CoherenceId, Ct)).Status);
        var checks = await session.ReadDeletedNamespaceRechecksAsync(engine.ScopeId, Ct);
        Assert.Single(checks);
        // Deliberately reopen only this owned test ledger's genuine captured
        // original with an unaccepted-looking phase. No remote action follows:
        // the retirement guard must inspect retained evidence, not phase alone.
        await fixture.Scalar($"UPDATE local_cycle_work SET active=1,phase={(deferred ? 8 : 0)} WHERE operation_id='{operation}'");
        var before = await session.ReadCycleWorkAsync(engine.ScopeId, operation, Ct);
        Assert.Empty(await session.ReadDeletedNamespaceRechecksAsync(engine.ScopeId, Ct));
        await session.RetireDeletedNamespaceRechecksAsync(engine.ScopeId, plan.PlanId, checks, Ct);
        Assert.Equal(before, await session.ReadCycleWorkAsync(engine.ScopeId, operation, Ct));
        Assert.Equal(1L, await fixture.Scalar($"SELECT COUNT(*) FROM local_cycle_work WHERE operation_id='{operation}' AND active=1"));
        Assert.Single(fixture.Remote.Mutations);
        await fixture.Evidence("T04-absence-proof-retains-captured-original", new { deferred, operation, before.Phase, before.Sha256, protectedOriginalUnchanged = true, fixtureOnlyPhaseMutation = true });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task T04_ConfirmedAbsenceRetiresOnlyUnacceptedDeleteRecheckWork(bool deferred)
    {
        RequireWindows();
        var clock = new Stage3Clock();
        await using var fixture = await WindowsFreshCycleTests.Fixture.Create(clock);
        fixture.Remote.ExtensionsEnabled = true;
        var path = Path.Combine(fixture.Source, "entry");
        File.WriteAllText(path, "reviewed original");
        await using var engine = await NamespaceEngine(fixture);
        await ManagedBaseline(engine, fixture, clock, 1);
        File.Delete(path);
        await AwaitHint(fixture);
        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.Null((await engine.FreshSyncAsync(Ct)).Error);
        var preview = await engine.PreviewDeletesAsync(Ct);
        Assert.Single(preview.Operations);
        await engine.ApproveDeletesAsync(preview.Digest, Ct);
        for (var attempt = 0; attempt < 5 && fixture.Remote.Mutations.Count == 0; attempt++)
        {
            await fixture.WaitDueAsync();
            Assert.Null((await engine.FreshSyncAsync(Ct)).Error);
        }
        Assert.Single(fixture.Remote.Mutations);
        var session = Session(engine);
        var original = await fixture.Scalar("SELECT request_json||entry_json||receipt_json FROM client_namespace_operations WHERE kind='quarantine-delete'");
        // A late source hint can admit capture work after the reviewed delete
        // was published. No input, capture, spool or remote attempt exists.
        await session.MarkParentCycleHintsAsync(engine.ScopeId, ["entry"], Ct);
        var work = await session.ClaimCycleWorkAsync(engine.ScopeId, Ct);
        Assert.NotNull(work);
        Assert.Null(work.InputJson);
        Assert.Null(work.CaptureProof);
        Assert.Null(work.SpoolPath);
        if (deferred)
        {
            Assert.True(await session.ScheduleReadyAsync(engine.ScopeId, work.OperationId, "file", Ct));
            await session.FinishCycleWorkAsync(engine.ScopeId, work.OperationId, LocalCycleWorkPhase.Deferred, null, "Source absent before capture.", Ct);
            await session.SaveOperationWaitAsync(engine.ScopeId, work.OperationId, new("SOURCE_BUSY", "source_or_original_unresolved"), Ct);
        }
        var checks = await session.ReadDeletedNamespaceRechecksAsync(engine.ScopeId, Ct);
        await fixture.Evidence("T04-unaccepted-delete-recheck-before-proof", new { deferred, work.OperationId, checks = checks.Count });
        Assert.Single(checks);
        Assert.True(await session.RequestFullParentProofAsync(engine.ScopeId, Ct));
        var plan = await session.BeginParentReconciliationAsync(engine.ScopeId, Ct);
        var coordinator = (WindowsLocalCoherenceCoordinator)typeof(WindowsFreshSyncEngine).GetField("_coordinator", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(engine)!;
        Assert.Equal(LocalCoherenceStatus.Anchored, (await coordinator.ReconcileParentsAsync(session, engine.ScopeId, plan.CoherenceId, Ct)).Status);
        await session.MarkParentCycleHintsAsync(engine.ScopeId, ["entry"], Ct);
        await session.RetireDeletedNamespaceRechecksAsync(engine.ScopeId, plan.PlanId, checks, Ct);
        Assert.Equal(1L, await fixture.Scalar($"SELECT COUNT(*) FROM local_cycle_work WHERE operation_id='{work.OperationId}' AND active=1"));
        Assert.Equal(1L, await fixture.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE phase<4 AND watcher_generation>ack_watcher"));
        checks = await session.ReadDeletedNamespaceRechecksAsync(engine.ScopeId, Ct);
        Assert.Single(checks);
        Assert.True(await session.RequestFullParentProofAsync(engine.ScopeId, Ct));
        plan = await session.BeginParentReconciliationAsync(engine.ScopeId, Ct);
        Assert.Equal(LocalCoherenceStatus.Anchored, (await coordinator.ReconcileParentsAsync(session, engine.ScopeId, plan.CoherenceId, Ct)).Status);
        await session.RetireDeletedNamespaceRechecksAsync(engine.ScopeId, plan.PlanId, checks, Ct);
        Assert.Equal(1L, await fixture.Scalar($"SELECT COUNT(*) FROM local_cycle_work WHERE operation_id='{work.OperationId}' AND phase=9 AND active IS NULL AND input_json IS NULL AND capture_proof IS NULL AND spool_path IS NULL AND received=0"));
        Assert.Equal(0L, await fixture.Scalar($"SELECT COUNT(*) FROM client_operation_schedule WHERE operation_id='{work.OperationId}'"));
        Assert.Equal(0L, await fixture.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE phase<4 OR watcher_generation<>ack_watcher"));
        Assert.Equal(original, await fixture.Scalar("SELECT request_json||entry_json||receipt_json FROM client_namespace_operations WHERE kind='quarantine-delete'"));
        Assert.Single(fixture.Remote.Mutations);
        await fixture.Evidence("T04-unaccepted-delete-recheck-retired", new { deferred, work.OperationId, plan.PlanId, state = await NamespaceState(fixture) });
    }

    [Fact]
    public async Task T04_OneHundredTwentyOnePublishedDeletesDrainBoundedRechecksThenNoopDoesNotScan()
    {
        RequireWindows();
        var clock = new Stage3Clock();
        await using var fixture = await WindowsFreshCycleTests.Fixture.Create(clock);
        fixture.Remote.ExtensionsEnabled = true;
        var names = Enumerable.Range(0, 121).Select(index => $"entry-{index:D3}").ToArray();
        foreach (var name in names) File.WriteAllText(Path.Combine(fixture.Source, name), "reviewed " + name);
        await using var engine = await NamespaceEngine(fixture);
        await ManagedBaseline(engine, fixture, clock, names.Length);
        foreach (var name in names) File.Delete(Path.Combine(fixture.Source, name));
        await AwaitHint(fixture);
        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.Null((await engine.FreshSyncAsync(Ct)).Error);
        var preview = await engine.PreviewDeletesAsync(Ct);
        Assert.Equal(names.Length, preview.Operations.Count);
        await engine.ApproveDeletesAsync(preview.Digest, Ct);
        for (var attempt = 0; attempt < 5 && fixture.Remote.Mutations.Count < names.Length; attempt++)
        {
            await fixture.WaitDueAsync();
            Assert.Null((await engine.FreshSyncAsync(Ct)).Error);
        }
        Assert.Equal(names.Length, fixture.Remote.Mutations.Count);
        Assert.Equal(names.Length, fixture.Remote.Mutations.Select(operation => operation.OperationId).Distinct().Count());
        // Isolate the newly introduced bounded retirement path from real
        // source changes: all 121 receipts are already terminal and retained.
        // Remote effect count can lead local receipt settlement at a bounded
        // cycle yield. Establish that precondition before dirtying all rows.
        const string settledReceipts = "SELECT COUNT(*) FROM client_namespace_operations o JOIN local_cycle_files f ON f.scope_id=o.scope_id AND f.node_id=o.node_id WHERE o.kind='quarantine-delete' AND o.state='PUBLISHED' AND o.receipt_json IS NOT NULL AND f.baseline IS NULL AND f.remote_node IS NULL AND NOT EXISTS(SELECT 1 FROM local_cycle_work w WHERE w.scope_id=f.scope_id AND w.node_id=f.node_id AND w.active=1)";
        var settlementObservations = new List<object>();
        for (var attempt = 0; attempt < 5 && (long)(await fixture.Scalar(settledReceipts))! < names.Length; attempt++)
        {
            await fixture.WaitDueAsync();
            var cycle = await engine.FreshSyncAsync(Ct);
            Assert.Null(cycle.Error);
            settlementObservations.Add(new { attempt = attempt + 1, retainedSettledReceipts = await fixture.Scalar(settledReceipts), cycle.Decision, cycle.Error, cycle.RetryState, cycle.PendingEntries, cycle.Deferred, cycle.PublishedFiles, mutations = fixture.Remote.Mutations.Count, clock = clock.GetUtcNow() });
        }
        await fixture.Evidence("T04-receipt-settlement-before-hints", new
        {
            expected = names.Length,
            retainedSettledReceipts = await fixture.Scalar(settledReceipts),
            settledFileRows = await fixture.Scalar("SELECT COUNT(*) FROM local_cycle_files f WHERE f.kind=0 AND f.baseline IS NULL AND f.remote_node IS NULL AND NOT EXISTS(SELECT 1 FROM local_cycle_work w WHERE w.scope_id=f.scope_id AND w.node_id=f.node_id AND w.active=1)"),
            operationStates = await fixture.Scalar("SELECT json_group_array(json_object('state',state,'rows',n,'receipts',r)) FROM(SELECT state,COUNT(*) n,SUM(receipt_json IS NOT NULL) r FROM client_namespace_operations WHERE kind='quarantine-delete' GROUP BY state)"),
            filePhases = await fixture.Scalar("SELECT json_group_array(json_object('phase',phase,'rows',n,'baselines',b,'remoteNodes',r)) FROM(SELECT phase,COUNT(*) n,SUM(baseline IS NOT NULL) b,SUM(remote_node IS NOT NULL) r FROM local_cycle_files WHERE kind=0 GROUP BY phase)"),
            activeWork = await fixture.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE active=1"),
            activeWorkFacts = await fixture.Scalar("SELECT json_group_array(json_object('operation',operation_id,'node',node_id,'phase',phase,'input',input_json IS NOT NULL,'spool',spool_path IS NOT NULL,'proof',capture_proof IS NOT NULL,'upload',upload_id,'received',received,'error',error)) FROM local_cycle_work WHERE active=1"),
            remoteEffects = fixture.Remote.Mutations.Count,
            attempts = settlementObservations
        });
        Assert.Equal((long)names.Length, await fixture.Scalar(settledReceipts));
        Assert.Equal(names.Length, fixture.Remote.Mutations.Count);
        await Session(engine).MarkParentCycleHintsAsync(engine.ScopeId, names, Ct);
        Assert.Equal(121L, await fixture.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE phase<4"));
        // Completed parent plans are compacted, so retained row counts are
        // not a cumulative proof counter. Exercise the same bounded recheck
        // protocol explicitly, retaining each actual completed plan identity.
        var session = Session(engine);
        var coordinator = (WindowsLocalCoherenceCoordinator)typeof(WindowsFreshSyncEngine).GetField("_coordinator", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(engine)!;
        var completedPlans = new List<string>();
        var batchSizes = new List<int>();
        var prepares = fixture.Remote.Prepares;
        foreach (var expectedBatchSize in new[] { 120, 1 })
        {
            var checks = await session.ReadDeletedNamespaceRechecksAsync(engine.ScopeId, Ct);
            Assert.Equal(expectedBatchSize, checks.Count);
            batchSizes.Add(checks.Count);
            Assert.True(await session.RequestFullParentProofAsync(engine.ScopeId, Ct));
            var plan = await session.BeginParentReconciliationAsync(engine.ScopeId, Ct);
            Assert.Equal(LocalCoherenceStatus.Anchored, (await coordinator.ReconcileParentsAsync(session, engine.ScopeId, plan.CoherenceId, Ct)).Status);
            Assert.Equal(1L, await fixture.Scalar($"SELECT COUNT(*) FROM local_parent_reconcile_plans WHERE plan_id='{plan.PlanId}' AND full_membership=1 AND status=3"));
            await session.RetireDeletedNamespaceRechecksAsync(engine.ScopeId, plan.PlanId, checks, Ct);
            completedPlans.Add(plan.PlanId);
        }
        Assert.Equal(2, completedPlans.Distinct().Count());
        Assert.Empty(await session.ReadDeletedNamespaceRechecksAsync(engine.ScopeId, Ct));
        var passes = new List<WindowsCycleResult>();
        for (var attempt = 0; attempt < 5; attempt++)
        {
            await fixture.WaitDueAsync();
            var result = await engine.FreshSyncAsync(Ct);
            passes.Add(result);
            Assert.Null(result.Error);
            if (result.PendingEntries == 0) break;
        }
        await fixture.Evidence("T04-121-bounded-delete-rechecks-progress", new { completedPlans, batchSizes, passes, state = await NamespaceState(fixture) });
        Assert.Equal(0, passes[^1].PendingEntries);
        Assert.Equal(0L, await fixture.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE phase<4 OR watcher_generation<>ack_watcher"));
        const string proofIdentities = "SELECT group_concat(plan_id, ',') FROM (SELECT plan_id FROM local_parent_reconcile_plans WHERE full_membership=1 AND status=3 ORDER BY plan_id)";
        var after = await fixture.Scalar(proofIdentities);
        await fixture.WaitDueAsync();
        var noop = await engine.FreshSyncAsync(Ct);
        Assert.Null(noop.Error);
        Assert.Equal(0, noop.PendingEntries);
        Assert.Equal(after, await fixture.Scalar(proofIdentities));
        Assert.Equal(prepares, fixture.Remote.Prepares);
        Assert.Equal(names.Length, fixture.Remote.Mutations.Count);
        Assert.Equal(1, fixture.Remote.Count);
        await fixture.Evidence("T04-121-bounded-delete-rechecks", new { entries = names.Length, bound = 120, completedPlans, batchSizes, after, passes, noop, fixture.Remote.Prepares, mutations = fixture.Remote.Mutations.Count });
    }
}

public sealed partial class WindowsFreshCycleTests
{
    internal sealed partial class FakeRemote
    {
        internal void EditAcceptanceFile(string nodeId, string text)
        {
            lock (_gate)
            {
                var bytes = System.Text.Encoding.UTF8.GetBytes(text);
                var changed = _nodes[nodeId] with
                {
                    SizeBytes = bytes.Length.ToString(System.Globalization.CultureInfo.InvariantCulture),
                    ContentSha256 = Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(bytes)),
                    ContentVersionId = Guid.NewGuid().ToString("D"),
                    Version = Guid.NewGuid().ToString("D")
                };
                _nodes[nodeId] = changed;
                Event(changed, Guid.NewGuid().ToString("D"));
            }
        }
    }
}
