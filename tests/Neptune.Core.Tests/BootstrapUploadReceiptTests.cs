using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class BootstrapUploadReceiptTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private const string RootId = "671c88f7-4ef9-4c5e-a2de-8b294135fc4e";
    private static readonly Uri TargetRoot = new("https://saturn.test/dav/sync/Audit/Mapping/");

    [Fact]
    public async Task OriginalCreateFingerprintAndUuidCommitBeforeAnyPatchAndRawReceiptIsDurable()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync(); using var s = new Saturn();
        s.BeforePatch = async () =>
        {
            Assert.Equal(s.UploadId,await f.ScalarAsync("SELECT upload_id FROM bootstrap_work_items"));
            Assert.Equal(s.IntentHash,await f.ScalarAsync("SELECT intent_hash FROM bootstrap_work_items"));
            Assert.Equal(RootId,await f.ScalarAsync("SELECT expected_root_id FROM bootstrap_work_sessions"));
            Assert.Equal("sync/Audit/Mapping/file.txt",await f.ScalarAsync("SELECT request_path FROM bootstrap_work_items"));
        };
        var result = await f.Executor(s).ScanAndTransferAsync(f.Request,cancellationToken:Ct);
        Assert.Equal(1,result.PublishedFiles); Assert.Equal(0,result.Failures);
        var work = Assert.Single(await f.Store.ListBootstrapWorkAsync(result.SessionId,cancellationToken:Ct));
        var receipt = JsonSerializer.Deserialize<BootstrapPublicationReceipt>(work.Receipt!)!;
        Assert.Equal(BootstrapPublicationProof.QualifiedImmutableReceipt,receipt.Proof);
        Assert.Equal(work.OperationId,receipt.OperationId); Assert.Equal(s.UploadId,receipt.ServerReceipt!.OperationId);
        Assert.NotEqual(work.OperationId,receipt.ServerReceipt.OperationId); Assert.Equal(s.LastRawReceipt,receipt.ImmutableReceiptJson);
        Assert.Equal(0L,await f.ScalarAsync("SELECT COUNT(*) FROM sync_state_baselines"));
        var methods = s.Methods.Count;
        var replay = await f.Executor(s).ResumeSessionAsync(result.SessionId,f.Request,cancellationToken:Ct);
        Assert.Equal(1,replay.PublishedFiles); Assert.Equal(methods,s.Methods.Count);
    }

    [Fact]
    public async Task LostV1CompletionWithIndependentV2RecoversOriginalHistoricalReceiptAfterRestart()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync(); using var s = new Saturn { LoseComplete = true, CurrentV2 = true };
        var first = await f.Executor(s).ScanAndTransferAsync(f.Request,cancellationToken:Ct);
        Assert.Equal(1,first.Failures); Assert.Equal(0,first.PublishedFiles);
        var before = Assert.Single(await f.Store.ListBootstrapWorkAsync(first.SessionId,cancellationToken:Ct));
        Assert.Equal(s.IntentHash,before.OriginalIntentHash); Assert.Equal(s.UploadId,before.UploadId); Assert.True(File.Exists(before.SpoolPath));
        var patched = s.Methods.Count(x => x == "PATCH");
        var reopened = new NeptuneStateStore(f.Database); await reopened.InitializeAsync(Ct);
        var result = await f.Executor(s,store:reopened).ResumeSessionAsync(first.SessionId,f.Request,cancellationToken:Ct);
        Assert.Equal(1,result.PublishedFiles); Assert.Equal(0,result.Failures); Assert.Equal(1,s.Publications);
        Assert.Equal(patched,s.Methods.Count(x => x == "PATCH")); Assert.Equal(1,s.Methods.Count(x => x == "COMPLETE"));
        var after = Assert.Single(await reopened.ListBootstrapWorkAsync(first.SessionId,cancellationToken:Ct));
        var receipt = JsonSerializer.Deserialize<BootstrapPublicationReceipt>(after.Receipt!)!;
        Assert.Equal(before.OperationId,after.OperationId); Assert.Equal(s.VersionV1,receipt.ServerReceipt!.VersionId);
        Assert.Equal($"\"sha256-{before.Sha256}\"",receipt.Etag); Assert.NotEqual(s.V2Etag,receipt.Etag);
        Assert.Equal(0L,await f.ScalarAsync("SELECT COUNT(*) FROM sync_state_baselines"));
        Assert.DoesNotContain("PUT",s.Methods); Assert.DoesNotContain("DELETE",s.Methods);
    }

    [Fact]
    public async Task LostCreateResponseRecoversSameKeyAndFingerprintBeforeData()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync(); using var s = new Saturn { LoseCreate = true };
        var first = await f.Executor(s).ScanAndTransferAsync(f.Request,cancellationToken:Ct);
        var before = Assert.Single(await f.Store.ListBootstrapWorkAsync(first.SessionId,cancellationToken:Ct));
        Assert.Equal(1,first.Failures); Assert.Null(before.UploadId); Assert.Null(before.OriginalIntentHash); Assert.DoesNotContain("PATCH",s.Methods);
        var result = await f.Executor(s).ResumeSessionAsync(first.SessionId,f.Request,cancellationToken:Ct);
        Assert.Equal(1,result.PublishedFiles); Assert.Single(s.Keys.Distinct()); Assert.Equal(1,s.Publications);
        var after = Assert.Single(await f.Store.ListBootstrapWorkAsync(first.SessionId,cancellationToken:Ct));
        Assert.Equal(before.OperationId,after.OperationId); Assert.Equal(s.IntentHash,after.OriginalIntentHash); Assert.Equal(s.UploadId,after.UploadId);
    }

    [Fact]
    public async Task HistoricalRecoveryDoesNotRecreateOrRequireGoneDavParents()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync(); using var s = new Saturn { LoseComplete=true };
        var first = await f.Executor(s).ScanAndTransferAsync(f.Request,cancellationToken:Ct);
        Assert.Equal(1,first.Failures); s.DavMissing=true;
        var before = s.Methods.Count(x => x is "PROPFIND" or "MKCOL");
        var replay = await f.Executor(s).ResumeSessionAsync(first.SessionId,f.Request,cancellationToken:Ct);
        Assert.Equal(1,replay.PublishedFiles); Assert.Equal(0,replay.Failures);
        Assert.Equal(before,s.Methods.Count(x => x is "PROPFIND" or "MKCOL")); Assert.Equal(1,s.Publications);
    }

    [Fact]
    public async Task QualifiedProofWithoutBoundRawReceiptCannotBypassDurableStateAndAcceptedVersionCannotChange()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync(); using var s = new Saturn { LoseComplete=true };
        var first = await f.Executor(s).ScanAndTransferAsync(f.Request,cancellationToken:Ct);
        var work = Assert.Single(await f.Store.ListBootstrapWorkAsync(first.SessionId,cancellationToken:Ct));
        var forged = new BootstrapPublicationReceipt(work.OperationId,WebDavSyncClient.RelativeTargetUri(TargetRoot,work.RelativePath).AbsoluteUri,
            work.RelativePath,false,work.SealedSize!.Value,work.Sha256,$"\"sha256-{work.Sha256}\"",BootstrapPublicationProof.QualifiedImmutableReceipt);
        await Assert.ThrowsAsync<InvalidDataException>(() => InvokeAsync(f.Store,"FinishBootstrapWorkAsync",work,BootstrapWorkPhase.Published,JsonSerializer.Serialize(forged),null));
        Assert.NotEqual(BootstrapWorkPhase.Published,(await f.Store.ListBootstrapWorkAsync(first.SessionId,cancellationToken:Ct)).Single().Phase);
        var recovered = await f.Executor(s).ResumeSessionAsync(first.SessionId,f.Request,cancellationToken:Ct); Assert.Equal(1,recovered.PublishedFiles);
        var accepted = Assert.Single(await f.Store.ListBootstrapWorkAsync(first.SessionId,cancellationToken:Ct));
        var receipt = JsonSerializer.Deserialize<BootstrapPublicationReceipt>(accepted.Receipt!)!;
        var changed = receipt.ServerReceipt! with { VersionId=Guid.NewGuid().ToString() };
        var json = JsonNode.Parse(receipt.ImmutableReceiptJson!)!.AsObject(); json["versionId"]=changed.VersionId;
        var replaced = receipt with { ServerReceipt=changed,ImmutableReceiptJson=json.ToJsonString() };
        await Assert.ThrowsAsync<InvalidDataException>(() => InvokeAsync(f.Store,"FinishBootstrapWorkAsync",accepted,BootstrapWorkPhase.Published,JsonSerializer.Serialize(replaced),null));
        Assert.Equal(accepted.Receipt,(await f.Store.ListBootstrapWorkAsync(first.SessionId,cancellationToken:Ct)).Single().Receipt);
        var reordered = JsonSerializer.SerializeToElement(JsonNode.Parse(receipt.ImmutableReceiptJson!)).EnumerateObject().Reverse().ToDictionary(x => x.Name,x => x.Value);
        await InvokeAsync(f.Store,"FinishBootstrapWorkAsync",accepted,BootstrapWorkPhase.Published,JsonSerializer.Serialize(receipt with { ImmutableReceiptJson=JsonSerializer.Serialize(reordered) }),null);
        Assert.Equal(accepted.Receipt,(await f.Store.ListBootstrapWorkAsync(first.SessionId,cancellationToken:Ct)).Single().Receipt);
    }

    [Fact]
    public async Task DirectoryProofIsLegacyEvenWithRequiredFileReceiptPolicy()
    {
        await using var f = await DirectFixture.CreateAsync(); using var s = new Saturn();
        var work=f.Work with { IsDirectory=true,RelativePath="empty",SealedSize=0,SpoolPath=null,Sha256=null };
        var receipt=await f.TransferAsync(s,policy:BootstrapReceiptPolicy.RequireImmutable,work:work);
        Assert.True(receipt.IsDirectory); Assert.Equal(BootstrapPublicationProof.LegacyObservedActive,receipt.Proof);
        Assert.False(receipt.ImmutablePublicationAuthority); Assert.DoesNotContain("CREATE",s.Methods);
    }

    [Theory]
    [InlineData("schema")][InlineData("uploadId")][InlineData("operationId")][InlineData("localOperationKey")]
    [InlineData("intentHash")][InlineData("rootId")][InlineData("resourceId")][InlineData("versionId")][InlineData("parentId")]
    [InlineData("sha256")][InlineData("uppercaseHash")][InlineData("sizeBytes")][InlineData("numericSize")]
    [InlineData("leadingZero")][InlineData("negativeSize")][InlineData("overflow")][InlineData("fraction")][InlineData("exponent")]
    [InlineData("name")][InlineData("unsafeName")][InlineData("caseName")][InlineData("etag")][InlineData("publishedAt")]
    [InlineData("localTime")][InlineData("missingField")][InlineData("extraField")][InlineData("duplicateField")]
    [InlineData("missingReceipt")][InlineData("outerHash")][InlineData("offset")][InlineData("id")]
    public async Task MalformedOrWrongHistoricalReceiptNeverQualifies(string defect)
    {
        await using var f = await DirectFixture.CreateAsync(); using var s = new Saturn();
        s.Mutate = (stage,json) =>
        {
            if (stage != "COMPLETE") return null;
            var r = json["publicationReceipt"]!.AsObject();
            switch (defect)
            {
                case "localOperationKey": r["operationId"] = f.Work.OperationId; break;
                case "rootId": case "uploadId": case "operationId": r[defect] = Guid.NewGuid().ToString(); break;
                case "resourceId": case "versionId": case "parentId": r[defect] = "00000000-0000-0000-0000-000000000000"; break;
                case "intentHash": case "sha256": r[defect] = new string('b',64); break;
                case "uppercaseHash": r["sha256"] = f.Work.Sha256!.ToUpperInvariant(); break;
                case "sizeBytes": r[defect] = "999"; break;
                case "numericSize": r["sizeBytes"] = 3; break;
                case "leadingZero": r["sizeBytes"] = "03"; break;
                case "negativeSize": r["sizeBytes"] = "-3"; break;
                case "overflow": r["sizeBytes"] = "9223372036854775808"; break;
                case "fraction": r["sizeBytes"] = "3.0"; break;
                case "exponent": r["sizeBytes"] = "3e0"; break;
                case "name": r[defect] = "other.txt"; break;
                case "caseName": r["name"] = "File.txt"; break;
                case "unsafeName": r["name"] = "bad/name"; break;
                case "etag": r[defect] = "unquoted"; break;
                case "publishedAt": r[defect] = "2026-02-30T18:00:00.000Z"; break;
                case "localTime": r["publishedAt"] = "2026-10-12T18:00:00.000+03:00"; break;
                case "missingField": r.Remove("parentId"); break;
                case "extraField": r["rootEpoch"] = "not-supported"; break;
                case "duplicateField": return json.ToJsonString().Replace("\"schema\":", "\"schema\":\"saturn.upload-publication.v1\",\"schema\":",StringComparison.Ordinal);
                case "missingReceipt": json.Remove("publicationReceipt"); break;
                case "outerHash": json["intentHash"] = new string('b',64); break;
                case "offset": json["receivedSize"] = 0; break;
                case "id": json["id"] = Guid.NewGuid().ToString(); break;
                default: r[defect] = "unsupported"; break;
            }
            return null;
        };
        var error = await Record.ExceptionAsync(() => f.TransferAsync(s));
        Assert.IsType<InvalidDataException>(error); Assert.Equal(1,s.Publications);
        Assert.DoesNotContain("PUT",s.Methods); Assert.DoesNotContain("DELETE",s.Methods);
    }

    [Theory]
    [InlineData("missingState")][InlineData("nullState")][InlineData("missingHash")][InlineData("badHash")]
    [InlineData("duplicate")][InlineData("tooBig")][InlineData("tooDeep")]
    public async Task BadCreateReplyFailsBeforeFirstPatch(string defect)
    {
        await using var f = await DirectFixture.CreateAsync(); using var s = new Saturn();
        s.Mutate = (stage,json) =>
        {
            if (stage != "CREATE") return null;
            switch (defect)
            {
                case "missingState": json.Remove("receiptState"); break;
                case "nullState": json["receiptState"] = null; break;
                case "missingHash": json.Remove("intentHash"); break;
                case "badHash": json["intentHash"] = "bad"; break;
                case "duplicate": return json.ToJsonString().Replace("\"id\":", "\"id\":\"duplicate\",\"id\":",StringComparison.Ordinal);
                case "tooBig": json["other"] = new string('x',5000); break;
                case "tooDeep": return json.ToJsonString()[..^1]+",\"other\":[[[[[[[[[[1]]]]]]]]]]}";
            }
            return null;
        };
        Assert.NotNull(await Record.ExceptionAsync(() => f.TransferAsync(s)));
        Assert.DoesNotContain("PATCH",s.Methods); Assert.Equal(0,s.Publications);
    }

    [Theory]
    [InlineData(true)][InlineData(false)]
    public async Task PreviouslySealedFingerprintCannotDisappearOrChangeDuringAppend(bool disappear)
    {
        await using var f = await DirectFixture.CreateAsync(); using var s = new Saturn();
        s.Mutate = (stage,json) =>
        {
            if (stage == "PATCH")
            {
                if (disappear) { json.Remove("receiptState"); json.Remove("intentHash"); }
                else json["intentHash"] = new string('b',64);
            }
            return null;
        };
        await Assert.ThrowsAsync<InvalidDataException>(() => f.TransferAsync(s));
        Assert.Equal(s.IntentHash,f.Checkpoints.Last().IntentHash); Assert.Equal(0,f.Checkpoints.Last().ReceivedBytes);
        Assert.DoesNotContain("COMPLETE",s.Methods);
    }

    [Fact]
    public async Task ExplicitPoliciesRetainLegacyAndRequireImmutableStopsBeforeData()
    {
        await using var f = await DirectFixture.CreateAsync(); using var s = new Saturn { Extensions = false };
        var optional = await f.TransferAsync(s);
        Assert.Equal(BootstrapPublicationProof.LegacyObservedActive,optional.Proof); Assert.Null(optional.ServerReceipt);
        using var required = new Saturn { Extensions = false };
        await Assert.ThrowsAsync<BootstrapPublicationUncertainException>(() => f.TransferAsync(required,policy:BootstrapReceiptPolicy.RequireImmutable));
        Assert.DoesNotContain("PATCH",required.Methods); Assert.NotNull(f.Checkpoints.Last().UploadId);
        using var defaultServer = new Saturn();
        var legacy = await f.TransferAsync(defaultServer,policy:BootstrapReceiptPolicy.LegacyOnly);
        Assert.Equal(BootstrapPublicationProof.LegacyObservedActive,legacy.Proof);
        using var noEndpoint = new Saturn { NoEndpoint = true };
        await Assert.ThrowsAsync<HttpRequestException>(() => f.TransferAsync(noEndpoint,policy:BootstrapReceiptPolicy.RequireImmutable,allowPut:true));
        Assert.DoesNotContain("PUT",noEndpoint.Methods);
    }

    [Fact]
    public async Task PendingCommittingIsUnknownAndCheckpointFailureBlocksPatch()
    {
        await using var f = await DirectFixture.CreateAsync(); using var s = new Saturn();
        s.Mutate = (stage,json) => { if (stage == "CREATE") json["status"] = "committing"; return null; };
        await Assert.ThrowsAsync<BootstrapPublicationUncertainException>(() => f.TransferAsync(s)); Assert.DoesNotContain("PATCH",s.Methods);
        using var next = new Saturn();
        await Assert.ThrowsAsync<IOException>(() => f.TransferAsync(next,checkpoint:p => p.UploadId is not null ? throw new IOException("checkpoint disk failed") : Task.CompletedTask));
        Assert.DoesNotContain("PATCH",next.Methods);
    }

    [Fact]
    public async Task RootAndPolicyRebindingAreRejectedBeforeHttpAndMalformedUuidCannotConfigureAuthority()
    {
        await using var f = await DirectFixture.CreateAsync(); using var s = new Saturn();
        var transport = new CreateOnlyWebDavBootstrapTransport(new(s.Http),TargetRoot,"token",receiptExpectation:new(Guid.NewGuid().ToString()));
        await Assert.ThrowsAsync<InvalidDataException>(() => transport.TransferAsync(f.Work,_ => Task.CompletedTask,Ct)); Assert.Empty(s.Methods);
        Assert.Throws<ArgumentException>(() => new CreateOnlyWebDavBootstrapTransport(new(s.Http),TargetRoot,"token",receiptExpectation:new("not-a-root-id")));
    }

    [Fact]
    public async Task MaximalValidNameFitsBoundAndCurrentV2DoesNotAffectQualifiedV1()
    {
        await using var f = await DirectFixture.CreateAsync(); using var s = new Saturn { CurrentV2 = true };
        var work = f.Work with { RelativePath = new string('\u0800',255), ReceiptRequestPath = null };
        var receipt = await f.TransferAsync(s,work:work);
        Assert.Equal(BootstrapPublicationProof.QualifiedImmutableReceipt,receipt.Proof);
        Assert.Equal(work.RelativePath,receipt.ServerReceipt!.Name); Assert.True(Encoding.UTF8.GetByteCount(s.LastReply!) <= 4096);
        Assert.NotEqual(s.V2Etag,receipt.Etag);
    }

    [Fact]
    public async Task CancelAfterServerCommitRetainsOriginalBindingAndRecoversHistoricalOutcome()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync(); using var cancel = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        using var s = new Saturn { AfterComplete = () => cancel.Cancel() };
        var first = await f.Executor(s).ScanAndTransferAsync(f.Request,cancellationToken:cancel.Token);
        Assert.True(first.Cancelled);
        var before = Assert.Single(await f.Store.ListBootstrapWorkAsync(first.SessionId,cancellationToken:Ct));
        Assert.Equal(s.IntentHash,before.OriginalIntentHash); Assert.Equal(s.UploadId,before.UploadId);
        var replay = await f.Executor(s).ResumeSessionAsync(first.SessionId,f.Request,cancellationToken:Ct);
        Assert.Equal(1,replay.PublishedFiles); Assert.Equal(1,s.Publications);
    }

    [Fact]
    public async Task LegacyV1MigrationKeepsOldWorkUnqualifiedAndIncludesWalUserDataInBackup()
    {
        await using var f = await Fixture.CreateAsync();
        await using var wal = new SqliteConnection("Data Source="+f.Database+";Pooling=False"); await wal.OpenAsync(Ct);
        await using var q = wal.CreateCommand(); q.CommandText = LegacySchema + "; PRAGMA wal_autocheckpoint=0; CREATE TABLE receipt_user_payload(value TEXT); INSERT INTO receipt_user_payload VALUES('WAL user data'); INSERT INTO bootstrap_work_sessions VALUES('old','old-scan',1,'mapping','client','source','https://saturn.test/dav/sync/Audit/Mapping/','scope',0); INSERT INTO bootstrap_work_items(work_id,session_id,entry_id,operation_id,relative_path,kind,enumerated_size,modified_at,phase,upload_id,received_bytes,attempt,receipt) VALUES(1,'old',1,'old-key','file',0,3,'2026-10-12T00:00:00.000Z',4,'671c88f7-4ef9-4c5e-a2de-8b294135fc4e',3,1,'original legacy receipt'); UPDATE sync_mappings SET enabled=0;";
        await q.ExecuteNonQueryAsync(Ct);
        await InvokeAsync(f.Store,"EnsureBootstrapWorkSchemaAsync",Ct);
        Assert.Equal(2L,await f.ScalarAsync("SELECT version FROM bootstrap_work_format"));
        var old = Assert.Single(await f.Store.ListBootstrapWorkAsync("old",cancellationToken:Ct));
        Assert.Equal(BootstrapReceiptPolicy.LegacyOnly,old.ReceiptPolicy); Assert.Null(old.ExpectedServerRootId); Assert.Null(old.OriginalIntentHash);
        Assert.Equal("original legacy receipt",old.Receipt); Assert.Equal(3,old.ReceivedBytes); Assert.Equal(0L,await f.ScalarAsync("SELECT enabled FROM sync_mappings"));
        var copy = await f.Store.CreatePreBootstrapReceiptRollbackCopyAsync(Path.Combine(f.Root,"pre-v2.db"),Ct);
        await using var backup = new SqliteConnection("Data Source="+copy+";Mode=ReadOnly;Pooling=False"); await backup.OpenAsync(Ct);
        await using var verify = backup.CreateCommand(); verify.CommandText = "SELECT value FROM receipt_user_payload"; Assert.Equal("WAL user data",await verify.ExecuteScalarAsync(Ct));
        verify.CommandText = "SELECT version FROM bootstrap_work_format"; Assert.Equal(1L,await verify.ExecuteScalarAsync(Ct));
        await InvokeAsync(f.Store,"EnsureBootstrapWorkSchemaAsync",Ct);
        Assert.Equal("original legacy receipt",(await f.Store.ListBootstrapWorkAsync("old",cancellationToken:Ct)).Single().Receipt);
        var output = Environment.GetEnvironmentVariable("NEPTUNE_RECEIPT_OLD_READER_FIXTURE");
        if (output is not null)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(output)!);
            await using var export = new SqliteConnection("Data Source="+output+";Pooling=False"); await export.OpenAsync(Ct); wal.BackupDatabase(export);
        }
    }

    [Fact]
    public async Task InterruptedMigrationRollbackAndConcurrentInitializersAreIdempotent()
    {
        await using var f = await Fixture.CreateAsync(); await f.ExecuteAsync(LegacySchema);
        await using (var c = new SqliteConnection("Data Source="+f.Database+";Pooling=False"))
        {
            await c.OpenAsync(Ct); using var tx = c.BeginTransaction(); await using var q = c.CreateCommand(); q.Transaction=tx;
            q.CommandText="ALTER TABLE bootstrap_work_items ADD COLUMN intent_hash TEXT"; await q.ExecuteNonQueryAsync(Ct); tx.Rollback();
        }
        using var cancel = new CancellationTokenSource(); cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => InvokeAsync(f.Store,"EnsureBootstrapWorkSchemaAsync",cancel.Token));
        Assert.Equal(1L,await f.ScalarAsync("SELECT version FROM bootstrap_work_format"));
        await Task.WhenAll(Enumerable.Range(0,3).Select(_ => InvokeAsync(new NeptuneStateStore(f.Database),"EnsureBootstrapWorkSchemaAsync",Ct)));
        Assert.Equal(2L,await f.ScalarAsync("SELECT version FROM bootstrap_work_format"));
        Assert.Single(Directory.GetFiles(f.Root,"*.pre-bootstrap-work-v2-*.db"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => f.Store.CreatePreBootstrapReceiptRollbackCopyAsync(f.Database,Ct));
    }

    [Theory]
    [InlineData(true)][InlineData(false)]
    public async Task FutureOrPartialReceiptSchemaRefusesBeforeWritableInitialization(bool future)
    {
        await using var f = await Fixture.CreateAsync(); await InvokeAsync(f.Store,"EnsureBootstrapWorkSchemaAsync",Ct);
        await using var wal = new SqliteConnection("Data Source="+f.Database+";Pooling=False"); await wal.OpenAsync(Ct);
        await using var q = wal.CreateCommand(); q.CommandText="PRAGMA wal_autocheckpoint=0; "+(future ? "UPDATE bootstrap_work_format SET version=99" : "ALTER TABLE bootstrap_work_items DROP COLUMN intent_hash"); await q.ExecuteNonQueryAsync(Ct);
        var db = await ReadSharedAsync(f.Database); var bytes = await ReadSharedAsync(f.Database+"-wal");
        await Assert.ThrowsAsync<InvalidDataException>(() => new NeptuneStateStore(f.Database).InitializeAsync(Ct));
        Assert.Equal(db,await ReadSharedAsync(f.Database)); Assert.Equal(bytes,await ReadSharedAsync(f.Database+"-wal"));
    }

    private static async Task InvokeAsync(NeptuneStateStore store,string name,params object?[] args)
    {
        try { await (Task)typeof(NeptuneStateStore).GetMethod(name,BindingFlags.Instance|BindingFlags.NonPublic)!.Invoke(store,args)!; }
        catch (TargetInvocationException e) when (e.InnerException is not null) { System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(e.InnerException).Throw(); }
    }
    private static async Task<byte[]> ReadSharedAsync(string path)
    { await using var stream = new FileStream(path,FileMode.Open,FileAccess.Read,FileShare.ReadWrite|FileShare.Delete); using var memory = new MemoryStream(); await stream.CopyToAsync(memory,Ct); return memory.ToArray(); }

    private sealed class DirectFixture : IAsyncDisposable
    {
        public string Root { get; } = Path.Combine(Path.GetTempPath(),"neptune-receipt-tests",Guid.NewGuid().ToString("N"));
        public BootstrapWorkRecord Work = null!; public List<BootstrapTransferCheckpoint> Checkpoints = [];
        public static async Task<DirectFixture> CreateAsync()
        {
            var f = new DirectFixture(); Directory.CreateDirectory(f.Root); var spool=Path.Combine(f.Root,"sealed.spool"); var bytes=Encoding.UTF8.GetBytes("abc"); await File.WriteAllBytesAsync(spool,bytes,Ct);
            using var http = new HttpClient(); var transport = new CreateOnlyWebDavBootstrapTransport(new(http),TargetRoot,"token",receiptExpectation:new(RootId));
            f.Work = new(1,"session","scan",1,1,new string('c',64),"mapping",f.Root,transport.TargetRoot.AbsoluteUri,transport.CredentialScope,
                "file.txt",false,bytes.Length,DateTimeOffset.UtcNow,BootstrapWorkPhase.Transferring,spool,bytes.Length,Convert.ToHexStringLower(SHA256.HashData(bytes)),"sealed",1,
                ClientInstanceId:"client",ExpectedServerRootId:RootId,ReceiptPolicy:BootstrapReceiptPolicy.ValidateWhenPresent,ReceiptRequestPath:"sync/Audit/Mapping/file.txt");
            return f;
        }
        public async Task<BootstrapPublicationReceipt> TransferAsync(Saturn server,BootstrapReceiptPolicy policy=BootstrapReceiptPolicy.ValidateWhenPresent,
            bool allowPut=false,BootstrapWorkRecord? work=null,Func<BootstrapTransferCheckpoint,Task>? checkpoint=null)
        {
            Checkpoints.Clear(); var expectation=policy == BootstrapReceiptPolicy.LegacyOnly ? null : new BootstrapReceiptExpectation(RootId,policy);
            var t = new CreateOnlyWebDavBootstrapTransport(new(server.Http),TargetRoot,"token",allowPut,expectation);
            var w = (work ?? Work) with { ReceiptPolicy=policy,ExpectedServerRootId=expectation?.AssignedRootId };
            return await t.TransferAsync(w,async p => { if (checkpoint is not null) await checkpoint(p); Checkpoints.Add(p); },Ct);
        }
        public ValueTask DisposeAsync() { Directory.Delete(Root,true); return ValueTask.CompletedTask; }
    }
    private sealed class Fixture : IAsyncDisposable
    {
        public string Root { get; } = Path.Combine(Path.GetTempPath(),"neptune-receipt-tests",Guid.NewGuid().ToString("N"));
        public string Source => Path.Combine(Root,"source"); public string Database => Path.Combine(Root,"profile.db");
        public NeptuneStateStore Store = null!;
        public BootstrapExecutionRequest Request => new("mapping","client",Source,Path.Combine(Root,"spool"),new(DiskFreeFloorBytes:0));
        public static async Task<Fixture> CreateAsync()
        {
            var f = new Fixture(); Directory.CreateDirectory(f.Source); f.Store=new(f.Database); await f.Store.InitializeAsync(Ct);
            await f.Store.AddMappingAsync(new("mapping","client",f.Source,true,DateTimeOffset.UtcNow),Ct); await File.WriteAllTextAsync(Path.Combine(f.Source,"file.txt"),"abc",Ct); return f;
        }
        public NeptuneBootstrapExecutor Executor(Saturn s,NeptuneStateStore? store=null)
            => new(store ?? Store,new CreateOnlyWebDavBootstrapTransport(new(s.Http,store ?? Store),TargetRoot,"token",receiptExpectation:new(RootId)));
        public async Task<object?> ScalarAsync(string sql) { await using var c = new SqliteConnection("Data Source="+Database+";Pooling=False"); await c.OpenAsync(Ct); await using var q=c.CreateCommand(); q.CommandText=sql; return await q.ExecuteScalarAsync(Ct); }
        public async Task ExecuteAsync(string sql) { await using var c = new SqliteConnection("Data Source="+Database+";Pooling=False"); await c.OpenAsync(Ct); await using var q=c.CreateCommand(); q.CommandText=sql; await q.ExecuteNonQueryAsync(Ct); }
        public ValueTask DisposeAsync() { SqliteConnection.ClearAllPools(); Directory.Delete(Root,true); return ValueTask.CompletedTask; }
    }
    private sealed class Saturn : HttpMessageHandler
    {
        public readonly HttpClient Http; public ConcurrentBag<string> Methods = [], Keys = [];
        public string UploadId=Guid.NewGuid().ToString(), IntentHash=new('a',64), VersionV1=Guid.NewGuid().ToString();
        public string V2Etag=$"\"sha256-{new string('b',64)}\""; public string? LastRawReceipt,LastReply;
        public bool Extensions=true,LoseCreate,LoseComplete,CurrentV2,NoEndpoint,DavMissing;
        public int Publications; public Func<string,JsonObject,string?>? Mutate; public Func<Task>? BeforePatch; public Action? AfterComplete;
        private string _path="",_sha=""; private long _size; private bool _active,_created; private MemoryStream _bytes=new();
        public Saturn() => Http=new(this,disposeHandler:false);
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,CancellationToken ct)
        {
            var path=request.RequestUri!.AbsolutePath;
            if (request.Method.Method=="PROPFIND") { Methods.Add("PROPFIND"); if (DavMissing) return new(HttpStatusCode.NotFound); return new((HttpStatusCode)207) { Content=new StringContent($"<d:multistatus xmlns:d='DAV:'><d:response><d:href>{path}</d:href><d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>") }; }
            if (request.Method.Method=="MKCOL") { Methods.Add("MKCOL"); return new(HttpStatusCode.Created); }
            var stage=request.Method==HttpMethod.Post && path=="/api/v1/sync/uploads" ? "CREATE" : request.Method==HttpMethod.Patch ? "PATCH" : "COMPLETE";
            Methods.Add(stage);
            if (stage=="CREATE")
            {
                if (NoEndpoint) return new(HttpStatusCode.NotFound);
                using var doc=JsonDocument.Parse(await request.Content!.ReadAsStringAsync(ct)); var v=doc.RootElement;
                Assert.Equal("*",v.GetProperty("ifNoneMatch").GetString()); Assert.False(v.TryGetProperty("ifMatch",out _));
                Keys.Add(v.GetProperty("idempotencyKey").GetString()!);
                if (!_created) { _created=true; _path=v.GetProperty("path").GetString()!; _sha=v.GetProperty("expectedSha256").GetString()!; _size=v.GetProperty("expectedSize").GetInt64(); }
                if (LoseCreate) { LoseCreate=false; throw new HttpRequestException("lost original create acknowledgement"); }
            }
            else if (stage=="PATCH")
            {
                if (BeforePatch is not null) await BeforePatch();
                Assert.Equal(_bytes.Length.ToString(CultureInfo.InvariantCulture),request.Headers.GetValues("Upload-Offset").Single());
                await request.Content!.CopyToAsync(_bytes,ct);
            }
            else
            {
                if (!_active) { _active=true; Publications++; Assert.Equal(_size,_bytes.Length); Assert.Equal(_sha,Convert.ToHexStringLower(SHA256.HashData(_bytes.ToArray()))); }
                AfterComplete?.Invoke();
                if (LoseComplete) { LoseComplete=false; throw new HttpRequestException("lost successful V1 completion response"); }
            }
            var json=new JsonObject { ["id"]=UploadId,["status"]=_active ? "active" : "uploading",["receivedSize"]=_bytes.Length,["expectedSize"]=_size,["etag"]=_active ? CurrentV2 ? V2Etag : $"\"sha256-{_sha}\"" : null };
            if (Extensions)
            {
                json["receiptState"]=_active ? "published" : "pending"; json["intentHash"]=IntentHash;
                if (_active)
                {
                    json["publicationReceipt"]=new JsonObject { ["schema"]="saturn.upload-publication.v1",["uploadId"]=UploadId,["operationId"]=UploadId,["intentHash"]=IntentHash,["rootId"]=RootId,
                        ["resourceId"]="2226c4b9-6513-4f9a-bf0d-c04b2593ac86",["versionId"]=VersionV1,["parentId"]="b0332635-5725-4fa4-a9c6-4ebf878c82db",
                        ["name"]=Uri.UnescapeDataString(_path.Split('/')[^1]),["sizeBytes"]=_size.ToString(CultureInfo.InvariantCulture),["sha256"]=_sha,["etag"]=$"\"sha256-{_sha}\"",["publishedAt"]="2026-10-12T18:00:00.000Z" };
                }
            }
            var overridden=Mutate?.Invoke(stage,json); LastRawReceipt=json["publicationReceipt"]?.ToJsonString(); LastReply=overridden ?? json.ToJsonString();
            return new(HttpStatusCode.OK) { Content=new StringContent(LastReply,Encoding.UTF8,"application/json") };
        }
        protected override void Dispose(bool disposing) { if (disposing) { Http.Dispose(); _bytes.Dispose(); } base.Dispose(disposing); }
    }
    private const string LegacySchema = """
        CREATE TABLE bootstrap_work_format(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL);
        CREATE TABLE bootstrap_work_sessions(session_id TEXT PRIMARY KEY,scan_id TEXT NOT NULL,generation INTEGER NOT NULL,mapping_id TEXT NOT NULL,client_instance_id TEXT NOT NULL,source_root TEXT NOT NULL,target_root TEXT NOT NULL,credential_scope TEXT NOT NULL,cursor INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE bootstrap_work_items(work_id INTEGER PRIMARY KEY AUTOINCREMENT,session_id TEXT NOT NULL,entry_id INTEGER NOT NULL,operation_id TEXT NOT NULL UNIQUE,relative_path TEXT NOT NULL,kind INTEGER NOT NULL,enumerated_size INTEGER NOT NULL,modified_at TEXT NOT NULL,phase INTEGER NOT NULL,spool_path TEXT,sealed_size INTEGER,sha256 TEXT,source_identity TEXT,attempt INTEGER NOT NULL DEFAULT 0,transport_mode TEXT,upload_id TEXT,received_bytes INTEGER NOT NULL DEFAULT 0,receipt TEXT,error TEXT,UNIQUE(session_id,entry_id),FOREIGN KEY(session_id) REFERENCES bootstrap_work_sessions(session_id) ON DELETE CASCADE);
        CREATE INDEX bootstrap_work_queue ON bootstrap_work_items(session_id,phase,work_id);
        INSERT INTO bootstrap_work_format VALUES(1,1);
        """;
}
