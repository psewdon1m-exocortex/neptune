namespace Neptune.Core;
public sealed partial class NeptuneStateStore
{
    public async Task AdoptClientProfileAsync(CancellationToken ct=default)
    {await using var owner=new FileStream(Path.GetFullPath(databasePath)+".local-source-owner.lock",FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None);await InitializeAsync(ct);if(await PreflightCycleAsync(ct)is 4 or 5 or 6 or 7)await EnsureClientEvolutionAsync(ct);}
    // Read-only compatibility probe for an offline update health/adoption plan.
    public async Task ValidateProfileCompatibilityAsync(CancellationToken ct=default)
    {
        await PreflightSyncStateAsync(false,ct);await PreflightLocalSourceAsync(ct);
        await ValidateOptionalLocalNamespaceBeforeInitializeAsync(ct);
        await ValidateOptionalLocalProjectionBeforeInitializeAsync(ct);
        await ValidateOptionalLocalCoherenceBeforeInitializeAsync(ct);
        await ValidateOptionalLocalCycleBeforeInitializeAsync(ct);
        await ValidateOptionalParentReconcileBeforeInitializeAsync(ct);
    }
}
