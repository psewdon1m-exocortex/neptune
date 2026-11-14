namespace Neptune.Core;

public sealed class LocalNamespaceScanner(ILocalNamespaceSource source)
{
    // A page/handle is live only until the parent finishes. Any crash/cancel
    // restarts a fresh candidate, never a persisted NTFS FileIndex position.
    public async Task<LocalNamespaceCandidate> ScanAsync(LocalSourceJournalSession session,
        LocalNamespaceAdmission? admission=null,CancellationToken cancellationToken=default)
    {
        var candidate=await session.BeginNamespaceCandidateAsync(source.RootProof,admission,cancellationToken);
        try
        {
            while(true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                while(await session.RefillNamespaceFrontierAsync(candidate.CandidateId,cancellationToken) is not null)cancellationToken.ThrowIfCancellationRequested();
                var parent=await session.ClaimNamespaceDirectoryAsync(candidate.CandidateId,cancellationToken);if(parent is null)break;
                await using var reader=await source.OpenDirectoryAsync(parent,cancellationToken);
                await session.RecordNamespaceHandleAsync(parent,reader.Proof,cancellationToken);
                while(true)
                {
                    var page=await reader.ReadPageAsync(cancellationToken);
                    if(page.Entries.Count>WindowsDirectoryPageParser.MaximumRecords)throw new InvalidDataException("Source adapter exceeded bounded metadata page size.");
                    var capacity=session.NamespaceStageCapacity;
                    for(var i=0;i<page.Entries.Count;i+=capacity)
                        await session.StageNamespaceEntriesAsync(parent,page.Entries.Skip(i).Take(capacity).ToArray(),cancellationToken);
                    if(page.End)break;
                }
                await reader.ValidateUnchangedAsync(cancellationToken);
                await session.CompleteNamespaceDirectoryAsync(parent,cancellationToken);
            }
            await session.FinishNamespaceAsync(candidate.CandidateId,LocalNamespaceStatus.EnumerationComplete,cancellationToken:cancellationToken);
        }
        catch(Exception error)
        {
            var message=error is OperationCanceledException?"Candidate cancelled; fresh generation required.":error.Message;
            if(message.Length>1024)message=message[..1024];
            try{await session.FinishNamespaceAsync(candidate.CandidateId,LocalNamespaceStatus.Incomplete,message,CancellationToken.None);}
            catch(InvalidOperationException){/* A newer owner/candidate is already fenced; never downgrade it. */}
            throw;
        }
        return await session.ReadNamespaceCandidateAsync(candidate.CandidateId,cancellationToken);
    }
}
