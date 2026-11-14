namespace Neptune.Core;

// An explicit new observed subscription replaces a LOST interval; it never
// repairs, resets or certifies the old interval as continuous.
public enum LocalJournalReplacementStatus { SourceOpened, LocalQualified, RemoteQualified, Installed, NeedsAttention }
public sealed record JournalGapReplacementRequest(string ReplacementId,string OriginalScopeId,string? SupersedesReplacementId=null);
public sealed record LocalJournalReplacement(string ReplacementId,string OriginalScopeId,string CurrentScopeId,
    LocalJournalReplacementStatus Status,string OriginalJournalId,long OriginalCursor,string OriginalGap,
    string CurrentJournalId,long CurrentStartUsn,long SubscriptionGeneration,int CandidateAttempts,string? Error);
internal sealed record LocalJournalReplacementOpen(LocalSourceJournalSession Session,LocalJournalReplacement Replacement,string? CoherenceId=null);
