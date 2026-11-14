namespace Neptune.Core;

// Received means a validated server ACK, not bytes merely read into a socket.
public sealed record UploadTransferProgress(long ReceivedBytes,long TotalBytes,bool Initial);
