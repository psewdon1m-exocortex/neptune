namespace Neptune.Core;

public static class RegisterFallback
{
    public static bool CanUse(Exception error) => error switch
    {
        HttpRequestException request => request.StatusCode is null || (int)request.StatusCode >= 500,
        IOException => true,
        OperationCanceledException => true,
        _ => false,
    };
}
