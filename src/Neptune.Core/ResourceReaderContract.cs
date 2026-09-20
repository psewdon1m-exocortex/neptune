using System.Text.RegularExpressions;

namespace Neptune.Core;

public sealed record ReaderRegistration(string SaturnTokenFile, string Root = "root")
{
    public ReaderRegistration Validate()
    {
        if (string.IsNullOrWhiteSpace(SaturnTokenFile) || !Path.IsPathFullyQualified(SaturnTokenFile))
            throw new ArgumentException("The reader credential must be an absolute protected-file reference.");
        ResourceReaderContract.Path(Root);
        return this;
    }
}

public static partial class ResourceReaderContract
{
    public const string Capability = "neptune.resource-reader.v1";
    public const int MaximumPage = 100;
    public const int MaximumMetadataBytes = 1024 * 1024;
    public const int StreamBufferBytes = 256 * 1024;

    [GeneratedRegex("%[0-9a-fA-F]{2}", RegexOptions.CultureInvariant)]
    private static partial Regex EncodedOctet();

    public static string Path(string? value, string root = "root")
    {
        if (value is null || System.Text.Encoding.UTF8.GetByteCount(value) > 4096 || (value != "root" && !value.StartsWith("root/", StringComparison.Ordinal))
            || value.Contains('\\') || value.Contains(':') || value.Any(c => char.IsControl(c)) || EncodedOctet().IsMatch(value))
            throw new ArgumentException("A canonical Saturn logical path is required.");
        var parts = value.Split('/');
        if (parts.Length > 64 || parts.Any(p => p.Length is 0 or > 255 || p is "." or ".."))
            throw new ArgumentException("A canonical Saturn logical path is required.");
        if (value != root && !value.StartsWith(root + "/", StringComparison.Ordinal))
            throw new UnauthorizedAccessException("The resource is outside the registered reader subtree.");
        return value;
    }

    public static void Purpose(string? purpose)
    {
        if (purpose is not ("owner-reference" or "owner-crusher"))
            throw new UnauthorizedAccessException("The reader purpose is not permitted.");
    }

    public static int Page(int limit)
    {
        if (limit is < 1 or > MaximumPage) throw new ArgumentException("Reader page size must be between 1 and 100.");
        return limit;
    }

    public static void Range(string? value)
    {
        if (value is not null && (value.Length > 100 || !SingleRange().IsMatch(value)))
            throw new ArgumentException("Only a single byte range is supported.");
    }

    [GeneratedRegex("^bytes=([0-9]+-[0-9]*|-[0-9]+)\\z", RegexOptions.CultureInvariant)]
    private static partial Regex SingleRange();
}
