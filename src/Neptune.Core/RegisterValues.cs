using System.Text.Json;

namespace Neptune.Core;

public static class RegisterValues
{
    public static string RequiredString(JsonElement values, string dottedKey)
    {
        var current = values;
        foreach (var segment in dottedKey.Split('.', StringSplitOptions.RemoveEmptyEntries))
        {
            if (current.ValueKind != JsonValueKind.Object || !current.TryGetProperty(segment, out current))
                throw new InvalidDataException($"Kernel Register key '{dottedKey}' is missing.");
        }

        return current.ValueKind switch
        {
            JsonValueKind.String => current.GetString()!,
            JsonValueKind.Number => current.GetRawText(),
            _ => throw new InvalidDataException($"Kernel Register key '{dottedKey}' is not a scalar string.")
        };
    }

    public static Uri HttpsOrigin(JsonElement values, string serviceName)
    {
        var host = RequiredString(values, $"services.{serviceName}.sni");
        var port = int.Parse(RequiredString(values, $"services.{serviceName}.port"), System.Globalization.CultureInfo.InvariantCulture);
        return new UriBuilder(Uri.UriSchemeHttps, host, port).Uri;
    }
}

