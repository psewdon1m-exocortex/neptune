using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using Neptune.Core;

namespace Neptune.Linux;

public sealed class PolicyProtocolException(int statusCode, string message) : Exception(message)
{
    public int StatusCode { get; } = statusCode;
}

// Local control-token authorization selects the registration. Its producer
// credential selects the same immutable scope at the authoritative backend.
public sealed class ServicePolicyClient(
    LinuxOptions options, ProjectRegistry registry, IHttpClientFactory clients)
{
    public async Task<JsonNode> RequestAsync(ProjectRegistration project, HttpMethod method,
        string suffix = "", JsonNode? body = null, CancellationToken cancellationToken = default)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(20));
        var token = timeout.Token;
        var http = clients.CreateClient("neptune");
        var kernelToken = (await File.ReadAllTextAsync(options.KernelTokenFile, token)).Trim();
        using var snapshot = await new KernelRegisterClient(http, Path.Combine(options.StateDirectory, "register-lkg.json"))
            .GetSnapshotAsync(options.KernelOrigin, kernelToken, token, ["services.saturn.sni", "services.saturn.port"]);
        var origin = RegisterValues.HttpsOrigin(snapshot.RootElement.GetProperty("values"), "saturn");
        if (method == HttpMethod.Put && body?["kind"]?.ToString() == "resume")
        {
            var exportToken = (await File.ReadAllTextAsync(project.ExportTokenFile, token)).Trim();
            await VerifySourceAsync(http, project.ExportUri, exportToken, token);
            if (project.Mirror is not null)
            {
                await VerifySourceAsync(http, project.Mirror.ExportUri, exportToken, token);
                using var mirrorRequest = new HttpRequestMessage(HttpMethod.Head, new Uri(origin, "/dav/" + project.Mirror.SaturnRoot + "/"));
                mirrorRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer",
                    (await File.ReadAllTextAsync(project.Mirror.SaturnTokenFile, token)).Trim());
                using var mirrorResponse = await http.SendAsync(mirrorRequest, HttpCompletionOption.ResponseHeadersRead, token);
                if (!mirrorResponse.IsSuccessStatusCode) throw new PolicyProtocolException(409, "Mirror destination verification failed; restored policy remains paused");
            }
        }
        using var request = new HttpRequestMessage(method, new Uri(origin, "/api/v1/neptune/agent/policy" + suffix));
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer",
            (await File.ReadAllTextAsync(project.SaturnTokenFile, token)).Trim());
        if (body is not null) request.Content = JsonContent.Create(body);
        using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);
        await using var input = await response.Content.ReadAsStreamAsync(token);
        using var output = new MemoryStream();
        var buffer = new byte[8192];
        int count;
        while ((count = await input.ReadAsync(buffer, token)) != 0)
        {
            if (output.Length + count > 256 * 1024) throw new InvalidDataException("Policy response exceeds its limit");
            output.Write(buffer, 0, count);
        }
        JsonNode? result = null;
        try
        {
            if (output.Length > 0) result = JsonNode.Parse(output.ToArray());
        }
        catch (JsonException) when (!response.IsSuccessStatusCode) { }
        if (!response.IsSuccessStatusCode)
        {
            if (response.StatusCode == HttpStatusCode.NotFound)
                throw new PolicyProtocolException(426,
                    "Saturn does not provide the service-owned backup policy protocol. Update Saturn before Neptune, then retry.");
            var error = result as JsonObject;
            var candidate = error?["message"] ?? error?["error"];
            var message = candidate?.GetValueKind() == JsonValueKind.String
                ? candidate.GetValue<string>() : "Backup policy request failed";
            throw new PolicyProtocolException((int)response.StatusCode, message.Length > 512 ? message[..512] : message);
        }
        if (result is null) throw new InvalidDataException("Empty policy response");
        if (suffix.Length == 0)
        {
            if (result["schema"]?.GetValue<string>() != "exocortex.backup.policy.v1")
                throw new InvalidDataException("Upgrade Saturn: service-owned backup policy protocol is required");
            if ((result["mirror"] is not null) != (project.Mirror is not null))
                throw new InvalidDataException("Enrolled backup policy and local pipeline profile differ");
            if (method == HttpMethod.Put)
            {
                await registry.ApplyRemoteDesiredAsync(project.ProjectId, result["revision"]!.GetValue<long>(),
                    result["archive"]!["enabled"]!.GetValue<bool>(), result["archive"]!["intervalHours"]!.GetValue<int>(),
                    result["mirror"]?["enabled"]?.GetValue<bool>() ?? false,
                    result["mirror"]?["intervalMinutes"]?.GetValue<int>() ?? 1440,
                    cancellationToken: token, paused: result["paused"]?.GetValue<bool>() ?? false);
                result["appliedRevision"] = (await registry.FindAsync(project.ProjectId, token))!.ControlRevision;
            }
        }
        return result;
    }

    private static async Task VerifySourceAsync(HttpClient http, Uri uri, string credential, CancellationToken token)
    {
        using var request = new HttpRequestMessage(HttpMethod.Head, uri);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", credential);
        using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);
        if (!response.IsSuccessStatusCode || !response.Headers.TryGetValues("X-Neptune-Ready", out var values) || !values.Contains("1"))
            throw new PolicyProtocolException(409, "Backup source is not verified; update or repair the service before resuming its restored policy");
    }

    public static async Task<JsonNode> ReadBodyAsync(Stream input, CancellationToken token)
    {
        using var output = new MemoryStream();
        var buffer = new byte[4096];
        int count;
        while ((count = await input.ReadAsync(buffer, token)) != 0)
        {
            if (output.Length + count > 16384) throw new PolicyProtocolException(413, "Policy request is too large");
            output.Write(buffer, 0, count);
        }
        var result = JsonNode.Parse(output.ToArray());
        if (result is not JsonObject) throw new PolicyProtocolException(400, "A policy object is required");
        return result;
    }
}
