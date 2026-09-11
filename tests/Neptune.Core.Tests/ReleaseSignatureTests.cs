using Xunit;

namespace Neptune.Core.Tests;

public sealed class ReleaseSignatureTests
{
    [Fact]
    public void NodeProducerSignatureIsVerifiedAndTamperingRejected()
    {
        var directory = Path.Combine(AppContext.BaseDirectory, "TestData");
        var manifest = File.ReadAllBytes(Path.Combine(directory, "manifest.json"));
        var envelope = File.ReadAllBytes(Path.Combine(directory, "manifest.json.sig.json"));
        var key = File.ReadAllText(Path.Combine(directory, "public.pem"));
        ReleaseSignature.Verify(manifest, envelope, key);
        manifest[10] ^= 1;
        Assert.Throws<InvalidDataException>(() => ReleaseSignature.Verify(manifest, envelope, key));
    }
}
