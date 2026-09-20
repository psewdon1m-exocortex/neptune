using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class ResourceReaderContractTests
{
    [Theory]
    [InlineData("root")]
    [InlineData("root/Projects/Folder with spaces/Файл.md")]
    public void CanonicalPathsRemainExact(string value) => Assert.Equal(value, ResourceReaderContract.Path(value));

    [Theory]
    [InlineData("/root/file")]
    [InlineData("root/../secret")]
    [InlineData("root/%2e%2e/file")]
    [InlineData("root/%252e%252e/file")]
    [InlineData("root\\file")]
    [InlineData("root//file")]
    [InlineData("root/file\0")]
    [InlineData("C:/private/file")]
    public void UnsafePathsAreRejected(string value) => Assert.Throws<ArgumentException>(() => ResourceReaderContract.Path(value));

    [Fact]
    public void SubtreeUsesSegmentBoundary()
    {
        Assert.Equal("root/project/file", ResourceReaderContract.Path("root/project/file", "root/project"));
        Assert.Throws<UnauthorizedAccessException>(() => ResourceReaderContract.Path("root/project-other/file", "root/project"));
        Assert.Throws<UnauthorizedAccessException>(() => ResourceReaderContract.Path("root", "root/project"));
    }

    [Theory]
    [InlineData("shared")]
    [InlineData("crusher-public")]
    [InlineData("")]
    public void PublicPurposesCannotUseOwnerReader(string purpose) => Assert.Throws<UnauthorizedAccessException>(() => ResourceReaderContract.Purpose(purpose));

    [Fact]
    public void BoundsAreExplicit()
    {
        Assert.Equal(100, ResourceReaderContract.Page(100));
        Assert.Throws<ArgumentException>(() => ResourceReaderContract.Page(101));
        ResourceReaderContract.Range("bytes=0-42");
        ResourceReaderContract.Range("bytes=-100");
        ResourceReaderContract.Range("bytes=100-");
        Assert.Throws<ArgumentException>(() => ResourceReaderContract.Range("bytes=0-1,2-3"));
    }
}
