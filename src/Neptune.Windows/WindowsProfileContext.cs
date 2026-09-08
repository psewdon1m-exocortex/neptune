using System.Text.RegularExpressions;

namespace Neptune.Windows;

public sealed partial record WindowsProfileContext(string ProfileId, string StateDirectory)
{
    public static WindowsProfileContext FromArguments(string[] arguments)
    {
        var index = Array.FindIndex(arguments, item => item == "--profile");
        var profile = index >= 0 && index + 1 < arguments.Length ? arguments[index + 1] : "default";
        if (!ProfilePattern().IsMatch(profile))
            throw new ArgumentException("Profile must contain only ASCII letters, digits, '-' or '_'.");
        profile = profile.ToLowerInvariant();
        var root = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        return new WindowsProfileContext(profile, Path.Combine(root, "Neptune", "profiles", profile));
    }

    [GeneratedRegex("^[A-Za-z0-9_-]{1,40}$", RegexOptions.CultureInvariant)]
    private static partial Regex ProfilePattern();
}
