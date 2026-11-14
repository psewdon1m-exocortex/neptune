namespace Neptune.Core;

// Registered retained spools count even when their mapping is disabled. This
// complements each mapping's disk floor and immutable spool ownership checks.
public static class WindowsCycleSharedBudget
{
    private static readonly object Gate=new();
    private static readonly Dictionary<string,long> Files=new(StringComparer.OrdinalIgnoreCase);
    private const long MaximumBytes=1024L*1024*1024;
    public static void ObserveSpoolDirectory(string directory)
    {
        directory=Path.GetFullPath(directory);if(!Directory.Exists(directory))return;
        if((File.GetAttributes(directory)&FileAttributes.ReparsePoint)!=0)throw new IOException("Shared spool inventory refuses reparse directories.");
        var observed=new Dictionary<string,long>(StringComparer.OrdinalIgnoreCase);
        foreach(var file in new DirectoryInfo(directory).EnumerateFiles())
        {
            if(file.Name==".cycle-spool-owner.lock")continue;
            if(observed.Count>=64||file.Name.Length!=42||!file.Name.EndsWith(".spool",StringComparison.Ordinal)||!LocalCycleWire.Uuid(file.Name[..36])||(file.Attributes&FileAttributes.ReparsePoint)!=0)
                throw new InvalidDataException("Shared retained spool inventory is unrecognized or unbounded.");
            observed.Add(file.FullName,file.Length);
        }
        lock(Gate)
        {
            foreach(var path in Files.Keys.Where(path=>string.Equals(Path.GetDirectoryName(path),directory,StringComparison.OrdinalIgnoreCase)).ToArray())Files.Remove(path);
            if(Files.Count+observed.Count>16_384)throw new InvalidDataException("Shared retained spool inventory exceeds its bound.");
            foreach(var pair in observed)Files.Add(pair.Key,pair.Value);
        }
    }
    internal static bool Reserve(string path,long bytes)
    {
        lock(Gate)
        {
            if(Files.TryGetValue(path,out var previous))return previous==bytes;
            if(Files.Count>=64||bytes>MaximumBytes-Files.Values.Sum())return false;
            Files.Add(path,bytes);return true;
        }
    }
    internal static void Forget(string path){lock(Gate)Files.Remove(path);}
}
