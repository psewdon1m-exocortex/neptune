using System.Runtime.InteropServices;

namespace Neptune.Windows;

internal static class WindowsInstallerTrust
{
    // OS Authenticode verification is independent from the signed bundle hash.
    internal static void RequireTrusted(string file)
    {
        if(!OperatingSystem.IsWindows())throw new PlatformNotSupportedException();
        var info=new FileInfoData{Size=(uint)Marshal.SizeOf<FileInfoData>(),Path=Path.GetFullPath(file),File=IntPtr.Zero,Subject=IntPtr.Zero};
        var pointer=Marshal.AllocHGlobal(Marshal.SizeOf<FileInfoData>());
        try
        {
            Marshal.StructureToPtr(info,pointer,false);
            var data=new TrustData{Size=(uint)Marshal.SizeOf<TrustData>(),Ui=2,Revocation=1,Choice=1,File=pointer,StateAction=1,Flags=0x80,Policy=IntPtr.Zero,Sip=IntPtr.Zero,State=IntPtr.Zero,Url=IntPtr.Zero,UiContext=0,SignatureSettings=IntPtr.Zero};
            var action=new Guid("00aac56b-cd44-11d0-8cc2-00c04fc295ee");
            try{if(WinVerifyTrust(new IntPtr(-1),ref action,ref data)!=0)throw new InvalidDataException("Windows rejected the installer Authenticode signature or certificate trust.");}
            finally{data.StateAction=2;_=WinVerifyTrust(new IntPtr(-1),ref action,ref data);}
        }
        finally{Marshal.DestroyStructure<FileInfoData>(pointer);Marshal.FreeHGlobal(pointer);}
    }
    [StructLayout(LayoutKind.Sequential,CharSet=CharSet.Unicode)]
    private struct FileInfoData{internal uint Size;[MarshalAs(UnmanagedType.LPWStr)]internal string Path;internal IntPtr File,Subject;}
    [StructLayout(LayoutKind.Sequential)]
    private struct TrustData{internal uint Size;internal IntPtr Policy,Sip;internal uint Ui,Revocation,Choice;internal IntPtr File;internal uint StateAction;internal IntPtr State,Url;internal uint Flags,UiContext;internal IntPtr SignatureSettings;}
    [DllImport("wintrust.dll",ExactSpelling=true,PreserveSig=true)]
    private static extern int WinVerifyTrust(IntPtr window,ref Guid action,ref TrustData data);
}
