using System;
using System.Runtime.InteropServices;

namespace ParquetSharp
{
    public sealed class ApplicationVersion
    {
        internal ApplicationVersion(CStruct cstruct)
        {
            Application = StringUtil.PtrToStringUtf8(cstruct.Application);
            Build = StringUtil.PtrToStringUtf8(cstruct.Build);

            Major = cstruct.Major;
            Minor = cstruct.Minor;
            Patch = cstruct.Patch;

            Unknown = StringUtil.PtrToStringUtf8(cstruct.Unknown);
            PreRelease = StringUtil.PtrToStringUtf8(cstruct.PreRelease);
            BuildInfo = StringUtil.PtrToStringUtf8(cstruct.BuildInfo);
        }

        public readonly string Application;
        public readonly string Build;

        public readonly int Major;
        public readonly int Minor;
        public readonly int Patch;

        public readonly string Unknown;
        public readonly string PreRelease;
        public readonly string BuildInfo;

        public override string ToString()
        {
            return $"{Application} version {Major}.{Minor}.{Patch}";
        }

        [StructLayout(LayoutKind.Sequential)]
        internal readonly struct CStruct
        {
            public readonly IntPtr Application;
            public readonly IntPtr Build;

            public readonly int Major;
            public readonly int Minor;
            public readonly int Patch;

            public readonly IntPtr Unknown;
            public readonly IntPtr PreRelease;
            public readonly IntPtr BuildInfo;
        };
    }

}