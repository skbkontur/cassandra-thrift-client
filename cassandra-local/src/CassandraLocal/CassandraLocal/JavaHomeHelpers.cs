using System;
using Microsoft.Win32;

namespace SkbKontur.Cassandra.Local
{
    public static class JavaHomeHelpers
    {
        public const string Jdk8Key = @"Software\JavaSoft\Java Development Kit\1.8";
        public const string Jre8Key = @"Software\JavaSoft\Java Runtime Environment\1.8";

        public static string GetJava8Home()
        {
            var java8Home = TryGetJavaHome(Jre8Key) ?? TryGetJavaHome(Jdk8Key);
            if (string.IsNullOrWhiteSpace(java8Home))
                throw new InvalidOperationException("Java 8 64-bit home directory is not found");
            return java8Home;
        }

        public static string TryGetJavaHome(string javaKey)
        {
            using (var hklm64 = RegistryKey.OpenBaseKey(RegistryHive.LocalMachine, RegistryView.Registry64))
            using (var registryKey = hklm64.OpenSubKey(javaKey))
                return (string)registryKey?.GetValue("JavaHome");
        }
    }
}