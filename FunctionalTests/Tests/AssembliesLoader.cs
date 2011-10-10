using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

namespace SKBKontur.Cassandra.FunctionalTests
{
    public static class AssembliesLoader
    {
        public static IEnumerable<Assembly> Load()
        {
            string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Bin");
            if (!Directory.Exists(path))
                path = AppDomain.CurrentDomain.BaseDirectory;
            return Directory.EnumerateFiles(path, "*", SearchOption.TopDirectoryOnly).Where(IsOurAssembly).Select(Assembly.LoadFrom).ToArray();
        }

        private static bool IsOurAssembly(string fullFileName)
        {
            var fileName = Path.GetFileName(fullFileName);
            if (string.IsNullOrEmpty(fileName)) return false;
            return (fileName.EndsWith(".dll", StringComparison.InvariantCultureIgnoreCase)
                    || fileName.EndsWith(".exe", StringComparison.InvariantCultureIgnoreCase))
                   &&
                   (fileName.StartsWith("WebPersonal.", StringComparison.InvariantCultureIgnoreCase)
                    || fileName.StartsWith("SKBKontur.", StringComparison.InvariantCultureIgnoreCase)
                    || fileName.StartsWith("Catalogue.", StringComparison.InvariantCultureIgnoreCase)
                    || fileName.StartsWith("GroboSerializer", StringComparison.InvariantCultureIgnoreCase)
                    || fileName.StartsWith("Cassandra.", StringComparison.InvariantCultureIgnoreCase));
        }
    }
}