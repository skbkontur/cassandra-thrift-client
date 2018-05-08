using System.IO;

namespace CassandraLocal.Tests
{
    public static class DirectoryHelpers
    {
        public static string FindDirectory(string currentDir, string searchForDirectory)
        {
            var matchingDirectory = Path.Combine(currentDir, searchForDirectory);
            if (Directory.Exists(matchingDirectory))
                return matchingDirectory;
            return FindDirectory(Path.GetDirectoryName(currentDir), searchForDirectory);
        }
    }
}