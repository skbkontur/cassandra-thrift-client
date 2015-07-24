using System;
using System.IO;

using log4net.Config;

namespace SKBKontur.Cassandra.FunctionalTests.Utils
{
    public class ServiceUtils
    {
        private static volatile string log4NetConfigurationFile;

        public static string Log4NetConfigurationFile
        {
            get
            {
                if (log4NetConfigurationFile == null)
                    throw new InvalidOperationException("log4net not configured");
                return log4NetConfigurationFile;
            }
        }

        public static void ConfugureLog4Net(string path)
        {
            string settingsPath = Path.Combine(path, "Settings");
            string[] files = Directory.GetFiles(settingsPath, "*log4net.config", SearchOption.TopDirectoryOnly);
            if (files.Length == 1)
            {
                log4NetConfigurationFile = files[0];
                ConfigureWithAbsolutePath(log4NetConfigurationFile);
            }
            else
                throw new Exception(string.Format("Should be 1 Log4Net config file, but was {0}",
                                                                      files.Length));
        }

        private static void ConfigureWithAbsolutePath(string fileName)
        {
            var fileInfo = new FileInfo(fileName);
            if (!fileInfo.Exists)
                throw new Exception(string.Format("Logger configuration file {0} not found",
                                                                            fileInfo.FullName));
            XmlConfigurator.ConfigureAndWatch(fileInfo);
            //XmlConfigurator.Configure(fileInfo);
        }
    }
}