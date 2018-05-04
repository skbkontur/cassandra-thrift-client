using System;
using System.IO;
using System.Text;

using Vostok.Logging;
using Vostok.Logging.Configuration;
using Vostok.Logging.Configuration.Settings;
using Vostok.Logging.Logs;

namespace SKBKontur.Cassandra.FunctionalTests.Utils
{
    public static class Logger
    {
        private static ILog log;

        public static ILog Instance => log ?? (log = new SilentLog());

        private static ILog InitFileLogger()
        {
            FileLog.Configure(() => new FileLogSettings
                {
                    AppendToFile = false,
                    EnableRolling = false,
                    Encoding = Encoding.UTF8,
                    ConversionPattern = ConversionPattern.Default,
                    FilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "logs", $"FunctionalTests-{DateTime.Now:yyyy-MM-dd.HH:mm:ss}.log"),
                });
            return new FileLog();
        }
    }
}