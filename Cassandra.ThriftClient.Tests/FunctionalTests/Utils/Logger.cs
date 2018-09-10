using System;
using System.IO;
using System.Text;

using Vostok.Logging.Abstractions;
using Vostok.Logging.File;
using Vostok.Logging.File.Configuration;
using Vostok.Logging.Formatting;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Utils
{
    public static class Logger
    {
        public static ILog Instance => log ?? (log = InitFileLogger());

        private static ILog InitFileLogger()
        {
            var logsDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "logs");
            return new FileLog(new FileLogSettings
                {
                    Encoding = Encoding.UTF8,
                    FileOpenMode = FileOpenMode.Rewrite,
                    OutputTemplate = OutputTemplate.Default,
                    RollingStrategy = new RollingStrategyOptions {Type = RollingStrategyType.None},
                    FilePath = Path.Combine(logsDir, $"FunctionalTests-{DateTime.Now:yyyy-MM-dd.HH-mm-ss}.log")
                });
        }

        private static ILog log;
    }
}