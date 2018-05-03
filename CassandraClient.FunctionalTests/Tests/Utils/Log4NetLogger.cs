using System;
using System.Globalization;
using System.Linq;

using log4net;
using log4net.Core;
using log4net.Util;

using Vostok.Logging;

using ILog = Vostok.Logging.ILog;

namespace SKBKontur.Cassandra.FunctionalTests.Utils
{
    public class Log4NetWrapper : ILog
    {
        public Log4NetWrapper(Type type)
        {
            this.log = LogManager.GetLogger(type);
            this.type = type;
        }
        
        public void Log(LogEvent @event)
        {
            log.Logger.Log(
                type,
                GetLevel(@event.Level),
                new SystemStringFormat(CultureInfo.InvariantCulture, @event.MessageTemplate, @event.Properties?.OrderBy(x => x.Key).Select(x => x.Value).ToArray()),
                @event.Exception
            );
        }

        public bool IsEnabledFor(LogLevel level)
        {
            return log.Logger.IsEnabledFor(GetLevel(level));
        }

        private Level GetLevel(LogLevel level)
        {
            switch(level)
            {
            case LogLevel.Debug:
                return Level.Debug;
            case LogLevel.Info:
                return Level.Info;
            case LogLevel.Warn:
                return Level.Warn;
            case LogLevel.Error:
                return Level.Error;
            case LogLevel.Fatal:
                return Level.Fatal;
            default:
                throw new ArgumentOutOfRangeException(nameof(level), level, $"Unknown LogLevel {level}");
            }
        }

        private readonly log4net.ILog log;
        private readonly Type type;
    }
}