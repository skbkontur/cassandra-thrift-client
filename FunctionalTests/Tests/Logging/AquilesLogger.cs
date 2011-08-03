using System;

using Aquiles;

using Tests.Logger;

namespace Tests.Logging
{
    public class AquilesLogger : ILogger
    {
        public void Fatal(string className, object message)
        {
            consoleLogger.Error("{0}:{1}", className, message);
        }

        public void Fatal(string className, object message, Exception exception)
        {
            consoleLogger.Error(exception, "{0}:{1}", className, message);
        }

        public void Error(string className, object message)
        {
            consoleLogger.Error("{0}:{1}", className, message);
        }

        public void Error(string className, object message, Exception exception)
        {
            consoleLogger.Error(exception, "{0}:{1}", className, message);
        }

        public void Warn(string className, object message)
        {
            consoleLogger.Warn("{0}:{1}", className, message);
        }

        public void Warn(string className, object message, Exception exception)
        {
            consoleLogger.Warn(exception, "{0}:{1}", className, message);
        }

        public void Info(string className, object message)
        {
        }

        public void Info(string className, object message, Exception exception)
        {
        }

        public void Trace(string className, object message)
        {
        }

        public void Trace(string className, object message, Exception exception)
        {
        }

        public void Debug(string className, object message)
        {
        }

        public void Debug(string className, object message, Exception exception)
        {
        }

        private static readonly ConsoleLogger consoleLogger = new ConsoleLogger(typeof(AquilesLogger).Name);
    }
}