using System;

using Vostok.Logging.Abstractions.Extensions;

namespace Vostok.Logging.Abstractions
{
    // todo (avk, 2018.06.03): move to Vostok.Logging.Abstractions
    public class ContextLogWrapper : ILog
    {
        public ContextLogWrapper(ILog baseLog, string subContextName)
        {
            if(string.IsNullOrEmpty(subContextName))
                throw new ArgumentException("subContextName is empty");
            this.baseLog = baseLog;
            this.subContextName = subContextName;
        }

        public void Log(LogEvent @event)
        {
            var contextName = subContextName;
            if(@event.Properties != null && @event.Properties.TryGetValue(contextNamePropsKey, out var innerContextName))
                contextName = $"{subContextName}.{innerContextName}";
            @event = @event.SetProperty(contextNamePropsKey, contextName);
            baseLog.Log(@event);
        }

        public bool IsEnabledFor(LogLevel level)
        {
            return baseLog.IsEnabledFor(level);
        }

        private const string contextNamePropsKey = "ContextName";

        private readonly ILog baseLog;
        private readonly string subContextName;
    }

    public static class LogExtensions
    {
        public static ILog WithContext(this ILog log, string subContextName)
        {
            return new ContextLogWrapper(log, subContextName);
        }
    }
}