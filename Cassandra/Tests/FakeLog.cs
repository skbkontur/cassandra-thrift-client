using Vostok.Logging;

namespace Cassandra.Tests
{
    public class FakeLog : ILog
    {
        public void Log(LogEvent @event)
        {
        }

        public bool IsEnabledFor(LogLevel level)
        {
            return false;
        }
    }
}