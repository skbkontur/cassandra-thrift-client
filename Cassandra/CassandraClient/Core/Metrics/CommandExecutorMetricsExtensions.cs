using System;

using Metrics;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    internal static class CommandExecutorMetricsExtensions
    {
        public static void Record(this CommandExecutorMetrics metrics, Action<CommandExecutorMetrics> action)
        {
            if(metrics != null)
                action(metrics);
        }

        public static void UpdateHistogram(this CommandExecutorMetrics metrics, Func<CommandExecutorMetrics, Histogram> histogramGetter,
                                           long value)
        {
            if(metrics != null)
                histogramGetter(metrics).Update(value, metrics.CommandInfo);
        }

        public static IDisposable CreateTimerContext(this CommandExecutorMetrics metrics, Func<CommandExecutorMetrics, Timer> timerGetter)
        {
            return metrics == null
                       ? (IDisposable)new TimerContextStub()
                       : timerGetter(metrics).NewContext(metrics.CommandInfo);
        }

        private class TimerContextStub : IDisposable
        {
            public void Dispose()
            {
            }
        }
    }
}