using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Core.Metrics
{
    internal interface ISimpleCommandMetrics : ICommandMetrics
    {
        void RecordRetry();
        void RecordCommandExecutionInfo([NotNull] ISimpleCommand command);
    }
}