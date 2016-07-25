using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Core.Metrics;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    using IThriftConnectionReplicaSetPool = IPoolSet<IThriftConnection, string>;

    internal static class CommandExecutorFactory
    {
        public static ICommandExecuter Create(
            IThriftConnectionReplicaSetPool dataCommandsConnectionPool,
            IThriftConnectionReplicaSetPool fierceCommandsConnectionPool,
            ICassandraClusterSettings settings)
        {
            ICommandExecuter commandExecutor = new CommandExecuter(dataCommandsConnectionPool, fierceCommandsConnectionPool, settings);
            if(settings.EnableMetrics)
                commandExecutor = new CommandExecutorWithMetrics(commandExecutor);
            return commandExecutor;
        }
    }
}