using SkbKontur.Cassandra.ThriftClient.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Clusters.ActualizationEventListener
{
    internal class EmptyCassandraActualizerEventListener : ICassandraActualizerEventListener
    {
        public void ActualizationStarted()
        {
        }

        public void SchemaRetrieved(Keyspace[] keyspaces)
        {
        }

        public void KeyspaceActualizationStarted(string keyspaceName)
        {
        }

        public void KeyspaceAdded(Keyspace keyspace)
        {
        }

        public void ActualizationCompleted()
        {
        }

        public void ColumnFamilyUpdated(string keyspaceName, ColumnFamily columnFamily)
        {
        }

        public void ColumnFamilyAdded(string keyspaceName, ColumnFamily columnFamily)
        {
        }

        public static ICassandraActualizerEventListener Instance
        {
            get
            {
                if (instance == null)
                {
                    lock (instanceLockObject)
                    {
                        if (instance == null)
                            instance = new EmptyCassandraActualizerEventListener();
                    }
                }
                return instance;
            }
        }

        private static EmptyCassandraActualizerEventListener instance;
        private static readonly object instanceLockObject = new object();
    }
}