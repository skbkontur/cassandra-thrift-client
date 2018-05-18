using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Clusters.ActualizationEventListener
{
    public interface ICassandraActualizerEventListener
    {
        void ActualizationStarted();
        void SchemaRetrieved(Keyspace[] keyspaces);
        void KeyspaceActualizationStarted(string keyspaceName);
        void KeyspaceAdded(Keyspace keyspace);
        void ActualizationCompleted();
        void ColumnFamilyUpdated(string keyspaceName, ColumnFamily columnFamily);
        void ColumnFamilyAdded(string keyspaceName, ColumnFamily columnFamily);
    }
}