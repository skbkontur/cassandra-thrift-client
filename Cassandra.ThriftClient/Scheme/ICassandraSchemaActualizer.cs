namespace SkbKontur.Cassandra.ThriftClient.Scheme
{
    public interface ICassandraSchemaActualizer
    {
        void ActualizeKeyspaces(KeyspaceScheme[] keyspaceShemas, bool changeExistingKeyspaceMetadata);
    }
}