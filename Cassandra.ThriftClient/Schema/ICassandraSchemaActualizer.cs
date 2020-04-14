namespace SkbKontur.Cassandra.ThriftClient.Schema
{
    public interface ICassandraSchemaActualizer
    {
        void ActualizeKeyspaces(KeyspaceSchema[] keyspaceShemas, bool changeExistingKeyspaceMetadata);
    }
}