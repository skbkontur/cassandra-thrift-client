using JetBrains.Annotations;

namespace SkbKontur.Cassandra.ThriftClient.Schema
{
    [PublicAPI]
    public interface ICassandraSchemaActualizer
    {
        void ActualizeKeyspaces(KeyspaceSchema[] keyspaceShemas, bool changeExistingKeyspaceMetadata);
    }
}