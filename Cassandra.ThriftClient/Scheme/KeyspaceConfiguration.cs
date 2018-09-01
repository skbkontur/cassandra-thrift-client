using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Scheme
{
    public class KeyspaceConfiguration
    {
        public KeyspaceConfiguration()
        {
            DurableWrites = true;
        }

        public bool DurableWrites { get; set; }
        public IReplicationStrategy ReplicationStrategy { get => replicationStrategy ?? defaultReplicationStrategy; set => replicationStrategy = value; }
        public ColumnFamily[] ColumnFamilies { get => columnFamilies ?? new ColumnFamily[0]; set => columnFamilies = value; }

        private ColumnFamily[] columnFamilies;
        private IReplicationStrategy replicationStrategy;
        private static readonly SimpleReplicationStrategy defaultReplicationStrategy = SimpleReplicationStrategy.Create(1);
    }
}