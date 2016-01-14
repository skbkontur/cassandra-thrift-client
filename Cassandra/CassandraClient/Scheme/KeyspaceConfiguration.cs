using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Scheme
{
    public class KeyspaceConfiguration
    {
        public IReplicationStrategy ReplicationStrategy { get { return replicationStrategy ?? defaultReplicationStrategy; } set { replicationStrategy = value; } }
        public ColumnFamily[] ColumnFamilies { get { return columnFamilies ?? new ColumnFamily[0]; } set { columnFamilies = value; } }

        private ColumnFamily[] columnFamilies = new ColumnFamily[0];
        private IReplicationStrategy replicationStrategy = defaultReplicationStrategy;
        private static readonly SimpleReplicationStrategy defaultReplicationStrategy = SimpleReplicationStrategy.Create(1);
    }
}