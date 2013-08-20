using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Scheme
{
    public class KeyspaceConfiguration
    {
        public int ReplicationFactor { get { return replicationFactor; } set { replicationFactor = value; } }
        public ReplicaPlacementStrategy ReplicaPlacementStrategy { get { return replicaPlacementStrategy; } set { replicaPlacementStrategy = value; } }
        public ColumnFamily[] ColumnFamilies { get { return columnFamilies ?? new ColumnFamily[0]; } set { columnFamilies = value; } }
        private int replicationFactor = 1;
        private ReplicaPlacementStrategy replicaPlacementStrategy = ReplicaPlacementStrategy.Simple;
        private ColumnFamily[] columnFamilies = new ColumnFamily[0];
    }
}