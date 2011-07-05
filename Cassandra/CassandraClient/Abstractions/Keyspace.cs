using System.Collections.Generic;

namespace CassandraClient.Abstractions
{
    public class Keyspace
    {
        public Dictionary<string, ColumnFamily> ColumnFamilies { get; set; }
        public string Name { get; set; }
        public int ReplicationFactor { get; set; }
        public string ReplicaPlacementStrategy { get; set; }
    }
}