using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.Helpers
{
    public static class KeyspaceConverter
    {
        public static AquilesKeyspace ToAquilesKeyspace(this Keyspace keyspace)
        {
            return keyspace == null
                       ? null
                       : new AquilesKeyspace
                           {
                               ColumnFamilies = ToAquilesColumnFamilies(keyspace.ColumnFamilies, keyspace.Name),
                               Name = keyspace.Name,
                               ReplicationFactor = keyspace.ReplicationFactor,
                               ReplicationPlacementStrategy = keyspace.ReplicaPlacementStrategy
                           };
        }

        public static Keyspace ToKeyspace(this AquilesKeyspace aquilesKeyspace)
        {
            return aquilesKeyspace == null
                       ? null
                       : new Keyspace
                           {
                               ColumnFamilies = ToColumnFamilies(aquilesKeyspace.ColumnFamilies),
                               Name = aquilesKeyspace.Name,
                               ReplicationFactor = aquilesKeyspace.ReplicationFactor,
                               ReplicaPlacementStrategy = aquilesKeyspace.ReplicationPlacementStrategy
                           };
        }

        private static Dictionary<string, AquilesColumnFamily> ToAquilesColumnFamilies(Dictionary<string, ColumnFamily> columnFamilies, string keyspaceName)
        {
            if(columnFamilies == null)
                return null;
            return columnFamilies.ToDictionary(columnFamily => columnFamily.Key,
                                               columnFamily => columnFamily.Value.ToAquilesColumnFamily(keyspaceName));
        }

        private static Dictionary<string, ColumnFamily> ToColumnFamilies(Dictionary<string, AquilesColumnFamily> columnFamilies)
        {
            if(columnFamilies == null)
                return null;
            return columnFamilies.ToDictionary(columnFamily => columnFamily.Key,
                                               columnFamily => columnFamily.Value.ToColumnFamily());
        }
    }
}