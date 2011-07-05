using System.Collections.Generic;
using System.Linq;

using Aquiles.Model;

using CassandraClient.Abstractions;

namespace CassandraClient.Helpers
{
    public static class KeyspaceConverter
    {
        public static AquilesKeyspace ToAquilesKeyspace(this Keyspace keyspace)
        {
            return keyspace == null
                       ? null
                       : new AquilesKeyspace
                           {
                               ColumnFamilies = ToAquilesColumnFamilies(keyspace.ColumnFamilies),
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

        private static Dictionary<string, AquilesColumnFamily> ToAquilesColumnFamilies(Dictionary<string, ColumnFamily> columnFamilies)
        {
            if(columnFamilies == null)
                return null;
            return columnFamilies.ToDictionary(columnFamily => columnFamily.Key,
                                               columnFamily => columnFamily.Value.ToAquilesColumnFamily());
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