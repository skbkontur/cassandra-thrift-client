using System.Collections.Generic;
using System.Linq;

using Aquiles.Model;

using CassandraClient.Abstractions;

namespace CassandraClient.Helpers
{
    public static class ColumnFamilyConverter
    {
        public static AquilesColumnFamily ToAquilesColumnFamily(this ColumnFamily columnFamily, string keyspace)
        {
            return columnFamily == null
                       ? null
                       : new AquilesColumnFamily
                           {
                               Id = columnFamily.Id,
                               Comparator = "UTF8Type",
                               Name = columnFamily.Name,
                               Keyspace = keyspace,
                               Columns = GetColumnDefinitions(columnFamily.Indexes),
                               RowCacheSize = columnFamily.RowCacheSize
                           };
        }

        public static ColumnFamily ToColumnFamily(this AquilesColumnFamily aquilesColumnFamily)
        {
            return aquilesColumnFamily == null
                       ? null
                       : new ColumnFamily
                           {
                               Id = aquilesColumnFamily.Id,
                               Name = aquilesColumnFamily.Name,
                               Indexes = GetIndexDefinitions(aquilesColumnFamily.Columns),
                               RowCacheSize = (int?)aquilesColumnFamily.RowCacheSize
                           };
        }

        private static List<AquilesColumnDefinition> GetColumnDefinitions(IEnumerable<IndexDefinition> indexDefinitions)
        {
            return indexDefinitions == null ? null : indexDefinitions.Select(definition => definition.ToAquilesColumnDefinition()).ToList();
        }

        private static List<IndexDefinition> GetIndexDefinitions(IEnumerable<AquilesColumnDefinition> columnDefinitions)
        {
            return columnDefinitions == null ? null : columnDefinitions.
                                                          Where(columnDefinition => columnDefinition.IsIndex).
                                                          Select(definition => definition.ToIndexDefinition()).ToList();
        }
    }
}