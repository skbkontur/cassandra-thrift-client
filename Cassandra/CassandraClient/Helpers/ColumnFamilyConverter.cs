using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.Helpers
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
                               Columns = columnFamily.Indexes,
                               RowCacheSize = columnFamily.RowCacheSize,
                               GCGraceSeconds = columnFamily.GCGraceSeconds
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
                               Indexes = aquilesColumnFamily.Columns,
                               RowCacheSize = (int?)aquilesColumnFamily.RowCacheSize,
                               GCGraceSeconds = aquilesColumnFamily.GCGraceSeconds
                           };
        }
    }
}