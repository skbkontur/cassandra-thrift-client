using System.Collections.Generic;

using Apache.Cassandra;

using System.Linq;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class ColumnFamily
    {
        public int? RowCacheSize { get; set; }
        public int? GCGraceSeconds { get; set; }
        public string Name { get; set; }
        public int Id { get; set; }
        public List<IndexDefinition> Indexes { get; set; }
    }

    internal static class ColumnFamilyExtensions
    {
        public static CfDef ToCassandraCfDef(this ColumnFamily columnFamily, string keyspace)
        {
            if (columnFamily == null)
                return null;
            var result = new CfDef
                {
                    Id = columnFamily.Id,
                    Keyspace = keyspace,
                    Name = columnFamily.Name,
                    Column_type = "Standard",
                    Comparator_type = "UTF8Type"
                };
            if (columnFamily.GCGraceSeconds.HasValue)
                result.Gc_grace_seconds = columnFamily.GCGraceSeconds.Value;
            if(columnFamily.Indexes != null)
                result.Column_metadata = new List<ColumnDef>(columnFamily.Indexes.Select(definition => definition.ToCassandraColumnDef()));
            if (columnFamily.RowCacheSize.HasValue)
                result.Row_cache_size = columnFamily.RowCacheSize.Value;
            return result;
        }

        public static ColumnFamily FromCassandraCfDef(this CfDef cfDef)
        {
            if (cfDef == null)
                return null;
            var result = new ColumnFamily
                {
                    GCGraceSeconds = cfDef.Gc_grace_seconds,
                    RowCacheSize = (int?)cfDef.Row_cache_size,
                    Name = cfDef.Name,
                    Id = cfDef.Id
                };
            if (cfDef.Column_metadata != null)
                result.Indexes = new List<IndexDefinition>(cfDef.Column_metadata.Select(def => def.FromCassandraColumnDef()));
            return result;
        }
    }
}