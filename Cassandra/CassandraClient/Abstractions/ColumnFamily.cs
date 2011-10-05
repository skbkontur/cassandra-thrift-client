using System.Collections.Generic;

namespace CassandraClient.Abstractions
{
    public class ColumnFamily
    {
        public int? RowCacheSize { get; set; }
        public int? GCGraceSeconds { get; set; }
        public string Name { get; set; }
        public int Id { get; set; }
        public List<IndexDefinition> Indexes { get; set; }
    }
}