using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class ColumnFamily
    {
        public ColumnFamily()
        {
            SetDefaults();
        }

        public string Name { get; set; }
        public int? GCGraceSeconds { get; set; }
        public List<IndexDefinition> Indexes { get; set; }
        public double? ReadRepairChance { get; set; }
        public CompactionStrategy CompactionStrategy { get { return compactionStrategy; } set { compactionStrategy = value; } }
        public ColumnFamilyCaching Caching { get; set; }
        public ColumnFamilyCompression Compression { get; set; }
        internal int Id { get; set; }

        private void SetDefaults()
        {
            Caching = ColumnFamilyCaching.KeysOnly;
        }

        private CompactionStrategy compactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy();
    }

    internal static class ColumnFamilyExtensions
    {
        public static CfDef ToCassandraCfDef(this ColumnFamily columnFamily, string keyspace)
        {
            if(columnFamily == null)
                return null;
            var result = new CfDef
                {
                    Id = columnFamily.Id,
                    Name = columnFamily.Name,
                    Keyspace = keyspace,
                    Column_type = "Standard",
                    Comparator_type = DataType.UTF8Type.ToStringValue(),
                    Caching = columnFamily.ToCassandraCachingValue()
                };
            if (columnFamily.Compression != null)
                result.Compression_options = columnFamily.Compression.ToCassandraCompressionDef();
            if(columnFamily.GCGraceSeconds.HasValue)
                result.Gc_grace_seconds = columnFamily.GCGraceSeconds.Value;
            if(columnFamily.Indexes != null)
                result.Column_metadata = new List<ColumnDef>(columnFamily.Indexes.Select(definition => definition.ToCassandraColumnDef()));
            if(columnFamily.ReadRepairChance.HasValue)
                result.Read_repair_chance = columnFamily.ReadRepairChance.Value;
            result.Compaction_strategy = columnFamily.CompactionStrategy.CompactionStrategyType.ToStringValue();
            result.Compaction_strategy_options = columnFamily.CompactionStrategy.CompactionStrategyOptions.ToCassandraCompactionStrategyOptions();
            return result;
        }

        public static ColumnFamily FromCassandraCfDef(this CfDef cfDef)
        {
            if(cfDef == null)
                return null;
            var result = new ColumnFamily
                {
                    Name = cfDef.Name,
                    Id = cfDef.Id
                };
            if(cfDef.__isset.gc_grace_seconds)
                result.GCGraceSeconds = cfDef.Gc_grace_seconds;
            if(cfDef.__isset.caching)
                result.Caching = cfDef.Caching.ToColumnFamilyCaching();
            if(cfDef.__isset.read_repair_chance)
                result.ReadRepairChance = cfDef.Read_repair_chance;
            if(cfDef.Column_metadata != null)
                result.Indexes = new List<IndexDefinition>(cfDef.Column_metadata.Select(def => def.FromCassandraColumnDef()));
            if(cfDef.Compression_options != null)
                result.Compression = cfDef.Compression_options.FromCassandraCompressionOptions();

            var compactionStrategyType = cfDef.Compaction_strategy.FromStringValue<CompactionStrategyType>();
            if(compactionStrategyType == CompactionStrategyType.Leveled)
            {
                var options = cfDef.Compaction_strategy_options.FromCassandraCompactionStrategyOptions();
                result.CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(options);
            }
            else
                result.CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy();

            return result;
        }

        private static string ToCassandraCachingValue(this ColumnFamily columnFamily)
        {
            return columnFamily.Caching.ToCassandraStringValue();
        }
    }
}