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
        public double? ReadRepairChance { get; set; }
        public CompactionStrategy CompactionStrategy { get; set; }
        public ColumnFamilyCaching Caching { get; set; }
        public ColumnFamilyCompression Compression { get; set; }
        public double? BloomFilterFpChance { get; set; }
        public ColumnComparatorType ComparatorType { get; set; }
        public int? DefaultTtl { get; set; }
        internal int Id { get; set; }

        private void SetDefaults()
        {
            Caching = ColumnFamilyCaching.KeysOnly;
            ComparatorType = new ColumnComparatorType(DataType.UTF8Type);
            CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy(minThreshold : 4, maxThreshold : 32);
        }
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
                    Name = columnFamily.Name,
                    Keyspace = keyspace,
                    Column_type = "Standard",
                    Comparator_type = columnFamily.ComparatorType.ToString(),
                    Caching = columnFamily.ToCassandraCachingValue()
                };
            if (columnFamily.Compression != null)
                result.Compression_options = columnFamily.Compression.ToCassandraCompressionDef();
            if (columnFamily.GCGraceSeconds.HasValue)
                result.Gc_grace_seconds = columnFamily.GCGraceSeconds.Value;
            if (columnFamily.ReadRepairChance != null)
                result.Read_repair_chance = columnFamily.ReadRepairChance.Value;

            var compactionStrategy = columnFamily.CompactionStrategy;
            if (compactionStrategy != null)
            {
                result.Compaction_strategy = compactionStrategy.CompactionStrategyType.ToStringValue();
                result.Compaction_strategy_options = compactionStrategy.CompactionStrategyOptions.ToCassandraCompactionStrategyOptions();
            }

            if (columnFamily.BloomFilterFpChance.HasValue)
                result.Bloom_filter_fp_chance = columnFamily.BloomFilterFpChance.Value;

            if (columnFamily.DefaultTtl.HasValue)
                result.Default_time_to_live = columnFamily.DefaultTtl.Value;

            return result;
        }

        public static ColumnFamily FromCassandraCfDef(this CfDef cfDef)
        {
            if (cfDef == null)
                return null;
            var result = new ColumnFamily
                {
                    Name = cfDef.Name,
                    Id = cfDef.Id,
                    ComparatorType = new ColumnComparatorType(cfDef.Comparator_type)
                };
            if (cfDef.__isset.gc_grace_seconds)
                result.GCGraceSeconds = cfDef.Gc_grace_seconds;
            if (cfDef.__isset.caching)
                result.Caching = cfDef.Caching.ToColumnFamilyCaching();
            if (cfDef.__isset.read_repair_chance)
                result.ReadRepairChance = cfDef.Read_repair_chance;
            if (cfDef.Compression_options != null)
                result.Compression = cfDef.Compression_options.FromCassandraCompressionOptions();

            var compactionStrategyType = cfDef.Compaction_strategy.FromStringValue<CompactionStrategyType>();
            var compactionStrategyOptions = cfDef.Compaction_strategy_options.FromCassandraCompactionStrategyOptions();
            result.CompactionStrategy = new CompactionStrategy(compactionStrategyType, compactionStrategyOptions);

            if (cfDef.__isset.bloom_filter_fp_chance)
                result.BloomFilterFpChance = cfDef.Bloom_filter_fp_chance;

            if (cfDef.__isset.default_time_to_live)
                result.DefaultTtl = cfDef.Default_time_to_live;

            return result;
        }

        private static string ToCassandraCachingValue(this ColumnFamily columnFamily)
        {
            return columnFamily.Caching.ToCassandraStringValue();
        }
    }
}