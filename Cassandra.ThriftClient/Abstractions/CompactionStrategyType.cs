namespace SkbKontur.Cassandra.ThriftClient.Abstractions
{
    public enum CompactionStrategyType
    {
        [StringValue("org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy")]
        SizeTiered,

        [StringValue("org.apache.cassandra.db.compaction.LeveledCompactionStrategy")]
        Leveled
    }
}