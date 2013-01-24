namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public enum CompactionStrategyType
    {
        [StringValue("org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy")]
        SizeTiered,

        [StringValue("org.apache.cassandra.db.compaction.LeveledCompactionStrategy")]
        Leveled
    }
}