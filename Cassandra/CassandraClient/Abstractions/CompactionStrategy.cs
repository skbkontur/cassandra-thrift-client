namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class CompactionStrategy
    {
        public static CompactionStrategy LeveledCompactionStrategy(CompactionStrategyOptions options)
        {
            return new CompactionStrategy
                {
                    CompactionStrategyType = CompactionStrategyType.Leveled,
                    CompactionStrategyOptions = options,
                };
        }

        public static CompactionStrategy SizeTieredCompactionStrategy(CompactionStrategyOptions options)
        {
            return new CompactionStrategy
                {
                    CompactionStrategyType = CompactionStrategyType.SizeTiered,
                    CompactionStrategyOptions = options,
                };
        }

        public CompactionStrategyType CompactionStrategyType { get; private set; }
        public CompactionStrategyOptions CompactionStrategyOptions { get; private set; }
    }
}