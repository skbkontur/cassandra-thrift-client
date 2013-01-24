namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class CompactionStrategy
    {
        public CompactionStrategyType CompactionStrategyType { get; private set; }
        public CompactionStrategyOptions CompactionStrategyOptions { get; private set; }

        public static CompactionStrategy LeveledCompactionStrategy(CompactionStrategyOptions options)
        {
            return new CompactionStrategy
                {
                    CompactionStrategyOptions = options,
                    CompactionStrategyType = CompactionStrategyType.Leveled
                };
        }

        public static CompactionStrategy SizeTieredCompactionStrategy()
        {
            return new CompactionStrategy
                {
                    CompactionStrategyType = CompactionStrategyType.SizeTiered
                };
        }
    }
}