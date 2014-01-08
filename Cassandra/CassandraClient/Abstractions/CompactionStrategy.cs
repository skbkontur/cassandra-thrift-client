namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class CompactionStrategy
    {
        public static CompactionStrategy LeveledCompactionStrategy(CompactionStrategyOptions options, int? minCompactionThreshold = null, int? maxCompactionThreshold = null)
        {
            return new CompactionStrategy
                {
                    CompactionStrategyOptions = options,
                    CompactionStrategyType = CompactionStrategyType.Leveled,
                    MinCompactionThreshold = minCompactionThreshold,
                    MaxCompactionThreshold = maxCompactionThreshold
                };
        }

        public static CompactionStrategy SizeTieredCompactionStrategy(int? minCompactionThreshold = null, int? maxCompactionThreshold = null)
        {
            return new CompactionStrategy
                {
                    CompactionStrategyType = CompactionStrategyType.SizeTiered,
                    MinCompactionThreshold = minCompactionThreshold,
                    MaxCompactionThreshold = maxCompactionThreshold
                };
        }

        public CompactionStrategyType CompactionStrategyType { get; private set; }
        public CompactionStrategyOptions CompactionStrategyOptions { get; private set; }
        public int? MinCompactionThreshold { get; private set; }
        public int? MaxCompactionThreshold { get; private set; }
    }
}