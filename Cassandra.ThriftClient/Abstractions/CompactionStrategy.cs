namespace SkbKontur.Cassandra.ThriftClient.Abstractions
{
    public class CompactionStrategy
    {
        public CompactionStrategy(CompactionStrategyType compactionStrategyType, CompactionStrategyOptions compactionStrategyOptions)
        {
            CompactionStrategyType = compactionStrategyType;
            CompactionStrategyOptions = compactionStrategyOptions;
        }

        public static CompactionStrategy LeveledCompactionStrategy(int sstableSizeInMb)
        {
            return new CompactionStrategy(CompactionStrategyType.Leveled, new CompactionStrategyOptions {Enabled = true, SstableSizeInMb = sstableSizeInMb});
        }

        public static CompactionStrategy LeveledCompactionStrategyDisabled()
        {
            return new CompactionStrategy(CompactionStrategyType.Leveled, new CompactionStrategyOptions {Enabled = false});
        }

        public static CompactionStrategy SizeTieredCompactionStrategy(int minThreshold, int maxThreshold)
        {
            return new CompactionStrategy(CompactionStrategyType.SizeTiered, new CompactionStrategyOptions {Enabled = true, MinThreshold = minThreshold, MaxThreshold = maxThreshold});
        }

        public static CompactionStrategy SizeTieredCompactionStrategyDisabled()
        {
            return new CompactionStrategy(CompactionStrategyType.SizeTiered, new CompactionStrategyOptions {Enabled = false});
        }

        public CompactionStrategyType CompactionStrategyType { get; }
        public CompactionStrategyOptions CompactionStrategyOptions { get; }
    }
}