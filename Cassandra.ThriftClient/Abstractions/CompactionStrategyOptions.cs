using System.Collections.Generic;
using System.Globalization;

namespace SkbKontur.Cassandra.ThriftClient.Abstractions
{
    public class CompactionStrategyOptions
    {
        public bool? Enabled { get; set; }

        // STCS
        public int? MinThreshold { get; set; }

        public int? MaxThreshold { get; set; }

        // LCS
        public int? SstableSizeInMb { get; set; }
    }

    internal static class CompactionStrategyOptionsExtensions
    {
        public static Dictionary<string, string> ToCassandraCompactionStrategyOptions(this CompactionStrategyOptions options)
        {
            if (options == null)
                return new Dictionary<string, string>();
            var result = new Dictionary<string, string>();
            if (options.Enabled.HasValue)
                result.Add(enabledOptionName, options.Enabled.Value.ToString(CultureInfo.InvariantCulture));
            if (options.MinThreshold.HasValue)
                result.Add(minThresholdOptionName, options.MinThreshold.Value.ToString(CultureInfo.InvariantCulture));
            if (options.MaxThreshold.HasValue)
                result.Add(maxThresholdOptionName, options.MaxThreshold.Value.ToString(CultureInfo.InvariantCulture));
            if (options.SstableSizeInMb.HasValue)
                result.Add(sstableSizeInMbOptionName, options.SstableSizeInMb.Value.ToString(CultureInfo.InvariantCulture));
            return result;
        }

        public static CompactionStrategyOptions FromCassandraCompactionStrategyOptions(this Dictionary<string, string> options)
        {
            if (options == null)
                return new CompactionStrategyOptions();
            var result = new CompactionStrategyOptions();
            if (options.ContainsKey(enabledOptionName))
                result.Enabled = GetBool(options[enabledOptionName]);
            if (options.ContainsKey(minThresholdOptionName))
                result.MinThreshold = GetInt(options[minThresholdOptionName]);
            if (options.ContainsKey(maxThresholdOptionName))
                result.MaxThreshold = GetInt(options[maxThresholdOptionName]);
            if (options.ContainsKey(sstableSizeInMbOptionName))
                result.SstableSizeInMb = GetInt(options[sstableSizeInMbOptionName]);
            return result;
        }

        private static bool? GetBool(string boolValue)
        {
            if (string.IsNullOrEmpty(boolValue))
                return null;
            if (!bool.TryParse(boolValue, out var result))
                return null;
            return result;
        }

        private static int? GetInt(string intValue)
        {
            if (string.IsNullOrEmpty(intValue))
                return null;
            if (!int.TryParse(intValue, out var result))
                return null;
            return result;
        }

        private const string enabledOptionName = "enabled";
        private const string minThresholdOptionName = "min_threshold";
        private const string maxThresholdOptionName = "max_threshold";
        private const string sstableSizeInMbOptionName = "sstable_size_in_mb";
    }
}