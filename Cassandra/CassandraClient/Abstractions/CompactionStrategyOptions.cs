using System.Collections.Generic;
using System.Globalization;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class CompactionStrategyOptions
    {
        public int? SstableSizeInMb { get; set; }
    }

    internal static class CompactionStrategyOptionsExtensions
    {
        public static Dictionary<string, string> ToCassandraCompactionStrategyOptions(this CompactionStrategyOptions options)
        {
            if(options == null)
                return new Dictionary<string, string>();
            var result = new Dictionary<string, string>();
            if(options.SstableSizeInMb.HasValue)
                result.Add(sstableSizeInMbOptionName, options.SstableSizeInMb.Value.ToString(CultureInfo.InvariantCulture));
            return result;
        }

        public static CompactionStrategyOptions FromCassandraCompactionStrategyOptions(this Dictionary<string, string> options)
        {
            if(options == null)
                return new CompactionStrategyOptions();
            var result = new CompactionStrategyOptions();
            if(options.ContainsKey(sstableSizeInMbOptionName))
                result.SstableSizeInMb = GetInt(options[sstableSizeInMbOptionName]);
            return result;
        }

        private static int? GetInt(string intValue)
        {
            if(string.IsNullOrEmpty(intValue))
                return null;
            int result;
            if(!int.TryParse(intValue, out result))
                return null;
            return result;
        }

        private const string sstableSizeInMbOptionName = "sstable_size_in_mb";
    }
}