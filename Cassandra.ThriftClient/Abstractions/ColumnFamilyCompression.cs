using System.Collections.Generic;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class ColumnFamilyCompression
    {
        internal ColumnFamilyCompression(string algorithm, CompressionOptions options)
        {
            Algorithm = algorithm;
            Options = options;
        }

        public static ColumnFamilyCompression None()
        {
            return new ColumnFamilyCompression(CompressionAlgorithms.None, null);
        }

        public static ColumnFamilyCompression Snappy(CompressionOptions options)
        {
            return new ColumnFamilyCompression(CompressionAlgorithms.Snappy, options);
        }

        public static ColumnFamilyCompression Deflate(CompressionOptions options)
        {
            return new ColumnFamilyCompression(CompressionAlgorithms.Deflate, options);
        }

        public static ColumnFamilyCompression LZ4(CompressionOptions options)
        {
            return new ColumnFamilyCompression(CompressionAlgorithms.LZ4, options);
        }

        public bool IsEnabled => !string.IsNullOrWhiteSpace(Algorithm);

        public string Algorithm { get; set; }
        public CompressionOptions Options { get; }
        public static ColumnFamilyCompression Default { get; } = new ColumnFamilyCompression(CompressionAlgorithms.Snappy, null);
    }

    internal static class ColumnFamilyCompressionExtensions
    {
        public static ColumnFamilyCompression FromCassandraCompressionOptions(this Dictionary<string, string> value)
        {
            if (value.ContainsKey(cassandraCompressionAlgorithmKeyName))
            {
                var algorithm = value[cassandraCompressionAlgorithmKeyName];
                var options = new CompressionOptions();
                if (value.ContainsKey("chunk_length_kb"))
                    options.ChunkLengthInKb = int.Parse(value["chunk_length_kb"]);
                return new ColumnFamilyCompression(algorithm, options);
            }
            return new ColumnFamilyCompression(CompressionAlgorithms.None, null);
        }

        public static Dictionary<string, string> ToCassandraCompressionDef(this ColumnFamilyCompression value)
        {
            var result = new Dictionary<string, string>();
            if (!value.IsEnabled)
            {
                result.Add(cassandraCompressionAlgorithmKeyName, string.Empty);
                return result;
            }

            result.Add(cassandraCompressionAlgorithmKeyName, value.Algorithm);
            if (value.Options?.ChunkLengthInKb != null)
                result.Add("chunk_length_kb", value.Options.ChunkLengthInKb.ToString());
            return result;
        }

        private const string cassandraCompressionAlgorithmKeyName = "sstable_compression";
    }
}