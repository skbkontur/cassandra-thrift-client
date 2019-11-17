namespace SkbKontur.Cassandra.ThriftClient.Abstractions
{
    internal class CompressionAlgorithms
    {
        public const string Snappy = "org.apache.cassandra.io.compress.SnappyCompressor";
        public const string Deflate = "org.apache.cassandra.io.compress.DeflateCompressor";
        public const string LZ4 = "org.apache.cassandra.io.compress.LZ4Compressor";
        public const string None = "";
    }
}