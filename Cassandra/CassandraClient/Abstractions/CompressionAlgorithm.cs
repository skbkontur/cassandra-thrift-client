using System;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public enum CompressionAlgorithm
    {
        None,
        Snappy,
        Deflate
    }

    internal static class CompressionAlgorithmExtensions
    {
        public static CompressionAlgorithm FromCassandraCompressionAlgorithm(this string value)
        {
            if(value.ToLower() == "org.apache.cassandra.io.compress.SnappyCompressor".ToLower())
                return CompressionAlgorithm.Snappy;
            if(value.ToLower() == "org.apache.cassandra.io.compress.DeflateCompressor".ToLower())
                return CompressionAlgorithm.Deflate;
            if(value.ToLower() == "")
                return CompressionAlgorithm.None;
            throw new ArgumentException(string.Format("Unknown compression algorithm '{0}'", value), "value");
        }

        public static string ToCassandraCompressionAlgorithm(this CompressionAlgorithm value)
        {
            switch(value)
            {
            case CompressionAlgorithm.Snappy:
                return "SnappyCompressor";
            case CompressionAlgorithm.Deflate:
                return "DeflateCompressor";
            case CompressionAlgorithm.None:
                return "";
            default:
                throw new ArgumentException(string.Format("Unknown compression algorithm '{0}'", value), "value");
            }
        }
    }
}