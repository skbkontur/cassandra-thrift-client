namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class CompressionOptions
    {
        public int? ChunkLengthInKb { get; set; }
        public double? CrcCheckChance { get; set; }
    }
}