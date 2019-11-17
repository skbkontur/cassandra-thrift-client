namespace SkbKontur.Cassandra.ThriftClient.Abstractions.Internal
{
    internal class KeyRange
    {
        public byte[] StartKey { get; set; }
        public byte[] EndKey { get; set; }
        public int Count { get; set; }
    }

    internal static class KeyRangeExtensions
    {
        internal static Apache.Cassandra.KeyRange ToCassandraKeyRange(this KeyRange keyRange)
        {
            if (keyRange == null)
                return null;
            return new Apache.Cassandra.KeyRange
                {
                    Count = keyRange.Count,
                    Start_key = keyRange.StartKey ?? new byte[0],
                    End_key = keyRange.EndKey ?? new byte[0]
                };
        }
    }
}