namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    internal class KeyRange
    {
        public static Apache.Cassandra.KeyRange ToCassandraKeyRange(KeyRange keyRange)
        {
            return new Apache.Cassandra.KeyRange
                {
                    Count = keyRange.Count,
                    Start_key = keyRange.StartKey,
                    End_key = keyRange.EndKey
                };
        }

        public static KeyRange FromCassandraKeyRange(Apache.Cassandra.KeyRange keyRange)
        {
            return new KeyRange
                {
                    Count = keyRange.Count,
                    EndKey = keyRange.End_key,
                    StartKey = keyRange.Start_key
                };
        }

        public byte[] StartKey { get; set; }
        public byte[] EndKey { get; set; }
        public int Count { get; set; }
    }
}