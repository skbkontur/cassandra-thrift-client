namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public struct CommandContext
    {
        public override string ToString()
        {
            if (string.IsNullOrEmpty(KeyspaceName))
                return "";
            if (string.IsNullOrEmpty(ColumnFamilyName))
                return $"(Keyspace: {KeyspaceName})";
            return $"(Keyspace: {KeyspaceName}, ColumnFamily: {ColumnFamilyName})";
        }

        public string KeyspaceName { get; set; }
        public string ColumnFamilyName { get; set; }
    }
}