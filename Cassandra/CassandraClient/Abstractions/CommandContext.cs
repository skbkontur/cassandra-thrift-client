namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public struct CommandContext
    {
        public override string ToString()
        {
            if(string.IsNullOrEmpty(KeyspaceName))
                return "";
            if(string.IsNullOrEmpty(ColumnFamilyName))
                return string.Format("(Keyspace: {0})", KeyspaceName);
            return string.Format("(Keyspace: {0}, ColumnFamily: {1})", KeyspaceName, ColumnFamilyName);
        }

        public string KeyspaceName { get; set; }
        public string ColumnFamilyName { get; set; }
    }
}