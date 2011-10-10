namespace SKBKontur.Cassandra.CassandraClient.Core.Pools
{
    public class KeyspaceConnectionPoolKnowledge
    {
        public int FreeConnectionCount { get; set; }
        public int BusyConnectionCount { get; set; }
    }
}