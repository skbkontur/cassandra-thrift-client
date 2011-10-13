namespace SKBKontur.Cassandra.CassandraClient.Core.Pools
{
    public class KeyspaceConnectionPoolKnowledge
    {
        public int FreeConnectionCount { get; set; }
        public int BusyConnectionCount { get; set; }

        public override string ToString()
        {
            return string.Format("BusyConnectionCount={0}; FreeConnectionCount={1}", BusyConnectionCount, FreeConnectionCount);
        }
    }
}