namespace SkbKontur.Cassandra.ThriftClient.Core.Pools
{
    public class KeyspaceConnectionPoolKnowledge
    {
        public override string ToString()
        {
            return $"BusyConnectionCount={BusyConnectionCount}; FreeConnectionCount={FreeConnectionCount}";
        }

        public int FreeConnectionCount { get; set; }
        public int BusyConnectionCount { get; set; }
    }
}