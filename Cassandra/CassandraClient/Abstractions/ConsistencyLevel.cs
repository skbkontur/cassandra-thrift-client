namespace CassandraClient.Abstractions
{
    public enum ConsistencyLevel
    {
        ALL,
        ANY,
        EACH_QUORUM,
        LOCAL_QUORUM,
        ONE,
        QUORUM
    }
}