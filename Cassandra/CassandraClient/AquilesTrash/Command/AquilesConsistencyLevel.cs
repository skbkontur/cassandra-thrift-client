namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public enum AquilesConsistencyLevel
    {
        ONE = 1,
        QUORUM = 2,
        LOCAL_QUORUM = 3,
        EACH_QUORUM = 4,
        ALL = 5,
        ANY = 6,
    }
}
