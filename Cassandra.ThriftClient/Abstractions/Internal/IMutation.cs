using Apache.Cassandra;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions.Internal
{
    internal interface IMutation
    {
        Mutation ToCassandraMutation();
    }
}