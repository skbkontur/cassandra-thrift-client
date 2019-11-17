using Apache.Cassandra;

namespace SkbKontur.Cassandra.ThriftClient.Abstractions.Internal
{
    internal interface IMutation
    {
        Mutation ToCassandraMutation();
    }
}