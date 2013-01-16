using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Core;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions.Internal
{
    internal class DeletionMutation : IMutation
    {
        public Mutation ToCassandraMutation()
        {
            return new Mutation
                {
                    Deletion = new Deletion
                        {
                            Predicate = SlicePredicate.ToCassandraSlicePredicate(),
                            Timestamp = Timestamp.HasValue ? Timestamp.Value : DateTimeService.UtcNow.Ticks
                        }
                };
        }

        public long? Timestamp { get; set; }
        public SlicePredicate SlicePredicate { get; set; }
    }
}