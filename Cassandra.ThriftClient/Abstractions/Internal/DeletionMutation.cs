using Apache.Cassandra;

using PreciseTimestamp = SkbKontur.Cassandra.TimeBasedUuid.Timestamp;

namespace SkbKontur.Cassandra.ThriftClient.Abstractions.Internal
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
                            Timestamp = Timestamp ?? PreciseTimestamp.Now.Ticks
                        }
                };
        }

        public long? Timestamp { get; set; }
        public SlicePredicate SlicePredicate { get; set; }
    }
}