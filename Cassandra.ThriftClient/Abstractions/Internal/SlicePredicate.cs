using System.Collections.Generic;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions.Internal
{
    internal class SlicePredicate
    {
        public SlicePredicate(List<byte[]> columns)
        {
            Columns = columns ?? new List<byte[]>();
        }

        public SlicePredicate(SliceRange sliceRange)
        {
            SliceRange = sliceRange ?? new SliceRange {Count = int.MaxValue};
        }

        public List<byte[]> Columns { get; private set; }
        public SliceRange SliceRange { get; private set; }
    }

    internal static class SlicePredicateExtensions
    {
        internal static Apache.Cassandra.SlicePredicate ToCassandraSlicePredicate(this SlicePredicate slicePredicate)
        {
            if (slicePredicate == null)
                return null;
            return new Apache.Cassandra.SlicePredicate
                {
                    Column_names = slicePredicate.Columns,
                    Slice_range = slicePredicate.SliceRange.ToCassandraSliceRange()
                };
        }
    }
}