namespace SKBKontur.Cassandra.CassandraClient.Abstractions.Internal
{
    internal class SliceRange
    {
        public int Count { get; set; }
        public byte[] StartColumn { get; set; }
        public byte[] EndColumn { get; set; }
        public bool Reversed { get; set; }
    }

    internal static class SliceRangeExtensions
    {
        internal static Apache.Cassandra.SliceRange ToCassandraSliceRange(this SliceRange sliceRange)
        {
            if (sliceRange == null)
                return null;
            return new Apache.Cassandra.SliceRange
                {
                    Count = sliceRange.Count,
                    Finish = sliceRange.EndColumn ?? new byte[0],
                    Reversed = sliceRange.Reversed,
                    Start = sliceRange.StartColumn ?? new byte[0]
                };
        }
    }
}