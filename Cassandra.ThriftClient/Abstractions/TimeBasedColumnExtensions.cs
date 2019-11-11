using JetBrains.Annotations;

using SkbKontur.Cassandra.TimeBasedUuid;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    internal static class TimeBasedColumnExtensions
    {
        [CanBeNull]
        public static RawColumn ToRawColumn([CanBeNull] this TimeBasedColumn column)
        {
            if (column == null)
                return null;
            return new RawColumn
                {
                    Name = column.Name?.ToByteArray(),
                    Value = column.Value,
                    Timestamp = column.Timestamp,
                    TTL = column.Ttl,
                };
        }

        [CanBeNull]
        public static TimeBasedColumn ToTimeBasedColumn([CanBeNull] this RawColumn column)
        {
            if (column == null)
                return null;
            return new TimeBasedColumn
                {
                    Name = column.Name == null ? null : new TimeGuid(column.Name),
                    Value = column.Value,
                    Timestamp = column.Timestamp,
                    Ttl = column.TTL,
                };
        }
    }
}