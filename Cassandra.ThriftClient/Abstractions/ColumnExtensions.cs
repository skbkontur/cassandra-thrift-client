using SkbKontur.Cassandra.ThriftClient.Helpers;
using SkbKontur.Cassandra.TimeBasedUuid;

namespace SkbKontur.Cassandra.ThriftClient.Abstractions
{
    internal static class ColumnExtensions
    {
        public static Apache.Cassandra.Column ToCassandraColumn(this RawColumn column)
        {
            if (column == null)
                return null;
            var result = new Apache.Cassandra.Column
                {
                    Name = column.Name,
                    Value = column.Value,
                    Timestamp = column.Timestamp ?? Timestamp.Now.Ticks,
                };
            if (column.TTL.HasValue)
                result.Ttl = column.TTL.Value;
            return result;
        }

        public static RawColumn FromCassandraColumn(this Apache.Cassandra.Column cassandraColumn)
        {
            if (cassandraColumn == null)
                return null;
            var result = new RawColumn
                {
                    Name = cassandraColumn.Name,
                    Timestamp = cassandraColumn.Timestamp,
                    Value = cassandraColumn.Value
                };
            if (cassandraColumn.__isset.ttl)
                result.TTL = cassandraColumn.Ttl;
            return result;
        }

        public static RawColumn ToRawColumn(this Column column)
        {
            if (column == null)
                return null;
            return new RawColumn
                {
                    Name = StringExtensions.StringToBytes(column.Name),
                    Value = column.Value,
                    Timestamp = column.Timestamp,
                    TTL = column.TTL,
                };
        }

        public static Column ToColumn(this RawColumn rawColumn)
        {
            if (rawColumn == null)
                return null;
            return new Column
                {
                    Name = StringExtensions.BytesToString(rawColumn.Name),
                    Timestamp = rawColumn.Timestamp,
                    Value = rawColumn.Value,
                    TTL = rawColumn.TTL,
                };
        }
    }
}