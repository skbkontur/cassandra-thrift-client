using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    internal static class ColumnExtensions
    {
        public static Apache.Cassandra.Column ToCassandraColumn(this RawColumn column)
        {
            if(column == null)
                return null;
            var result = new Apache.Cassandra.Column
                {
                    Name = column.Name,
                    Value = column.Value,
                    Timestamp = column.Timestamp ?? DateTimeService.UtcNow.Ticks,
                };
            if(column.TTL.HasValue)
                result.Ttl = column.TTL.Value;
            return result;
        }

        public static RawColumn FromCassandraColumn(this Apache.Cassandra.Column cassandraColumn)
        {
            if(cassandraColumn == null)
                return null;
            var result = new RawColumn();
            if(cassandraColumn.__isset.ttl)
                result.TTL = cassandraColumn.Ttl;
            result.Name = cassandraColumn.Name;
            result.Timestamp = cassandraColumn.Timestamp;
            result.Value = cassandraColumn.Value;
            return result;
        }

        public static RawColumn ToRawColumn(this Column column)
        {
            if(column == null)
                return null;
            var result = new RawColumn
                {
                    Name = StringExtensions.StringToBytes(column.Name),
                    Value = column.Value,
                    Timestamp = column.Timestamp ?? DateTimeService.UtcNow.Ticks
                };
            if(column.TTL.HasValue)
                result.TTL = column.TTL.Value;
            return result;
        }

        public static Column ToColumn(this RawColumn rawColumn)
        {
            if(rawColumn == null)
                return null;
            var result = new Column();
            if(rawColumn.TTL.HasValue)
                result.TTL = rawColumn.TTL;
            result.Name = StringExtensions.BytesToString(rawColumn.Name);
            result.Timestamp = rawColumn.Timestamp;
            result.Value = rawColumn.Value;
            return result;
        }
    }
}