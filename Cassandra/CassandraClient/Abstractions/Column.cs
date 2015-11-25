using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public interface IColumn
    {
        byte[] RawName { get; set; }
        byte[] Value { get; set; }
        int? TTL { get; set; }
        long? Timestamp { get; set; }
        string Description { get; }
    }

    public class Column : IColumn
    {
        private string name;
        private byte[] rawName;

        public string Name
        {
            get { return name; }
            set
            {
                name = value;
                rawName = StringExtensions.StringToBytes(value);
            }
        }

        public byte[] RawName
        {
            get { return rawName; }
            set
            {
                rawName = value;
                name = StringExtensions.BytesToString(value);
            }
        }

        public byte[] Value { get; set; }
        public int? TTL { get; set; }
        public long? Timestamp { get; set; }
        public string Description { get { return Name; } }
    }

    internal static class ColumnExtensions
    {
        public static Apache.Cassandra.Column ToCassandraColumn<T>(this T column) where T : IColumn
        {
            if(column == null)
                return null;
            var result = new Apache.Cassandra.Column
                {
                    Name = column.RawName,
                    Value = column.Value,
                    Timestamp = column.Timestamp ?? DateTimeService.UtcNow.Ticks,
                };
            if(column.TTL.HasValue)
                result.Ttl = column.TTL.Value;
            return result;
        }

        public static T FromCassandraColumn<T>(this Apache.Cassandra.Column cassandraColumn) where T : class, IColumn, new()
        {
            if(cassandraColumn == null)
                return null;
            var result = new T();
            if(cassandraColumn.__isset.ttl)
                result.TTL = cassandraColumn.Ttl;
            result.RawName = cassandraColumn.Name;
            result.Timestamp = cassandraColumn.Timestamp;
            result.Value = cassandraColumn.Value;
            return result;
        }
    }
}