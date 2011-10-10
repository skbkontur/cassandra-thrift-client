using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;
using SKBKontur.Cassandra.CassandraClient.Core;

namespace SKBKontur.Cassandra.CassandraClient.Helpers
{
    public static class ColumnConverter
    {
        public static Column ToColumn(this AquilesColumn aquilesColumn)
        {
            return aquilesColumn == null
                       ? null
                       : new Column
                           {
                               Name = StringHelpers.BytesToString(aquilesColumn.ColumnName),
                               Timestamp = aquilesColumn.Timestamp,
                               TTL = aquilesColumn.TTL,
                               Value = aquilesColumn.Value
                           };
        }

        public static AquilesColumn ToAquilesColumn(this Column column)
        {
            return column == null
                       ? null
                       : new AquilesColumn
                           {
                               ColumnName = StringHelpers.StringToBytes(column.Name),
                               Timestamp = column.Timestamp ?? DateTimeService.UtcNow.Ticks,
                               TTL = column.TTL,
                               Value = column.Value
                           };
        }
    }
}