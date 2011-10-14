using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.StorageCore.KeyValueTables
{
    public abstract class KeyValueIndexTable : IKeyValueIndexTable
    {
        protected KeyValueIndexTable(ICassandraCluster cassandraCluster,
                                     ICassandraCoreSettings cassandraCoreSettings)
        {
            connection = cassandraCluster.RetrieveColumnFamilyConnection(cassandraCoreSettings.KeyspaceName, GetColumnFamilyName());
            this.cassandraCoreSettings = cassandraCoreSettings;
        }

        public void AddLinks(params KeyToValue[] links)
        {
            connection.BatchInsert(links.Select(link => new KeyValuePair<string, IEnumerable<Column>>(link.Id, GetColumns(link))));
        }

        public void AddLink(string key, string value)
        {
            var link = new KeyToValue {Key = key, Value = value};
            connection.AddBatch(link.Id, GetColumns(link));
        }

        public void DeleteLink(string key, string value)
        {
            var link = new KeyToValue {Key = key, Value = value};
            connection.DeleteBatch(link.Id, new[] {"Key", "Value"});
        }

        public string[] GetKeys(string value)
        {
            string[] ids = connection.GetRowsWithColumnValue(cassandraCoreSettings.MaximalColumnsCount, "Value", StringHelpers.StringToBytes(value));
            if(ids == null || ids.Length == 0)
                return new string[0];
            List<KeyValuePair<string, Column[]>> rows = connection.GetRows(ids, null, cassandraCoreSettings.MaximalRowsCount);
            return rows.Select(row => StringHelpers.BytesToString(row.Value.First(column => column.Name == "Key").Value)).ToArray();
        }

        public string[] GetValues(string key)
        {
            string[] ids = connection.GetRowsWithColumnValue(cassandraCoreSettings.MaximalColumnsCount, "Key", StringHelpers.StringToBytes(key));
            if(ids == null || ids.Length == 0)
                return new string[0];
            List<KeyValuePair<string, Column[]>> rows = connection.GetRows(ids, null, cassandraCoreSettings.MaximalRowsCount);
            return rows.Select(row => StringHelpers.BytesToString(row.Value.First(column => column.Name == "Value").Value)).ToArray();
        }

        protected abstract string GetColumnFamilyName();

        private static IEnumerable<Column> GetColumns(KeyToValue link)
        {
            return new[]
                {
                    new Column {Name = "Key", Value = StringHelpers.StringToBytes(link.Key)},
                    new Column {Name = "Value", Value = StringHelpers.StringToBytes(link.Value)}
                };
        }

        private readonly ICassandraCoreSettings cassandraCoreSettings;
        private readonly IColumnFamilyConnection connection;
    }
}