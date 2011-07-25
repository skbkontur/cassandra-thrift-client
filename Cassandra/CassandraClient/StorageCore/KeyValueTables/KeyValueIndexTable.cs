using System.Collections.Generic;
using System.Linq;

using CassandraClient.Abstractions;
using CassandraClient.Clusters;
using CassandraClient.Connections;

namespace CassandraClient.StorageCore.KeyValueTables
{
    public abstract class KeyValueIndexTable : IKeyValueIndexTable
    {
        protected KeyValueIndexTable(ICassandraCluster cassandraCluster,
                                     ICassandraCoreSettings cassandraCoreSettings)
        {
            this.cassandraCluster = cassandraCluster;
            this.cassandraCoreSettings = cassandraCoreSettings;
        }

        public void AddLinks(params KeyToValue[] links)
        {
            using(IColumnFamilyConnection connection = cassandraCluster.RetrieveColumnFamilyConnection(cassandraCoreSettings.KeyspaceName, GetColumnFamilyName()))
                connection.BatchInsert(links.Select(link => new KeyValuePair<string, IEnumerable<Column>>(link.Id, GetColumns(link))));
        }

        public void AddLink(string key, string value)
        {
            var link = new KeyToValue {Key = key, Value = value};
            using(IColumnFamilyConnection connection = cassandraCluster.RetrieveColumnFamilyConnection(cassandraCoreSettings.KeyspaceName, GetColumnFamilyName()))
                connection.AddBatch(link.Id, GetColumns(link));
        }

        public string[] GetKeys(string value)
        {
            using(IColumnFamilyConnection connection = cassandraCluster.RetrieveColumnFamilyConnection(cassandraCoreSettings.KeyspaceName, GetColumnFamilyName()))
            {
                string[] ids = connection.GetRowsWithColumnValue(cassandraCoreSettings.MaximalColumnsCount, "Value", CassandraStringHelpers.StringToBytes(value));
                if (ids == null || ids.Length == 0)
                    return new string[0];
                List<KeyValuePair<string, Column[]>> rows = connection.GetRows(ids, null, cassandraCoreSettings.MaximalRowsCount);
                return rows.Select(row => CassandraStringHelpers.BytesToString(row.Value.First(column => column.Name == "Key").Value)).ToArray();
            }
        }

        public string[] GetValues(string key)
        {
            using(IColumnFamilyConnection connection = cassandraCluster.RetrieveColumnFamilyConnection(cassandraCoreSettings.KeyspaceName, GetColumnFamilyName()))
            {
                string[] ids = connection.GetRowsWithColumnValue(cassandraCoreSettings.MaximalColumnsCount, "Key", CassandraStringHelpers.StringToBytes(key));
                if (ids == null || ids.Length == 0)
                    return new string[0];
                List<KeyValuePair<string, Column[]>> rows = connection.GetRows(ids, null, cassandraCoreSettings.MaximalRowsCount);
                return rows.Select(row => CassandraStringHelpers.BytesToString(row.Value.First(column => column.Name == "Value").Value)).ToArray();
            }
        }

        protected abstract string GetColumnFamilyName();

        private static IEnumerable<Column> GetColumns(KeyToValue link)
        {
            return new[]
                {
                    new Column {Name = "Key", Value = CassandraStringHelpers.StringToBytes(link.Key)},
                    new Column {Name = "Value", Value = CassandraStringHelpers.StringToBytes(link.Value)}
                };
        }

        private readonly ICassandraCluster cassandraCluster;
        private readonly ICassandraCoreSettings cassandraCoreSettings;
    }
}