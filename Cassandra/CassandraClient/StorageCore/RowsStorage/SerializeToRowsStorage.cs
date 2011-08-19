using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;

using CassandraClient.Abstractions;
using CassandraClient.Clusters;
using CassandraClient.Connections;
using CassandraClient.StorageCore.Exceptions;

using GroboSerializer;

namespace CassandraClient.StorageCore.RowsStorage
{
    public class SerializeToRowsStorage : ISerializeToRowsStorage
    {
        public SerializeToRowsStorage(
            IColumnFamilyRegistry columnFamilyRegistry,
            ISerializeToRowsStorageColumnFamilyNameGetter serializeToRowsStorageColumnFamilyNameGetter,
            ICassandraCluster cassandraCluster,
            ICassandraCoreSettings cassandraCoreSettings,
            ISerializer serializer)
        {
            this.columnFamilyRegistry = columnFamilyRegistry;
            this.serializeToRowsStorageColumnFamilyNameGetter = serializeToRowsStorageColumnFamilyNameGetter;
            this.cassandraCluster = cassandraCluster;
            this.cassandraCoreSettings = cassandraCoreSettings;
            this.serializer = serializer;
        }

        public void Write<T>(string id, T obj) where T : class
        {
            if(ReferenceEquals(obj, null))
                throw new ArgumentNullException("obj", string.Format("Attempt to save null object of type '{0}'", typeof(T)));
            if(string.IsNullOrEmpty(id))
                throw new ArgumentNullException("id", string.Format("Attempt to save object of type '{0}' with empty id", typeof(T)));
            MakeInConnection<T>(
                connection =>
                    {
                        var columnNames = new HashSet<string>((connection.GetRow(id, null, cassandraCoreSettings.MaximalColumnsCount) ?? new Column[0]).Select(column => column.Name));
                        Column[] row = GetRow(id, obj);
                        connection.AddBatch(id, row);
                        foreach(var column in row)
                        {
                            if(columnNames.Contains(column.Name))
                                columnNames.Remove(column.Name);
                        }
                        if(columnNames.Count > 0)
                            connection.DeleteBatch(id, columnNames);
                    });
        }

        public void Write<T>(KeyValuePair<string, T>[] objects) where T : class
        {
            if(objects == null)
                throw new ArgumentNullException("objects");
            if(objects.Length == 0) return;
            if(objects.Any(pair => ReferenceEquals(pair.Value, null)))
                throw new ArgumentNullException("objects", string.Format("Attempt to save null object of type '{0}'", typeof(T)));
            if(objects.Any(pair => string.IsNullOrEmpty(pair.Key)))
                throw new ArgumentNullException("objects", string.Format("Attempt to save object of type '{0}' with empty id", typeof(T)));
            var rows = objects.Select(pair => new KeyValuePair<string, IEnumerable<Column>>(pair.Key, GetRow(pair.Key, pair.Value))).ToArray();
            MakeInConnection<T>(
                connection =>
                    {
                        var readData = connection.GetRows(rows.Select(row => row.Key).ToArray(), null, cassandraCoreSettings.MaximalColumnsCount);
                        var columnNames = readData.ToDictionary(item => item.Key, item => new HashSet<string>((item.Value ?? new Column[0]).Select(column => column.Name)));
                        connection.BatchInsert(rows);
                        foreach(var row in rows)
                        {
                            if(columnNames.ContainsKey(row.Key))
                            {
                                var columns = columnNames[row.Key];
                                foreach(var column in row.Value)
                                {
                                    if(columns.Contains(column.Name))
                                        columns.Remove(column.Name);
                                }
                                if(columns.Count == 0)
                                    columnNames.Remove(row.Key);
                            }
                        }
                        if(columnNames.Count > 0)
                            connection.BatchDelete(columnNames.Select(pair => new KeyValuePair<string, IEnumerable<string>>(pair.Key, pair.Value.ToArray())));
                    });
        }

        public bool TryRead<T>(string id, out T result) where T : class
        {
            result = null;
            Column[] columns = null;
            MakeInConnection<T>(connection => columns = connection.GetRow(id, null, cassandraCoreSettings.MaximalColumnsCount));
            if(columns == null || columns.Length == 0)
                return false;
            result = Read<T>(columns);
            return true;
        }

        public void Delete<T>(string id) where T : class
        {
            MakeInConnection<T>(connection =>
                                    {
                                        Column[] columns = connection.GetRow(id, null, cassandraCoreSettings.MaximalColumnsCount);
                                        connection.DeleteBatch(id, columns.Select(col => col.Name));
                                    });
        }

        public T Read<T>(string id) where T : class
        {
            T res;
            if(!TryRead(id, out res))
                throw new ObjectNotFoundException("Object of type '{0}' with id='{1}' not found", typeof(T), id);
            return res;
        }

        public T[] Read<T>(string[] ids) where T : class
        {
            if(ids == null) throw new ArgumentNullException("ids");
            if(ids.Length == 0) return new T[0];
            List<KeyValuePair<string, Column[]>> rows = null;
            MakeInConnection<T>(connection => rows = connection.GetRows(ids, null, cassandraCoreSettings.MaximalColumnsCount));
            if(rows.Count != ids.Length)
                throw new StorageCoreException("Objects not found. Expected {0}, but was {1}", ids.Length, rows.Count);

            Dictionary<string, KeyValuePair<string, Column[]>> rowsDict = rows.ToDictionary(row => row.Key);
            return ids.Select(id => rowsDict[id]).Select(row => Read<T>(row.Value)).ToArray();
        }

        public string[] GetIds<T>(string greaterThanId, int count) where T : class
        {
            string[] result = null;
            MakeInConnection<T>(conn =>
                                    {
                                        result = conn.GetKeys(greaterThanId, count);
                                    });
            return result;
        }

        public string[] Search<TData, TTemplate>(TTemplate template)
            where TTemplate : class
            where TData : class
        {
            NameValueCollection nameValueCollection = serializer.SerializeToNameValueCollection(template);
            if(nameValueCollection.Count == 0)
                throw new EmptySearchRequestException(typeof(TTemplate).Name);
            string[] result = null;
            MakeInConnection<TData>(connection =>
                                        {
                                            IndexExpression[] conditions = nameValueCollection.AllKeys.Select(
                                                key => new IndexExpression
                                                    {
                                                        ColumnName = key,
                                                        IndexOperator = IndexOperator.EQ,
                                                        Value = CassandraStringHelpers.StringToBytes(nameValueCollection[key])
                                                    }).ToArray();
                                            result = connection.GetRowsWhere(cassandraCoreSettings.MaximalRowsCount, conditions, new[] {idColumn});
                                        });
            return result;
        }

        public T ReadOrCreate<T>(string id) where T : class, new()
        {
            return ReadOrCreate<T>(new[] {id}).Single();
        }

        public T[] ReadOrCreate<T>(string[] ids) where T : class, new()
        {
            if(ids == null) throw new ArgumentNullException("ids");
            if(ids.Length == 0) return new T[0];
            List<KeyValuePair<string, Column[]>> rows = null;
            MakeInConnection<T>(connection => rows = connection.GetRows(ids, null, cassandraCoreSettings.MaximalColumnsCount));
            var rowsDict = rows.ToDictionary(row => row.Key);
            var result = new List<T>();
            var newIds = new List<string>();
            foreach(var id in ids)
            {
                if(!rowsDict.ContainsKey(id))
                {
                    result.Add(new T());
                    newIds.Add(id);
                }
                else result.Add(Read<T>(rowsDict[id].Value));
            }
            MakeInConnection<T>(
                conn =>
                conn.BatchInsert(
                    newIds.Select(id =>
                                      {
                                          var column = new Column {Name = idColumn, Value = CassandraStringHelpers.StringToBytes(id)};
                                          return new KeyValuePair<string, IEnumerable<Column>>(id, new[] {column});
                                      })));
            return result.ToArray();
        }

        public const string idColumn = "3BB854C5-53E8-4B78-99FA-CCE49B3CC759";

        private void MakeInConnection<T>(Action<IColumnFamilyConnection> action)
        {
            string columnFamily = serializeToRowsStorageColumnFamilyNameGetter.GetColumnFamilyName(typeof(T));
            if(!columnFamilyRegistry.ContainsColumnFamily(columnFamily))
                throw new ColumnFamilyNotRegisteredException(columnFamily);
            using(IColumnFamilyConnection connection = cassandraCluster.RetrieveColumnFamilyConnection(cassandraCoreSettings.KeyspaceName, columnFamily))
                action(connection);
        }

        private T Read<T>(IEnumerable<Column> columns)
        {
            var nameValueCollection = new NameValueCollection();
            foreach(var column in columns)
                nameValueCollection.Add(column.Name, CassandraStringHelpers.BytesToString(column.Value));
            return serializer.Deserialize<T>(nameValueCollection);
        }

        private Column[] GetRow<T>(string id, T obj)
        {
            NameValueCollection nameValueCollection = serializer.SerializeToNameValueCollection(obj);
            return nameValueCollection.AllKeys.Select(
                key => new Column
                    {
                        Name = key,
                        Value = CassandraStringHelpers.StringToBytes(nameValueCollection[key])
                    }).Concat(new[] {new Column {Name = idColumn, Value = CassandraStringHelpers.StringToBytes(id)}}).ToArray();
        }

        private readonly IColumnFamilyRegistry columnFamilyRegistry;
        private readonly ISerializeToRowsStorageColumnFamilyNameGetter serializeToRowsStorageColumnFamilyNameGetter;
        private readonly ICassandraCluster cassandraCluster;
        private readonly ICassandraCoreSettings cassandraCoreSettings;
        private readonly ISerializer serializer;
    }
}