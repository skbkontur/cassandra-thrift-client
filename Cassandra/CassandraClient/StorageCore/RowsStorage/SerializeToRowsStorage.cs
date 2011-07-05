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
            MakeInConnection<T>(connection => connection.AddBatch(id, GetRow(id, obj)));
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
            IEnumerable<KeyValuePair<string, IEnumerable<Column>>> rows = objects.Select(pair => new KeyValuePair<string, IEnumerable<Column>>(pair.Key, GetRow(pair.Key, pair.Value)));
            MakeInConnection<T>(connection => connection.BatchInsert(rows));
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

        public const string idColumn = "3BB854C5-53E8-4B78-99FA-CCE49B3CC759";

        private void MakeInConnection<T>(Action<IColumnFamilyConnection> action)
        {
            var columnFamily = serializeToRowsStorageColumnFamilyNameGetter.GetColumnFamilyName(typeof(T));
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