using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;

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
            ISerializer serializer, IObjectReader objectReader)
        {
            this.columnFamilyRegistry = columnFamilyRegistry;
            this.serializeToRowsStorageColumnFamilyNameGetter = serializeToRowsStorageColumnFamilyNameGetter;
            this.cassandraCluster = cassandraCluster;
            this.cassandraCoreSettings = cassandraCoreSettings;
            this.serializer = serializer;
            this.objectReader = objectReader;
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
                        var columns = (connection.GetRow(id, null, cassandraCoreSettings.MaximalColumnsCount) ?? new Column[0]).ToDictionary(column => column.Name);
                        Column[] row = GetRow(id, obj);
                        long? timestamp = null;
                        long ticks = DateTime.UtcNow.Ticks;
                        foreach(var column in row)
                        {
                            Column c;
                            if (columns.TryGetValue(column.Name, out c))
                            {
                                long max = Math.Max(ticks, (c.Timestamp ?? 0) + 1);
                                column.Timestamp = max;
                                timestamp = Math.Max(timestamp ?? 0, max);
                            }
                        }
                        connection.AddBatch(id, row);
                        foreach(var column in row)
                        {
                            if(columns.ContainsKey(column.Name))
                                columns.Remove(column.Name);
                        }
                        if(columns.Count > 0)
                            connection.DeleteBatch(id, columns.Keys, timestamp);
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
                        var columns = readData.ToDictionary(item => item.Key, item => ((item.Value ?? new Column[0]).ToDictionary(column => column.Name)));
                        long? timestamp = null;
                        long ticks = DateTime.UtcNow.Ticks;
                        foreach(var row in rows)
                        {
                            Dictionary<string, Column> currentColumns;
                            if(columns.TryGetValue(row.Key, out currentColumns))
                            {
                                foreach (var column in row.Value)
                                {
                                    Column c;
                                    if (currentColumns.TryGetValue(column.Name, out c))
                                    {
                                        long max = Math.Max(ticks, (c.Timestamp ?? 0) + 1);
                                        column.Timestamp = max;
                                        timestamp = Math.Max(timestamp ?? 0, max);
                                    }
                                }
                            }
                        }
                        connection.BatchInsert(rows);
                        foreach(var row in rows)
                        {
                            Dictionary<string, Column> currentColumns;
                            if(columns.TryGetValue(row.Key, out currentColumns))
                            {
                                foreach(var column in row.Value)
                                {
                                    if(currentColumns.ContainsKey(column.Name))
                                        currentColumns.Remove(column.Name);
                                }
                                if(currentColumns.Count == 0)
                                    columns.Remove(row.Key);
                            }
                        }
                        if(columns.Count > 0)
                            connection.BatchDelete(columns.Select(pair => new KeyValuePair<string, IEnumerable<string>>(pair.Key, pair.Value.Keys.ToArray())), timestamp);
                    });
        }
        
        public bool TryRead<T>(string id, out T result) where T : class
        {
            Column[] columns = null;
            MakeInConnection<T>(connection => columns = connection.GetRow(id, null, cassandraCoreSettings.MaximalColumnsCount));
            return objectReader.TryReadObject(columns, out result);
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
            
            Dictionary<string, KeyValuePair<string, Column[]>> rowsDict = rows.ToDictionary(row => row.Key);
            var result = ids.Where(rowsDict.ContainsKey).Select(id => Read<T>(rowsDict[id].Value)).Where(obj => obj != null).ToArray();
            if(result.Length != ids.Length)
                throw new StorageCoreException("Objects not found. Expected {0}, but was {1}", ids.Length, result.Length);
            return result;
        }

        public T[] TryRead<T>(string[] ids) where T : class
        {
            if (ids == null) throw new ArgumentNullException("ids");
            if (ids.Length == 0) return new T[0];
            List<KeyValuePair<string, Column[]>> rows = null;
            MakeInConnection<T>(connection => rows = connection.GetRows(ids, null, cassandraCoreSettings.MaximalColumnsCount));
            Dictionary<string, KeyValuePair<string, Column[]>> rowsDict = rows.ToDictionary(row => row.Key);
            return ids.Where(rowsDict.ContainsKey).Select(id => Read<T>(rowsDict[id].Value)).Where(obj => obj != null).ToArray();
        }

        public string[] GetIds<T>(string exclusiveStartId, int count) where T : class
        {
            string[] result = null;
            MakeInConnection<T>(conn => { result = conn.GetKeys(exclusiveStartId, count); });
            return result;
        }

        public string[] Search<TData, TTemplate>(string exclusiveStartKey, int count, TTemplate template)
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
                                            result = connection.GetRowsWhere(exclusiveStartKey, count, conditions, new[] {SerializeToRowsStorageConstants.idColumnName});
                                        });
            return result;
        }

        public T ReadOrCreate<T>(string id, Func<T> creator) where T : class
        {
            return ReadOrCreate(new[] {id}, id1 => creator()).Single();
        }

        public T[] ReadOrCreate<T>(string[] ids, Func<string, T> creator) where T : class
        {
            if(ids == null) throw new ArgumentNullException("ids");
            if(ids.Length == 0) return new T[0];
            List<KeyValuePair<string, Column[]>> rows = null;
            MakeInConnection<T>(connection => rows = connection.GetRows(ids, null, cassandraCoreSettings.MaximalColumnsCount));
            var rowsDict = rows.ToDictionary(row => row.Key, row => row.Value);
            var result = new List<T>();
            var newIds = new List<string>();
            foreach(var id in ids)
            {
                T obj = null;
                if (rowsDict.ContainsKey(id))
                    obj = Read<T>(rowsDict[id]);
                if (obj == null)
                {
                    var created = creator(id);
                    result.Add(created);
                    rowsDict.Add(id, GetRow(id, created));
                    newIds.Add(id);
                }
                else result.Add(obj);
            }
            MakeInConnection<T>(conn => conn.BatchInsert(newIds.Select(id => new KeyValuePair<string, IEnumerable<Column>>(id, rowsDict[id]))));
            return result.ToArray();
        }

        private void MakeInConnection<T>(Action<IColumnFamilyConnection> action)
        {
            string columnFamily = serializeToRowsStorageColumnFamilyNameGetter.GetColumnFamilyName(typeof(T));
            if(!columnFamilyRegistry.ContainsColumnFamily(columnFamily))
                throw new ColumnFamilyNotRegisteredException(columnFamily);
            using(IColumnFamilyConnection connection = cassandraCluster.RetrieveColumnFamilyConnection(cassandraCoreSettings.KeyspaceName, columnFamily))
                action(connection);
        }

        private T Read<T>(Column[] columns) where T : class
        {
            T result;
            objectReader.TryReadObject(columns, out result);
            return result;
        }

        private Column[] GetRow<T>(string id, T obj)
        {
            var formatVersionColumn = new Column
                {
                    Name = SerializeToRowsStorageConstants.formatVersionColumnName,
                    Value = CassandraStringHelpers.StringToBytes(FormatVersions.version2)
                };
            var fullObjectColumn = new Column
                {
                    Name = SerializeToRowsStorageConstants.fullObjectColumnName,
                    Value = serializer.SerializeToBytes(obj, true, Encoding.UTF8)
                };
            var idColumn = new Column
                {
                    Name = SerializeToRowsStorageConstants.idColumnName,
                    Value = CassandraStringHelpers.StringToBytes(id)
                };
            var nameValueCollection = serializer.SerializeToNameValueCollection(obj);
            var nameValueColumns = nameValueCollection.AllKeys.Select(
                key => new Column
                    {
                        Name = key,
                        Value = CassandraStringHelpers.StringToBytes(nameValueCollection[key])
                    });
            return new[]{formatVersionColumn, fullObjectColumn, idColumn}.Concat(nameValueColumns).ToArray();
        }

        private readonly IColumnFamilyRegistry columnFamilyRegistry;
        private readonly ISerializeToRowsStorageColumnFamilyNameGetter serializeToRowsStorageColumnFamilyNameGetter;
        private readonly ICassandraCluster cassandraCluster;
        private readonly ICassandraCoreSettings cassandraCoreSettings;
        private readonly ISerializer serializer;
        private readonly IObjectReader objectReader;
    }
}