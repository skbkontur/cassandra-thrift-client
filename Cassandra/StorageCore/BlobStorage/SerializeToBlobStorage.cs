using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using GroboSerializer;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.StorageCore.Exceptions;

namespace SKBKontur.Cassandra.StorageCore.BlobStorage
{
    public class SerializeToBlobStorage : ISerializeToBlobStorage
    {
        public SerializeToBlobStorage(
            ICassandraCluster cassandraCluster,
            ICassandraCoreSettings cassandraCoreSettings,
            IColumnFamilyRegistry columnFamilyRegistry,
            ISerializer serializer,
            ISerializeToBlobStorageColumnFamilyNameGetter serializeToBlobStorageColumnFamilyNameGetter)
        {
            this.cassandraCluster = cassandraCluster;
            this.cassandraCoreSettings = cassandraCoreSettings;
            this.columnFamilyRegistry = columnFamilyRegistry;
            this.serializer = serializer;
            this.serializeToBlobStorageColumnFamilyNameGetter = serializeToBlobStorageColumnFamilyNameGetter;
        }

        public void Write<T>(KeyValuePair<string, T>[] objects) where T : class
        {
            var batch = new List<KeyValuePair<string, IEnumerable<Column>>>();
            foreach(var keyValuePair in objects)
                batch.Add(new KeyValuePair<string, IEnumerable<Column>>(keyValuePair.Key, new[] {GetColumn(keyValuePair.Value)}));
            MakeInConnection<T>(connection => connection.BatchInsert(batch));
        }

        public bool TryRead<T>(string id, out T result) where T : class
        {
            T temp = null;
            MakeInConnection<T>(
                conn =>
                    {
                        Column column;
                        if(conn.TryGetColumn(id, "Content", out column))
                            temp = serializer.Deserialize<T>(column.Value);
                    });
            result = temp;
            return result != null;
        }

        public void Write<T>(string id, T data) where T : class
        {
            MakeInConnection<T>(
                conn =>
                    {
                        var content = serializer.SerializeToBytes(data, false, new UTF8Encoding(false));
                        conn.AddColumn(id, new Column
                            {
                                Name = "Content",
                                Value = content
                            });
                    });
        }

        public void Delete<T>(string id) where T : class
        {
            MakeInConnection<T>(connection =>
                                    {
                                        Column[] columns = connection.GetColumns(id, null, cassandraCoreSettings.MaximalColumnsCount);
                                        connection.DeleteBatch(id, columns.Select(col => col.Name));
                                    });
        }

        public T Read<T>(string id) where T : class
        {
            T result;
            if(!TryRead(id, out result))
                throw new ObjectNotFoundException("Object of type '{0}' with id='{1}' not found", typeof(T), id);
            return result;
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
            if(ids == null) throw new ArgumentNullException("ids");
            if(ids.Length == 0) return new T[0];
            List<KeyValuePair<string, Column[]>> rows = null;
            MakeInConnection<T>(connection => rows = connection.GetRows(ids, null, cassandraCoreSettings.MaximalColumnsCount));
            Dictionary<string, KeyValuePair<string, Column[]>> rowsDict = rows.ToDictionary(row => row.Key);
            return ids.Where(rowsDict.ContainsKey).Select(id => Read<T>(rowsDict[id].Value)).Where(obj => obj != null).ToArray();
        }

        public T ReadOrCreate<T>(string id, Func<string, T> creator) where T : class
        {
            return ReadOrCreate(new[] {id}, creator).Single();
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
                if(rowsDict.ContainsKey(id))
                    obj = Read<T>(rowsDict[id]);
                if(obj == null)
                {
                    var created = creator(id);
                    result.Add(created);
                    rowsDict.Add(id, new[] {GetColumn(created)});
                    newIds.Add(id);
                }
                else result.Add(obj);
            }
            MakeInConnection<T>(conn => conn.BatchInsert(newIds.Select(id => new KeyValuePair<string, IEnumerable<Column>>(id, rowsDict[id]))));
            return result.ToArray();
        }

        public string[] GetIds<T>(string exclusiveStartId, int count) where T : class
        {
            string[] result = null;
            MakeInConnection<T>(conn => { result = conn.GetKeys(exclusiveStartId, count); });
            return result;
        }

        private T Read<T>(Column[] columns) where T : class
        {
            foreach(var column in columns)
            {
                if(column.Name == "Content")
                    return serializer.Deserialize<T>(column.Value);
            }
            return null;
        }

        private Column GetColumn<T>(T data)
        {
            var content = serializer.SerializeToBytes(data, false, new UTF8Encoding(false));
            return new Column
                {
                    Name = "Content",
                    Value = content
                };
        }

        private void MakeInConnection<T>(Action<IColumnFamilyConnection> action)
        {
            var columnFamily = serializeToBlobStorageColumnFamilyNameGetter.GetColumnFamilyName(typeof(T));
            if(!columnFamilyRegistry.ContainsColumnFamily(columnFamily))
                throw new ColumnFamilyNotRegisteredException(columnFamily);
            using(IColumnFamilyConnection connection = cassandraCluster.RetrieveColumnFamilyConnection(cassandraCoreSettings.KeyspaceName, columnFamily))
                action(connection);
        }

        private readonly ICassandraCluster cassandraCluster;
        private readonly ICassandraCoreSettings cassandraCoreSettings;
        private readonly IColumnFamilyRegistry columnFamilyRegistry;
        private readonly ISerializer serializer;
        private readonly ISerializeToBlobStorageColumnFamilyNameGetter serializeToBlobStorageColumnFamilyNameGetter;
    }
}