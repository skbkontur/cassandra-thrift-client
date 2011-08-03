using System;
using System.Linq;
using System.Text;

using CassandraClient.Abstractions;
using CassandraClient.Clusters;
using CassandraClient.Connections;
using CassandraClient.StorageCore.Exceptions;

using GroboSerializer;

namespace CassandraClient.StorageCore.BlobStorage
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
                                        Column[] columns = connection.GetRow(id, null, cassandraCoreSettings.MaximalColumnsCount);
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