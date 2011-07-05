using System;

using CassandraClient.Abstractions;
using CassandraClient.StorageCore;
using CassandraClient.StorageCore.RowsStorage;

using GroboSerializer;

namespace Tests.StorageCoreTests
{
    public class TestColumnFamilyRegistry : IColumnFamilyRegistry, ISerializeToRowsStorageColumnFamilyNameGetter
    {
        public TestColumnFamilyRegistry(ISerializer serializer)
        {
            this.serializer = serializer;
        }

        #region IColumnFamilyRegistry Members

        public bool ContainsColumnFamily(string columnFamilyName)
        {
            return columnFamilyName.Contains(columnFamilyName);
        }

        public string[] GetColumnFamilyNames()
        {
            return columnFamilyNames;
        }

        public IndexDefinition[] GetIndexDefinitions(string columnName)
        {
            return new TestStorageElementIndexesDefinition(serializer).IndexDefinitions;
        }

        #endregion

        public string GetColumnFamilyName(Type type)
        {
            return type.Name;
        }

        public Type[] GetRegisteredTypes()
        {
            return new[] {typeof(TestStorageElement)};
        }

        private readonly string[] columnFamilyNames = new[] {typeof(TestStorageElement).Name};
        private readonly ISerializer serializer;
    }
}