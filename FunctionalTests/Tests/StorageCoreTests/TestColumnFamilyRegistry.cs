using System;

using CassandraClient.Abstractions;

using GroboSerializer;

using StorageCore;
using StorageCore.RowsStorage;

namespace SKBKontur.Cassandra.FunctionalTests.StorageCoreTests
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

        private readonly string[] columnFamilyNames = new[] {typeof(TestStorageElement).Name, typeof(TestObject).Name};
        private readonly ISerializer serializer;
    }
}