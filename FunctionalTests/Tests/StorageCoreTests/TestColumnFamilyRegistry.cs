using System;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.StorageCore;
using SKBKontur.Cassandra.StorageCore.BlobStorage;
using SKBKontur.Cassandra.StorageCore.RowsStorage;

namespace SKBKontur.Cassandra.FunctionalTests.StorageCoreTests
{
    public class TestColumnFamilyRegistry : IColumnFamilyRegistry, ISerializeToRowsStorageColumnFamilyNameGetter, ISerializeToBlobStorageColumnFamilyNameGetter
    {
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
            return new[]
                {
                    new IndexDefinition {Name = "StringProperty", ValidationClass = ValidationClass.UTF8Type},
                    new IndexDefinition {Name = "IntProperty", ValidationClass = ValidationClass.UTF8Type},
                    new IndexDefinition {Name = "ComplexProperty.StringProperty", ValidationClass = ValidationClass.UTF8Type},
                    new IndexDefinition {Name = "ComplexProperty.IntProperty", ValidationClass = ValidationClass.UTF8Type}
                };
        }

        #endregion

        public string GetColumnFamilyName(Type type)
        {
            return type.Name;
        }

        public bool TryGetColumnFamilyName(Type type, out string columnFamilyName)
        {
            columnFamilyName = type.Name;
            return true;
        }

        private readonly string[] columnFamilyNames = new[] {typeof(TestStorageElement).Name, typeof(TestObject).Name};
    }
}