using SKBKontur.Cassandra.CassandraClient.Abstractions;

using GroboSerializer;

namespace SKBKontur.Cassandra.StorageCore.RowsStorage
{
    public class Version2Reader : IVersionReader
    {
        private readonly ISerializer serializer;

        public Version2Reader(ISerializer serializer)
        {
            this.serializer = serializer;
        }

        public bool TryReadObject<T>(Column[] allColumns, Column[] specialColumns, out T result) where T : class
        {
            result = null;
            Column fullObjectColumn = GetFullObjectColumn(specialColumns);
            if (fullObjectColumn == null)
                return false;
            result = serializer.Deserialize<T>(fullObjectColumn.Value);
            return true;
        }

        private Column GetFullObjectColumn(Column[] specialColumns)
        {
            foreach(var specialColumn in specialColumns)
            {
                if (specialColumn.Name == SerializeToRowsStorageConstants.fullObjectColumnName)
                    return specialColumn;
            }
            return null;
        }
    }
}