using System.Collections.Specialized;

using CassandraClient.Abstractions;

using GroboSerializer;

namespace StorageCore.RowsStorage
{
    public class Version1Reader : IVersionReader
    {
        public Version1Reader(ISerializer serializer)
        {
            this.serializer = serializer;
        }

        public bool TryReadObject<T>(Column[] allColumns, Column[] specialColumns, out T result) where T : class
        {
            result = null;
            if(allColumns.Length == 0)
                return false;
            var nvc = new NameValueCollection();
            foreach(var column in allColumns)
                nvc.Add(column.Name, CassandraStringHelpers.BytesToString(column.Value));
            result = serializer.Deserialize<T>(nvc);
            return true;
        }

        private readonly ISerializer serializer;
    }
}