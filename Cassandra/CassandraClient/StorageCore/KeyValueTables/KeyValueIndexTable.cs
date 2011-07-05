using System.Collections.Generic;
using System.Linq;

using CassandraClient.StorageCore.RowsStorage;

namespace CassandraClient.StorageCore.KeyValueTables
{
    public abstract class KeyValueIndexTable : IKeyValueIndexTable
    {
        protected KeyValueIndexTable(ISerializeToRowsStorage serializeToRowsStorage)
        {
            this.serializeToRowsStorage = serializeToRowsStorage;
        }

        public void AddLinks(params KeyToValue[] links)
        {
            serializeToRowsStorage.Write(links.Select(link => new KeyValuePair<string, KeyToValue>(link.Id, link)).ToArray());
        }

        public void AddLink(string key, string value)
        {
            var keyToValue = new KeyToValue {Key = key, Value = value};
            serializeToRowsStorage.Write(keyToValue.Id, keyToValue);
        }

        public string[] GetValues(string key)
        {
            var query = new KeyToValueSearchQuery {Key = key};
            string[] ids = serializeToRowsStorage.Search<KeyToValue, KeyToValueSearchQuery>(query);
            KeyToValue[] searchResult = serializeToRowsStorage.Read<KeyToValue>(ids);
            return searchResult.Select(item => item.Value).ToArray();
        }

        public string[] GetKeys(string value)
        {
            var query = new KeyToValueSearchQuery {Value = value};
            string[] ids = serializeToRowsStorage.Search<KeyToValue, KeyToValueSearchQuery>(query);
            KeyToValue[] searchResult = serializeToRowsStorage.Read<KeyToValue>(ids);
            return searchResult.Select(item => item.Value).ToArray();
        }

        private readonly ISerializeToRowsStorage serializeToRowsStorage;
    }
}