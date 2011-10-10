namespace SKBKontur.Cassandra.StorageCore.KeyValueTables
{
    public class KeyToValue : KeyToValueSearchQuery
    {
        public string Id { get { return GetId(); } }

        private string GetId()
        {
            return Key + "_" + Value;
        }
    }
}