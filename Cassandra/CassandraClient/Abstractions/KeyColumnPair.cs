using System;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class KeyColumnPair<TKey, TColumn>
    {
        public KeyColumnPair(TKey key, TColumn column)
        {
            Key = key;
            Column = column;
        }

        public TKey Key { get; set; }
        public TColumn Column { get; set; }

        public KeyColumnPair<TNewKey, TColumn> ConvertKey<TNewKey>(Func<TKey, TNewKey> keyConverter)
        {
            return new KeyColumnPair<TNewKey, TColumn>(keyConverter(Key), Column);
        }
    }

    public class KeyColumnPair<TKey> : KeyColumnPair<TKey, Column>
    {
        public KeyColumnPair(TKey key, Column column)
            : base(key, column)
        {
        }
    }
}