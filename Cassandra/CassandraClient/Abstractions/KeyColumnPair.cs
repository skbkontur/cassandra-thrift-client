using System;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class KeyColumnPair<TKey>
    {
        public KeyColumnPair(TKey key, Column column)
        {
            Key = key;
            Column = column;
        }

        public TKey Key { get; set; }

        public Column Column { get; set; }

        public KeyColumnPair<TNewKey> ConvertKey<TNewKey>(Func<TKey, TNewKey> keyConverter)
        {
            return new KeyColumnPair<TNewKey>(keyConverter(Key), Column);
        } 
    }
}