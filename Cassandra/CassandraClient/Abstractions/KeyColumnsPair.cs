using System;
using System.Collections.Generic;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class KeyColumnsPair<TKey>
    {
        public KeyColumnsPair(TKey key, IEnumerable<Column> column)
        {
            Key = key;
            Columns = column;
        }

        public TKey Key { get; set; }

        public IEnumerable<Column> Columns { get; set; }

        public KeyColumnsPair<TNewKey> ConvertKey<TNewKey>(Func<TKey, TNewKey> keyConverter)
        {
            return new KeyColumnsPair<TNewKey>(keyConverter(Key), Columns);
        } 
    }
}