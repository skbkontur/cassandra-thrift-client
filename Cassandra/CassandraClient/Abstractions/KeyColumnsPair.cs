using System;
using System.Collections.Generic;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class KeyColumnsPair<TKey, TColumn>
    {
        public KeyColumnsPair(TKey key, IEnumerable<TColumn> column)
        {
            Key = key;
            Columns = column;
        }

        public TKey Key { get; set; }
        public IEnumerable<TColumn> Columns { get; set; }

        public KeyColumnsPair<TNewKey, TColumn> ConvertKey<TNewKey>(Func<TKey, TNewKey> keyConverter)
        {
            return new KeyColumnsPair<TNewKey, TColumn>(keyConverter(Key), Columns);
        }
    }

    public class KeyColumnsPair<TKey> : KeyColumnsPair<TKey, Column>
    {
        public KeyColumnsPair(TKey key, IEnumerable<Column> column)
            : base(key, column)
        {
        }
    }
}