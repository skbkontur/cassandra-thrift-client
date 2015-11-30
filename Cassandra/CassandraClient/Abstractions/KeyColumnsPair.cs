using System;
using System.Collections.Generic;
using System.Linq;

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

        public KeyColumnsPair<TNewKey, TNewColumn> Convert<TNewKey, TNewColumn>(Func<TKey, TNewKey> keyConverter, Func<TColumn, TNewColumn> columnConverter)
        {
            return new KeyColumnsPair<TNewKey, TNewColumn>(keyConverter(Key), Columns.Select(columnConverter));
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