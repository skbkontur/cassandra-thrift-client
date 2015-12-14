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

        public KeyColumnPair<TNewKey, TNewColumn> Convert<TNewKey, TNewColumn>(Func<TKey, TNewKey> keyConverter, Func<TColumn, TNewColumn> columnConverter)
        {
            return new KeyColumnPair<TNewKey, TNewColumn>(keyConverter(Key), columnConverter(Column));
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