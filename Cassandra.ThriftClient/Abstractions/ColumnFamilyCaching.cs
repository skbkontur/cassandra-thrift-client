using System;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public enum ColumnFamilyCaching
    {
        All,
        KeysOnly,
        RowsOnly,
        None,
    }

    public static class ColumnFamilyCachingExtensions
    {
        public static ColumnFamilyCaching ToColumnFamilyCaching(this string value)
        {
            switch (value.ToUpper())
            {
            case "ALL":
                return ColumnFamilyCaching.All;
            case "KEYS_ONLY":
                return ColumnFamilyCaching.KeysOnly;
            case "ROWS_ONLY":
                return ColumnFamilyCaching.RowsOnly;
            case "NONE":
                return ColumnFamilyCaching.None;
            default:
                throw new ArgumentException("value", string.Format("Cannot parse '{0}' to ColumnFamilyCaching", value));
            }
        }

        public static string ToCassandraStringValue(this ColumnFamilyCaching value)
        {
            switch (value)
            {
            case ColumnFamilyCaching.All:
                return "ALL";
            case ColumnFamilyCaching.KeysOnly:
                return "KEYS_ONLY";
            case ColumnFamilyCaching.RowsOnly:
                return "ROWS_ONLY";
            case ColumnFamilyCaching.None:
                return "NONE";
            default:
                throw new ArgumentOutOfRangeException("value");
            }
        }
    }
}