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
                    break;
                case "KEYS_ONLY":
                    return ColumnFamilyCaching.KeysOnly;
                    break;
                case "ROWS_ONLY":
                    return ColumnFamilyCaching.RowsOnly;
                    break;
                case "NONE":
                    return ColumnFamilyCaching.None;
                    break;
                default:
                    throw new ArgumentException("value", string.Format("Cannot parse '{0}' to ColumnFamilyCaching", value));
            }
            
        }

        public static string ToCassandraStringValue(this ColumnFamilyCaching value)
        {
            switch(value)
            {
            case ColumnFamilyCaching.All:
                return "ALL";
                break;
            case ColumnFamilyCaching.KeysOnly:
                return "KEYS_ONLY";
                break;
            case ColumnFamilyCaching.RowsOnly:
                return "ROWS_ONLY";
                break;
            case ColumnFamilyCaching.None:
                return "NONE";
                break;
            default:
                throw new ArgumentOutOfRangeException("value");
            }
        }
    }
}