using System;
using System.Text;

namespace SKBKontur.Cassandra.CassandraClient.Helpers
{
    internal static class StringExtensions
    {
        public static string BytesToString(byte[] bytes)
        {
            return bytes == null ? null : Encoding.UTF8.GetString(bytes);
        }

        public static byte[] StringToBytes(string value)
        {
            return value == null ? null : Encoding.UTF8.GetBytes(value);
        }
    }

    public static class StringHelpers
    {
        [Obsolete("Это внутренный метод CassandraClient'а. Испоьзуйте свою реализацию аналогичного метода")]
        public static string BytesToString(byte[] bytes)
        {
            return StringExtensions.BytesToString(bytes);
        }

        [Obsolete("Это внутренный метод CassandraClient'а. Испоьзуйте свою реализацию аналогичного метода")]
        public static byte[] StringToBytes(string str)
        {
            return StringExtensions.StringToBytes(str);
        }
    }
}