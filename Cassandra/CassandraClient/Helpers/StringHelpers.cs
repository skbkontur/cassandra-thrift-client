using System;
using System.Text;

namespace SKBKontur.Cassandra.CassandraClient.Helpers
{
    public static class StringHelpers
    {
        [Obsolete("Это внутренный метод CassandraClient'а. Испоьзуйте свою реализацию аналогичного метода")]
        public static string BytesToString(byte[] bytes)
        {
            return bytes == null ? null : Encoding.UTF8.GetString(bytes);
        }

        [Obsolete("Это внутренный метод CassandraClient'а. Испоьзуйте свою реализацию аналогичного метода")]
        public static byte[] StringToBytes(string str)
        {
            return str == null ? null : Encoding.UTF8.GetBytes(str);
        }
    }
}