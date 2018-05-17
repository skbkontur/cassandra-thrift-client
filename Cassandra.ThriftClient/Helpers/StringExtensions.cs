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
}