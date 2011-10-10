using System.Text;

namespace SKBKontur.Cassandra.CassandraClient.Helpers
{
    public static class StringHelpers
    {
        public static string BytesToString(byte[] bytes)
        {
            return bytes == null ? null : Encoding.UTF8.GetString(bytes);
        }

        public static byte[] StringToBytes(string str)
        {
            return str == null ? null : Encoding.UTF8.GetBytes(str);
        }
    }
}