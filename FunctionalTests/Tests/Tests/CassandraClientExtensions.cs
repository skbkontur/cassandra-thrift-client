using System.Text;

namespace Tests.Tests
{
    public static class CassandraClientExtensions
    {
        public static void Add(this ICassandraClient client, string keySpaceName, string columnFamilyName, string key, string columnName,
                               string columnValue, long? timestamp = null, int? ttl = null)
        {
            client.Add(keySpaceName, columnFamilyName, key, columnName, ToBytes(columnValue), timestamp, ttl);
        }

        private static byte[] ToBytes(string str)
        {
            return Encoding.UTF8.GetBytes(str);
        }
    }
}