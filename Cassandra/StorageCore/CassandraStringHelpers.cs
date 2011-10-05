using System.Text;

namespace StorageCore
{
    public static class CassandraStringHelpers
    {
        public static byte[] StringToBytes(string str)
        {
            return string.IsNullOrEmpty(str) ? new byte[0] : Encoding.UTF8.GetBytes(str);
        }

        public static string BytesToString(byte[] bytes)
        {
            return (bytes == null || bytes.Length == 0) ? null : Encoding.UTF8.GetString(bytes);
        }
    }
}