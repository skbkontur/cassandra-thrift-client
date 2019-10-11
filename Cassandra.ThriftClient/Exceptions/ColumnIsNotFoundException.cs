using System.Text;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class ColumnIsNotFoundException : CassandraClientException
    {
        internal ColumnIsNotFoundException(string columnFamilyName, byte[] keyName, byte[] columnName)
            : base($"Column {ColumnToString(columnFamilyName, keyName, columnName)} are not found")
        {
        }

        private static string ColumnToString(string columnFamilyName, byte[] keyName, byte[] columnName)
        {
            return $"columnFamily = {columnFamilyName}, key = {Encoding.UTF8.GetString(keyName)}, column = {Encoding.UTF8.GetString(columnName)}";
        }
    }
}