using System.Text;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class ColumnIsNotFoundException : CassandraClientException
    {
        public ColumnIsNotFoundException(string columnFamilyName, byte[] keyName, byte[] columnName)
            : base(string.Format("Column {0} are not found", ColumnToString(columnFamilyName, keyName, columnName)))
        {
        }

        private static string ColumnToString(string columnFamilyName, byte[] keyName, byte[] columnName)
        {
            return string.Format("columnFamily = {0}, key = {1}, column = {2}", columnFamilyName,
                                 Encoding.UTF8.GetString(keyName), Encoding.UTF8.GetString(columnName));
        }
    }
}