namespace StorageCore.Exceptions
{
    public class ColumnFamilyNotRegisteredException : StorageCoreException
    {
        public ColumnFamilyNotRegisteredException(string columnFamilyName)
            : base("Column family '{0}' doesn't exist in Registry", columnFamilyName)
        {
        }
    }
}