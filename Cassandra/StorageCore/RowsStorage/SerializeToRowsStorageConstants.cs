namespace SKBKontur.Cassandra.StorageCore.RowsStorage
{
    public class SerializeToRowsStorageConstants
    {
        public const string idColumnName = "3BB854C5-53E8-4B78-99FA-CCE49B3CC759";
        public const string fullObjectColumnName = "7D9FA845-7866-4749-9509-81FF5C905C65";
        public const string formatVersionColumnName = "EC056F9E-6C0C-4F87-8244-6C5052E82F2C";
        public static readonly string[] SpecialColumnNames = new[]{idColumnName,fullObjectColumnName,formatVersionColumnName};
    }
}