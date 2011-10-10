namespace SKBKontur.Cassandra.StorageCore.RowsStorage
{
    public interface ISerializeToRowsStorage : IStorage
    {
        string[] Search<TData, TTemplate>(string exclusiveStartKey, int count, TTemplate template)
            where TTemplate : class
            where TData : class;
    }
}