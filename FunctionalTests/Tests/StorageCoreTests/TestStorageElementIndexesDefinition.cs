using CassandraClient.StorageCore;

using GroboSerializer;

namespace Tests.StorageCoreTests
{
    public class TestStorageElementIndexesDefinition : SearchQueryIndexesDefinition<TestStorageElement>
    {
        public TestStorageElementIndexesDefinition(ISerializer serializer)
            : base(serializer)
        {
        }
    }
}