using GroboSerializer;

using StorageCore;

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