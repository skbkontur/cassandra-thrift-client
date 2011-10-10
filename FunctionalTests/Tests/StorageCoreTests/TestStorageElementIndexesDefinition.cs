using GroboSerializer;

using SKBKontur.Cassandra.StorageCore;

namespace SKBKontur.Cassandra.FunctionalTests.StorageCoreTests
{
    public class TestStorageElementIndexesDefinition : SearchQueryIndexesDefinition<TestStorageElement>
    {
        public TestStorageElementIndexesDefinition(ISerializer serializer)
            : base(serializer)
        {
        }
    }
}