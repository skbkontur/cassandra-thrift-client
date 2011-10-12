using GroboSerializer;

using SKBKontur.Cassandra.StorageCore;
using SKBKontur.Cassandra.StorageCore.BlobStorage;

namespace SKBKontur.Cassandra.FunctionalTests.StorageCoreTests
{
    public class SerializeToBlobStorageTest : StorageTestBase
    {
        public override void SetUp()
        {
            base.SetUp();

            var serializer = new Serializer(new TestXmlNamespaceFactory());
            var columnFamilyRegistry = new TestColumnFamilyRegistry();
            CassandraInitializer.CreateNewKeyspace(cassandraCluster, columnFamilyRegistry);

            var cassandraCoreSettings = new TestCassandraCoreSettings();
            storage = new SerializeToBlobStorage(cassandraCluster, cassandraCoreSettings, columnFamilyRegistry, serializer, columnFamilyRegistry);
        }

        protected override IStorage GetStorage()
        {
            return storage;
        }

        private SerializeToBlobStorage storage;
    }
}