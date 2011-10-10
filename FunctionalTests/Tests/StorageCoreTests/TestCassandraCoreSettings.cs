
using SKBKontur.Cassandra.FunctionalTests.Tests;
using SKBKontur.Cassandra.StorageCore;

namespace SKBKontur.Cassandra.FunctionalTests.StorageCoreTests
{
    public class TestCassandraCoreSettings : ICassandraCoreSettings
    {
        #region ICassandraCoreSettings Members

        public int MaximalColumnsCount { get { return 1000; } }

        public int MaximalRowsCount { get { return 1000; } }

        public string ClusterName { get { return Constants.ClusterName; } }

        public string KeyspaceName { get { return Constants.KeyspaceName; } }

        #endregion
    }
}