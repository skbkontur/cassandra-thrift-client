using CassandraClient.StorageCore;

using Tests.Tests;

namespace Tests.StorageCoreTests
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