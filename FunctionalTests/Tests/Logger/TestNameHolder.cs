namespace SKBKontur.Cassandra.FunctionalTests.Logger
{
    public static class TestNameHolder
    {
        public static string TestName { get { return testName ?? "UnknownTest"; } set { testName = value; } }
        private static volatile string testName;
    }
}