using System;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Tests.SchemaTests.Utils
{
    public static class TestSchemaUtils
    {
        public static string GetRandomKeyspaceName()
        {
            return "K_" + Guid.NewGuid().ToString().Substring(0, 10).Replace("-", string.Empty);
        }

        public static string GetRandomColumnFamilyName()
        {
            return "CF_" + Guid.NewGuid().ToString().Substring(0, 10).Replace("-", string.Empty);
        }
    }
}