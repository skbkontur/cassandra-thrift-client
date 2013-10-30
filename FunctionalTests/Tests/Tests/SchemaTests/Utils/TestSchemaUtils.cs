using System;

namespace SKBKontur.Cassandra.FunctionalTests.Tests.SchemaTests
{
    public static class TestSchemaUtils
    {
        public static string GetRandomKeyspaceName()
        {
            return "K_" + Guid.NewGuid().ToString().Substring(0, 10).Replace("-", string.Empty);
        }       
    }
}