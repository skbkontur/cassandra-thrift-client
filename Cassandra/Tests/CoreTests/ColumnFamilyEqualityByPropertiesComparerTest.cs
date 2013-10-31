using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Scheme;

namespace Cassandra.Tests.CoreTests
{
    [TestFixture]
    public class ColumnFamilyEqualityByPropertiesComparerTest
    {
        [SetUp]
        public void SetUp()
        {
            comparer = new ColumnFamilyEqualityByPropertiesComparer();
        }

        [Test]
        public void TestCompareByReadRepairChanceProperty()
        {
            Assert.That(
                comparer.Equals(
                    new ColumnFamily {Name = "123", ReadRepairChance = 1.0},
                    new ColumnFamily {Name = "123", ReadRepairChance = 1.0}
                    )
                );
            Assert.That(
                !comparer.Equals(
                    new ColumnFamily {Name = "123", ReadRepairChance = 1.0},
                    new ColumnFamily {Name = "123", ReadRepairChance = 1.1}
                    )
                );
        }

        private ColumnFamilyEqualityByPropertiesComparer comparer;
    }
}