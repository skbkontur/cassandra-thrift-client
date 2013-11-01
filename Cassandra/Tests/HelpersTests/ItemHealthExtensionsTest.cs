using System.Linq;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace Cassandra.Tests.HelpersTests
{
    [TestFixture]
    public class ItemHealthExtensionsTest
    {
        [Test]
        public void FirstItemsInShuffledListInRange()
        {
            const int count = 100000;

            var items = new[]
                {
                    new HealthItem(1.0),
                    new HealthItem(0.7),
                    new HealthItem(0.5),
                    new HealthItem(0.1)
                };

            var itemCounts =
                Enumerable.Range(0, count)
                          .Select(x => items.ShuffleByHealth(i => i.Health).First())
                          .GroupBy(x => x)
                          .ToDictionary(x => x.Key, x => x.Count());

            Assert.That(itemCounts[items[0]], Is.InRange(0.40 * count, 0.45 * count));
            Assert.That(itemCounts[items[1]], Is.InRange(0.29 * count, 0.35 * count));
            Assert.That(itemCounts[items[2]], Is.InRange(0.20 * count, 0.25 * count));
            Assert.That(itemCounts[items[3]], Is.InRange(0.03 * count, 0.07 * count));
        }

        [Test]
        public void TestMultipleShuffledListResultLength()
        {
            const int count = 100000;

            var items = new[]
                {
                    new HealthItem(1.0),
                    new HealthItem(0.7),
                    new HealthItem(0.5),
                    new HealthItem(0.1)
                };

            var actualCount =
                Enumerable.Range(0, count)
                          .Select(x => items.ShuffleByHealth(i => i.Health).Count())
                          .Where(x => x == items.Length)
                          .ToArray();

            Assert.That(actualCount.Length, Is.EqualTo(count));
        }

        [Test]
        public void TryShuffleEmptyList()
        {
            var shuffledList = (new HealthItem[] {}).ShuffleByHealth(x => x.Health).ToList();
            Assert.That(shuffledList.Count, Is.EqualTo(0));
        }

        [Test]
        public void TryShuffleMultipleTimesListOfTwoElements()
        {
            const int count = 1000;

            var items = new[]
                {
                    new HealthItem(1.0),
                    new HealthItem(1.0)
                };

            var itemCounts =
                Enumerable.Range(0, count)
                          .Select(x => items.ShuffleByHealth(i => i.Health).ToArray())
                          .GroupBy(x => x[1])
                          .ToDictionary(x => x.Key, x => x.Count());

            Assert.That(itemCounts[items[0]], Is.InRange(0.45 * count, 0.55 * count));
            Assert.That(itemCounts[items[1]], Is.InRange(0.45 * count, 0.55 * count));
        }

        [Test]
        public void TryShuffleListOfTwoElements()
        {
            var items = new[]
                {
                    new HealthItem(1.0),
                    new HealthItem(1.0)
                };

            var itemCounts = items.ShuffleByHealth(i => i.Health).ToArray();
            Assert.That(itemCounts.Length, Is.EqualTo(2));
        }

        private class HealthItem
        {
            public HealthItem(double health)
            {
                Health = health;
            }

            public double Health { get; private set; }
        }
    }
}