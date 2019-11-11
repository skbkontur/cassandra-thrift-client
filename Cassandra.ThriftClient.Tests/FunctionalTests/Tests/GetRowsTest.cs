using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

using SkbKontur.Cassandra.TimeBasedUuid;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Tests
{
    public class GetRowsTest : CassandraFunctionalTestBase
    {
        [TestCase(20)]
        [TestCase(100)]
        public void TestGetRowsExclusive(int columnsCount)
        {
            const int rowKeysCount = 100;
            const int columnNamesCount = 100;

            string[] rowKeys = new int[rowKeysCount].Select(x => Guid.NewGuid().ToString()).ToArray();
            Array.Sort(rowKeys);
            foreach (string rowKey in rowKeys)
            {
                IEnumerable<Column> columns = new int[columnNamesCount].Select((x, i) => new Column
                    {
                        Name = IntToString(i),
                        Timestamp = Timestamp.Now.Ticks,
                        Value = new byte[] {1, 2, 3}
                    });
                IEnumerable<KeyValuePair<string, IEnumerable<Column>>> keyValuePairs = columns.Select(column => new KeyValuePair<string, IEnumerable<Column>>(rowKey, new[] {column}));
                columnFamilyConnection.BatchInsert(keyValuePairs);
            }

            for (int i = 0; i < columnNamesCount; i++)
            {
                List<KeyValuePair<string, Column[]>> res = columnFamilyConnection.GetRowsExclusive(rowKeys, IntToString(i), columnsCount);
                res.Sort((x, y) => String.Compare(x.Key, y.Key, StringComparison.Ordinal));
                Assert.AreEqual(rowKeysCount, res.Count);
                for (int j = 0; j < res.Count; j++)
                {
                    KeyValuePair<string, Column[]> row = res[j];
                    Assert.AreEqual(row.Key, rowKeys[j]);
                    Column[] columns = row.Value;
                    Assert.AreEqual(Math.Min(columnsCount, columnNamesCount - i - 1), columns.Length);
                    for (int k = 0; k < columns.Length; k++)
                        Assert.AreEqual(columns[k].Name, IntToString(k + i + 1));
                }
            }
        }

        [Test]
        public void TestGetRows()
        {
            const int rowKeysCount = 100;
            const int columnNamesCount = 100;

            var rowKeys = new int[rowKeysCount].Select(x => Guid.NewGuid().ToString()).ToArray();
            Array.Sort(rowKeys);
            foreach (var rowKey in rowKeys)
            {
                var currentRowKey = rowKey;
                var columns = new int[columnNamesCount].Select((x, i) => new Column
                    {
                        Name = IntToString(i),
                        Timestamp = Timestamp.Now.Ticks,
                        Value = Encoding.UTF8.GetBytes(currentRowKey + "_" + IntToString(i))
                    });
                var keyValuePairs = columns.Select(column => new KeyValuePair<string, IEnumerable<Column>>(currentRowKey, new[] {column}));
                columnFamilyConnection.BatchInsert(keyValuePairs);
            }

            for (var i = 0; i < columnNamesCount; i++)
            {
                var intColumnNames = GetRandomColumnIndexesFromRange(0, columnNamesCount, columnNamesCount).ToArray();
                var orderedIntColumnNames = intColumnNames.Distinct().OrderBy(i1 => i1).ToArray();
                var strColumnNames = intColumnNames.Select(IntToString).ToArray();
                var res = columnFamilyConnection.GetRows(rowKeys, strColumnNames.Concat(new[] {100, 101, 102}.Select(IntToString)).ToArray());
                res.Sort((x, y) => String.Compare(x.Key, y.Key, StringComparison.Ordinal));
                Assert.AreEqual(rowKeysCount, res.Count);
                for (var j = 0; j < res.Count; j++)
                {
                    var row = res[j];
                    Assert.AreEqual(row.Key, rowKeys[j]);
                    var columns = row.Value;
                    Assert.AreEqual(orderedIntColumnNames.Length, columns.Length);
                    for (var k = 0; k < columns.Length; k++)
                    {
                        Assert.AreEqual(IntToString(orderedIntColumnNames[k]), columns[k].Name);
                        Assert.AreEqual(row.Key + "_" + IntToString(orderedIntColumnNames[k]), Encoding.UTF8.GetString(columns[k].Value));
                    }
                }
            }
        }

        [Test]
        public void TestGetNonexistentRows()
        {
            const int rowKeysCount = 100;
            const int columnNamesCount = 100;
            var rowKeys = new int[rowKeysCount].Select(x => Guid.NewGuid().ToString()).ToArray();
            var intColumnNames = GetRandomColumnIndexesFromRange(0, columnNamesCount, columnNamesCount).ToArray();
            var strColumnNames = intColumnNames.Select(IntToString).ToArray();
            var res = columnFamilyConnection.GetRows(rowKeys, strColumnNames.Concat(new[] {100, 101, 102}.Select(IntToString)).ToArray());
            Assert.IsEmpty(res);
        }

        private static string IntToString(int x)
        {
            return x.ToString("0000", CultureInfo.InvariantCulture);
        }

        private IEnumerable<int> GetRandomColumnIndexesFromRange(int fromInclusive, int toExclusive, int maxCount)
        {
            Assert.That(maxCount > 1);
            var count = ThreadLocalRandom.Instance.Next(1, maxCount);
            for (var i = 0; i < count; i++)
                yield return ThreadLocalRandom.Instance.Next(fromInclusive, toExclusive);
        }
    }
}