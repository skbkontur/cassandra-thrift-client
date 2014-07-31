using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
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
            foreach(string rowKey in rowKeys)
            {
                IEnumerable<Column> columns = new int[columnNamesCount].Select((x, i) => new Column
                    {
                        Name = IntToString(i),
                        Timestamp = DateTime.UtcNow.Ticks,
                        Value = new byte[] {1, 2, 3}
                    });
                IEnumerable<KeyValuePair<string, IEnumerable<Column>>> keyValuePairs = columns.Select(column => new KeyValuePair<string, IEnumerable<Column>>(rowKey, new[] {column}));
                columnFamilyConnection.BatchInsert(keyValuePairs);
            }

            for(int i = 0; i < columnNamesCount; i++)
            {
                List<KeyValuePair<string, Column[]>> res = columnFamilyConnection.GetRowsExclusive(rowKeys, IntToString(i), columnsCount);
                res.Sort((x, y) => String.Compare(x.Key, y.Key, StringComparison.Ordinal));
                Assert.AreEqual(rowKeysCount, res.Count);
                for(int j = 0; j < res.Count; j++)
                {
                    KeyValuePair<string, Column[]> row = res[j];
                    Assert.AreEqual(row.Key, rowKeys[j]);
                    Column[] columns = row.Value;
                    Assert.AreEqual(Math.Min(columnsCount, columnNamesCount - i - 1), columns.Length);
                    for(int k = 0; k < columns.Length; k++)
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
            foreach(var rowKey in rowKeys)
            {
                var currentRowKey = rowKey;
                var columns = new int[columnNamesCount].Select((x, i) => new Column
                    {
                        Name = currentRowKey + ':' + IntToString(i),
                        Timestamp = DateTime.UtcNow.Ticks,
                        Value = BitConverter.GetBytes(i * i)
                    });
                var keyValuePairs = columns.Select(column => new KeyValuePair<string, IEnumerable<Column>>(currentRowKey, new[] {column}));
                columnFamilyConnection.BatchInsert(keyValuePairs);
            }

            for(var i = 0; i < columnNamesCount; i++)
            {
                var intColumnNames = GetRandomColumnIndexesFromRange(0, columnNamesCount, columnNamesCount).ToArray();
                var strColumnNames = intColumnNames.Select(IntToString).ToArray();
                var res = columnFamilyConnection.GetRows(rowKeys, strColumnNames);
                res.Sort((x, y) => String.Compare(x.Key, y.Key, StringComparison.Ordinal));
                Assert.AreEqual(rowKeysCount, res.Count);
                for(var j = 0; j < res.Count; j++)
                {
                    var row = res[j];
                    Assert.AreEqual(row.Key, rowKeys[j]);
                    var columns = row.Value;
                    Assert.AreEqual(strColumnNames.Length, columns.Length);
                    for(var k = 0; k < columns.Length; k++)
                    {
                        Assert.AreEqual(columns[k].Name, row.Key + ':' + IntToString(intColumnNames[k]));
                        CollectionAssert.AreEqual(columns[k].Value, BitConverter.GetBytes(intColumnNames[k]));
                    }
                }
            }
        }

        private string IntToString(int x)
        {
            return x.ToString("0000", CultureInfo.InvariantCulture);
        }

        private IEnumerable<int> GetRandomColumnIndexesFromRange(int fromInclusive, int toExclusive, int maxCount)
        {
            Assert.That(maxCount > 1);
            var count = random.Next(1, maxCount);
            for (var i = 0; i < count; i++)
                yield return random.Next(fromInclusive, toExclusive);
        }

        private readonly Random random = new Random();
    }
}