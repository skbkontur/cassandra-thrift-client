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
        [Test]
        public void TestGetRows()
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
                List<KeyValuePair<string, Column[]>> res = columnFamilyConnection.GetRowsExclusive(rowKeys, IntToString(i), 100);
                res.Sort((x, y) => String.Compare(x.Key, y.Key, StringComparison.Ordinal));
                Assert.AreEqual(rowKeysCount, res.Count);
                for(int j = 0; j < res.Count; j++)
                {
                    KeyValuePair<string, Column[]> row = res[j];
                    Assert.AreEqual(row.Key, rowKeys[j]);
                    Column[] columns = row.Value;
                    Assert.AreEqual(Math.Min(100, columnNamesCount - i - 1), columns.Length);
                    for(int k = 0; k < columns.Length; k++)
                        Assert.AreEqual(columns[k].Name, IntToString(k + i + 1));
                }
            }
        }

        [Test]
        public void TestGetRows2()
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
                    Timestamp = DateTime.UtcNow.Ticks,
                    Value = new byte[] { 1, 2, 3 }
                });
                IEnumerable<KeyValuePair<string, IEnumerable<Column>>> keyValuePairs = columns.Select(column => new KeyValuePair<string, IEnumerable<Column>>(rowKey, new[] { column }));
                columnFamilyConnection.BatchInsert(keyValuePairs);
            }

            for (int i = 0; i < columnNamesCount; i++)
            {
                List<KeyValuePair<string, Column[]>> res = columnFamilyConnection.GetRowsExclusive(rowKeys, IntToString(i), 20);
                res.Sort((x, y) => String.Compare(x.Key, y.Key, StringComparison.Ordinal));
                Assert.AreEqual(rowKeysCount, res.Count);
                for (int j = 0; j < res.Count; j++)
                {
                    KeyValuePair<string, Column[]> row = res[j];
                    Assert.AreEqual(row.Key, rowKeys[j]);
                    Column[] columns = row.Value;
                    Assert.AreEqual(Math.Min(20, columnNamesCount - i - 1), columns.Length);
                    for (int k = 0; k < columns.Length; k++)
                        Assert.AreEqual(columns[k].Name, IntToString(k + i + 1));
                }
            }
        }

        private string IntToString(int x)
        {
            return x.ToString("0000", CultureInfo.InvariantCulture);
        }
    }
}