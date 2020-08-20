using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

using NUnit.Framework;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.TimeBasedUuid;

namespace SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Tests
{
    public class GetRegionTest : CassandraFunctionalTestBase
    {
        [Test]
        public void TestRegionFromStart()
        {
            var rowKeys = GenerateGuids(100);
            GenerateAndInsertColumnsWithNumericNames(rowKeys, 100);

            var actualColumns = columnFamilyConnection.GetRegion(rowKeys, null, "019", 1000).ToDictionary(x => x.Key, x => x.Value.ToArray());

            Assert.That(actualColumns.Keys.Count, Is.EqualTo(100));
            foreach (var rowKey in rowKeys)
            {
                Assert.That(actualColumns[rowKey].Length, Is.EqualTo(20));
                Assert.That(actualColumns[rowKey][0].Name, Is.EqualTo("000"));
                Assert.That(actualColumns[rowKey][1].Name, Is.EqualTo("001"));
                Assert.That(actualColumns[rowKey][19].Name, Is.EqualTo("019"));
            }
        }

        [Test]
        public void TestRegionInMiddle()
        {
            var rowKeys = GenerateGuids(100);
            GenerateAndInsertColumnsWithNumericNames(rowKeys, 100);

            var actualColumns = columnFamilyConnection.GetRegion(rowKeys, "042", "058", 1000).ToDictionary(x => x.Key, x => x.Value.ToArray());

            Assert.That(actualColumns.Keys.Count, Is.EqualTo(100));
            foreach (var rowKey in rowKeys)
            {
                Assert.That(actualColumns[rowKey].Length, Is.EqualTo(17));
                Assert.That(actualColumns[rowKey][0].Name, Is.EqualTo("042"));
                Assert.That(actualColumns[rowKey][1].Name, Is.EqualTo("043"));
                Assert.That(actualColumns[rowKey][16].Name, Is.EqualTo("058"));
            }
        }

        [Test]
        public void TestRegionInEnd()
        {
            var rowKeys = GenerateGuids(100);
            GenerateAndInsertColumnsWithNumericNames(rowKeys, 100);

            var actualColumns = columnFamilyConnection.GetRegion(rowKeys, "095", null, 1000).ToDictionary(x => x.Key, x => x.Value.ToArray());

            Assert.That(actualColumns.Keys.Count, Is.EqualTo(100));
            foreach (var rowKey in rowKeys)
            {
                Assert.That(actualColumns[rowKey].Length, Is.EqualTo(5));
                Assert.That(actualColumns[rowKey][0].Name, Is.EqualTo("095"));
                Assert.That(actualColumns[rowKey][1].Name, Is.EqualTo("096"));
                Assert.That(actualColumns[rowKey][4].Name, Is.EqualTo("099"));
            }
        }

        [Test]
        public void TestRegionWithCountLessThanRangeLength()
        {
            var rowKeys = GenerateGuids(100);
            GenerateAndInsertColumnsWithNumericNames(rowKeys, 100);

            var actualColumns = columnFamilyConnection.GetRegion(rowKeys, "001", "025", 20).ToDictionary(x => x.Key, x => x.Value.ToArray());

            Assert.That(actualColumns.Keys.Count, Is.EqualTo(100));
            foreach (var rowKey in rowKeys)
            {
                Assert.That(actualColumns[rowKey].Length, Is.EqualTo(20));
                Assert.That(actualColumns[rowKey][0].Name, Is.EqualTo("001"));
                Assert.That(actualColumns[rowKey][1].Name, Is.EqualTo("002"));
                Assert.That(actualColumns[rowKey][19].Name, Is.EqualTo("020"));
            }
        }

        [Test]
        public void TestRegionWithEmptyRows()
        {
            var rowKeys = GenerateGuids(100);
            GenerateAndInsertColumnsWithNumericNames(rowKeys, 100);
            var emptyRowKeys = GenerateGuids(100);

            var actualColumns = columnFamilyConnection.GetRegion(rowKeys.Concat(emptyRowKeys), "001", "025", 100).ToDictionary(x => x.Key, x => x.Value.ToArray());

            Assert.That(actualColumns.Keys.Count, Is.EqualTo(100));
            foreach (var emptyRowKey in emptyRowKeys)
                Assert.That(actualColumns.Keys, Has.No.EqualTo(emptyRowKey));
        }

        private void GenerateAndInsertColumnsWithNumericNames(string[] rowKeys, int columnPerRowCount)
        {
            var columns = rowKeys.Select(x => new KeyValuePair<string, IEnumerable<Column>>(x, GenerateColumnWithNumericNames(columnPerRowCount).ToArray()));
            columnFamilyConnection.BatchInsert(columns);
        }

        private static IEnumerable<Column> GenerateColumnWithNumericNames(int count)
        {
            var format = new string('0', GetNumberLength(count));
            return Enumerable.Range(0, count).Select(i => new Column
                {
                    Name = i.ToString(format),
                    Timestamp = Timestamp.Now.Ticks,
                    Value = new byte[] {1, 2, 3}
                });
        }

        private static int GetNumberLength(int count)
        {
            return count.ToString(CultureInfo.InvariantCulture).Length;
        }

        private static string[] GenerateGuids(int count)
        {
            return Enumerable.Range(0, count).Select(x => Guid.NewGuid().ToString()).ToArray();
        }
    }
}