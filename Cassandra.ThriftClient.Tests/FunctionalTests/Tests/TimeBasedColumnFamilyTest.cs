using System;
using System.Collections.Generic;
using System.Linq;

using Cassandra.ThriftClient.Tests.FunctionalTests.Utils;

using MoreLinq;

using NUnit.Framework;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Connections;
using SkbKontur.Cassandra.ThriftClient.Scheme;
using SkbKontur.Cassandra.TimeBasedUuid;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Tests
{
    public class TimeBasedColumnFamilyTest : CassandraFunctionalTestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            var cfName = Guid.NewGuid().ToString("N").Substring(10);
            cassandraSchemaActualizer.ActualizeKeyspaces(new[]
                {
                    new KeyspaceScheme
                        {
                            Name = KeyspaceName,
                            Configuration = new KeyspaceConfiguration
                                {
                                    ReplicationStrategy = SimpleReplicationStrategy.Create(1),
                                    ColumnFamilies = new[]
                                        {
                                            new ColumnFamily
                                                {
                                                    Name = cfName,
                                                    ComparatorType = new ColumnComparatorType(DataType.TimeUUIDType)
                                                }
                                        }
                                }
                        }
                }, false);
            cfConnection = cassandraCluster.RetrieveColumnFamilyConnectionImplementation(KeyspaceName, cfName);
        }

        [Test]
        public void GetColumns()
        {
            var rowKey = RandomRowKey();
            var timeBasedColumns = RandomColumnsByTimestamp(50);
            Insert(rowKey, timeBasedColumns);
            var expectedColumnNames = timeBasedColumns.OrderBy(x => x.Name).Skip(10).Take(11).Select(x => x.Name).ToArray();
            var rawColumns = cfConnection.GetColumns(rowKey, expectedColumnNames.Select(x => x.ToByteArray()).ToList());
            var actualColumnNames = GetTimeGuidNames(rawColumns);
            Assert.That(actualColumnNames, Is.EqualTo(expectedColumnNames));
        }

        [Test]
        public void GetSlice()
        {
            var rowKey = RandomRowKey();
            var timeBasedColumns = RandomColumnsByTimestamp(100);
            Insert(rowKey, timeBasedColumns);
            var expectedColumnNames = timeBasedColumns.OrderBy(x => x.Name).Skip(10).Take(11).Select(x => x.Name).ToArray();
            var rawColumns = cfConnection.GetRow(rowKey, expectedColumnNames.First().ToByteArray(), expectedColumnNames.Last().ToByteArray(), int.MaxValue, false);
            var actualColumnNames = GetTimeGuidNames(rawColumns);
            Assert.That(actualColumnNames, Is.EqualTo(expectedColumnNames));
        }

        [Test]
        public void VerifyOrder_DifferentTimestamps()
        {
            VerifyOrder(RandomColumnsByTimestamp(1000));
        }

        [Test]
        public void VerifyOrder_DifferentClockSequences()
        {
            VerifyOrder(RandomColumnsByClockSequence(TimeGuidBitsLayout.MaxClockSequence + 1));
        }

        [Test]
        public void VerifyOrder_DifferentNodes()
        {
            VerifyOrder(RandomColumnsByNode(1000));
        }

        private void VerifyOrder(TimeBasedColumn[] timeBasedColumns)
        {
            var rowKey = RandomRowKey();
            Insert(rowKey, timeBasedColumns);
            var rawColumns = cfConnection.GetRow(rowKey, null, int.MaxValue, false);
            var actualColumnNames = GetTimeGuidNames(rawColumns);
            var expectedColumnNames = timeBasedColumns.OrderBy(x => x.Name).Select(x => x.Name).ToArray();
            Assert.That(actualColumnNames, Is.EqualTo(expectedColumnNames));
            Assert.That(expectedColumnNames, Is.Not.EqualTo(timeBasedColumns.Select(x => x.Name).ToArray()));
            //Console.Out.WriteLine("Row {0}:\r\n{1}", new Guid(rowKey), string.Join("\r\n", actualColumnNames.Select(x => x.ToString())));
        }

        private static byte[] RandomRowKey()
        {
            return Guid.NewGuid().ToByteArray();
        }

        private void Insert(byte[] rowKey, TimeBasedColumn[] columns)
        {
            var rawColumns = columns.Select(TimeBasedColumnExtensions.ToRawColumn).ToList();
            cfConnection.BatchInsert(new List<KeyValuePair<byte[], List<RawColumn>>> {new KeyValuePair<byte[], List<RawColumn>>(rowKey, rawColumns)});
        }

        private static TimeGuid[] GetTimeGuidNames(IEnumerable<RawColumn> rawColumns)
        {
            return rawColumns.Select(x => x.ToTimeBasedColumn().Name).ToArray();
        }

        private static TimeBasedColumn[] RandomColumnsByTimestamp(int count)
        {
            var now = Timestamp.Now;
            return Enumerable.Range(0, int.MaxValue).Select(x => new TimeBasedColumn
                {
                    Timestamp = now.Ticks,
                    Name = new TimeGuid(TimeGuidBitsLayout.Format(now + ThreadLocalRandom.Instance.NextTimeSpan(TimeSpan.FromDays(1)), 0, new byte[6])),
                    Value = new byte[0],
                }).DistinctBy(x => x.Name).Take(count).ToArray();
        }

        private static TimeBasedColumn[] RandomColumnsByClockSequence(int count)
        {
            var now = Timestamp.Now;
            return Enumerable.Range(0, int.MaxValue).Select(x => new TimeBasedColumn
                {
                    Timestamp = now.Ticks,
                    Name = new TimeGuid(TimeGuidBitsLayout.Format(now, ThreadLocalRandom.Instance.NextUshort(TimeGuidBitsLayout.MinClockSequence, TimeGuidBitsLayout.MaxClockSequence + 1), new byte[6])),
                    Value = new byte[0],
                }).DistinctBy(x => x.Name).Take(count).ToArray();
        }

        private static TimeBasedColumn[] RandomColumnsByNode(int count)
        {
            var now = Timestamp.Now;
            return Enumerable.Range(0, int.MaxValue).Select(x => new TimeBasedColumn
                {
                    Timestamp = now.Ticks,
                    Name = new TimeGuid(TimeGuidBitsLayout.Format(now, 0, ThreadLocalRandom.Instance.NextBytes(6))),
                    Value = new byte[0],
                }).DistinctBy(x => x.Name).Take(count).ToArray();
        }

        private IColumnFamilyConnectionImplementation cfConnection;
    }
}