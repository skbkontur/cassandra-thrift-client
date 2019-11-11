using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.CassandraClient.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Scheme;

using SkbKontur.Cassandra.TimeBasedUuid;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Tests
{
    public class TimeBasedColumnFamilyConnectionTest : CassandraFunctionalTestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            var cfName = Guid.NewGuid().ToString("N").Substring(10);
            cassandraCluster.ActualizeKeyspaces(new[]
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
                });
            cfConnection = cassandraCluster.RetrieveTimeBasedColumnFamilyConnection(KeyspaceName, cfName);
        }

        [Test]
        public void BatchInsert()
        {
            var rowKey1 = NewRowKey();
            var rowKey2 = NewRowKey();
            var columns1 = NewColumns(1, 2, 3);
            var columns2 = NewColumns(3, 4, 5);
            cfConnection.BatchInsert(new List<Tuple<string, List<TimeBasedColumn>>>
                {
                    Tuple.Create(rowKey1, columns1),
                    Tuple.Create(rowKey2, columns2),
                });
            AssertColumns(cfConnection.GetRange(rowKey1, null, null, int.MaxValue, false), new byte[] {1, 2, 3});
            AssertColumns(cfConnection.GetRange(rowKey2, null, null, int.MaxValue, false), new byte[] {3, 4, 5});
        }

        [Test]
        public void TryGetColumn()
        {
            var rowKey = NewRowKey();
            Insert(rowKey, 1, 2, 3);
            AssertColumn(cfConnection.TryGetColumn(rowKey, ColName(2)), 2);
            Assert.That(cfConnection.TryGetColumn(rowKey, ColName(5)), Is.Null);
        }

        [Test]
        public void GetRange()
        {
            var rowKey = NewRowKey();
            Insert(rowKey, 2, 3, 4, 5);

            AssertColumns(cfConnection.GetRange(rowKey, ColName(null), ColName(null), take : int.MaxValue, reversed : false), new byte[] {2, 3, 4, 5});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(1), ColName(null), take : int.MaxValue, reversed : false), new byte[] {2, 3, 4, 5});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(2), ColName(null), take : int.MaxValue, reversed : false), new byte[] {3, 4, 5});

            AssertColumns(cfConnection.GetRange(rowKey, ColName(null), ColName(6), take : int.MaxValue, reversed : false), new byte[] {2, 3, 4, 5});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(null), ColName(5), take : int.MaxValue, reversed : false), new byte[] {2, 3, 4, 5});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(null), ColName(4), take : int.MaxValue, reversed : false), new byte[] {2, 3, 4});

            AssertColumns(cfConnection.GetRange(rowKey, ColName(1), ColName(5), take : int.MaxValue, reversed : false), new byte[] {2, 3, 4, 5});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(2), ColName(5), take : int.MaxValue, reversed : false), new byte[] {3, 4, 5});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(2), ColName(4), take : int.MaxValue, reversed : false), new byte[] {3, 4});

            AssertColumns(cfConnection.GetRange(rowKey, ColName(3), ColName(3), take : int.MaxValue, reversed : false), new byte[] {});
            Assert.Throws<CassandraClientInvalidRequestException>(() => cfConnection.GetRange(rowKey, ColName(4), ColName(3), take : int.MaxValue, reversed : false));

            AssertColumns(cfConnection.GetRange(rowKey, ColName(2), ColName(4), take : int.MaxValue, reversed : false), new byte[] {3, 4});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(2), ColName(4), take : 3, reversed : false), new byte[] {3, 4});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(2), ColName(4), take : 2, reversed : false), new byte[] {3, 4});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(2), ColName(4), take : 1, reversed : false), new byte[] {3});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(2), ColName(4), take : 0, reversed : false), new byte[] {});
        }

        [Test]
        public void GetRange_Reversed()
        {
            var rowKey = NewRowKey();
            Insert(rowKey, 2, 3, 4, 5);

            AssertColumns(cfConnection.GetRange(rowKey, ColName(null), ColName(null), take : int.MaxValue, reversed : true), new byte[] {5, 4, 3, 2});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(6), ColName(null), take : int.MaxValue, reversed : true), new byte[] {5, 4, 3, 2});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(5), ColName(null), take : int.MaxValue, reversed : true), new byte[] {4, 3, 2});

            AssertColumns(cfConnection.GetRange(rowKey, ColName(6), ColName(null), take : int.MaxValue, reversed : true), new byte[] {5, 4, 3, 2});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(6), ColName(1), take : int.MaxValue, reversed : true), new byte[] {5, 4, 3, 2});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(6), ColName(2), take : int.MaxValue, reversed : true), new byte[] {5, 4, 3, 2});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(6), ColName(3), take : int.MaxValue, reversed : true), new byte[] {5, 4, 3});

            AssertColumns(cfConnection.GetRange(rowKey, ColName(6), ColName(2), take : int.MaxValue, reversed : true), new byte[] {5, 4, 3, 2});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(5), ColName(2), take : int.MaxValue, reversed : true), new byte[] {4, 3, 2});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(5), ColName(3), take : int.MaxValue, reversed : true), new byte[] {4, 3});

            AssertColumns(cfConnection.GetRange(rowKey, ColName(3), ColName(3), take : int.MaxValue, reversed : true), new byte[] {});
            Assert.Throws<CassandraClientInvalidRequestException>(() => cfConnection.GetRange(rowKey, ColName(3), ColName(4), take : int.MaxValue, reversed : true));

            AssertColumns(cfConnection.GetRange(rowKey, ColName(4), ColName(2), take : int.MaxValue, reversed : true), new byte[] {3, 2});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(4), ColName(2), take : 3, reversed : true), new byte[] {3, 2});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(4), ColName(2), take : 2, reversed : true), new byte[] {3, 2});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(4), ColName(2), take : 1, reversed : true), new byte[] {3});
            AssertColumns(cfConnection.GetRange(rowKey, ColName(4), ColName(2), take : 0, reversed : true), new byte[] {});
        }

        private static string NewRowKey()
        {
            return Guid.NewGuid().ToString();
        }

        private static TimeGuid ColName(byte? x)
        {
            return x.HasValue ? new TimeGuid(TimeGuidBitsLayout.GregorianCalendarStart, x.Value, TimeGuidBitsLayout.MinNode) : null;
        }

        private void Insert(string rowKey, params byte[] colNames)
        {
            cfConnection.BatchInsert(new List<Tuple<string, List<TimeBasedColumn>>> {Tuple.Create(rowKey, NewColumns(colNames))});
        }

        private static List<TimeBasedColumn> NewColumns(params byte[] colNames)
        {
            var now = Timestamp.Now;
            return colNames.Select(x => new TimeBasedColumn
                {
                    Name = ColName(x),
                    Value = new[] {x},
                    Timestamp = now.Ticks,
                    Ttl = null,
                }).ToList();
        }

        private static void AssertColumns(TimeBasedColumn[] actual, byte[] expected)
        {
            Assert.That(actual.Length, Is.EqualTo(expected.Length));
            for (var i = 0; i < actual.Length; i++)
                AssertColumn(actual[i], expected[i]);
        }

        private static void AssertColumn(TimeBasedColumn actual, byte expected)
        {
            Assert.That(actual.Name, Is.EqualTo(ColName(expected)));
            Assert.That(actual.Value, Is.EqualTo(new[] {expected}));
        }

        private ITimeBasedColumnFamilyConnection cfConnection;
    }
}