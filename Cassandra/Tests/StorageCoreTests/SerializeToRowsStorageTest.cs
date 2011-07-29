using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;

using CassandraClient.Abstractions;
using CassandraClient.Clusters;
using CassandraClient.Connections;
using CassandraClient.StorageCore;
using CassandraClient.StorageCore.Exceptions;
using CassandraClient.StorageCore.RowsStorage;

using GroboSerializer;

using NUnit.Framework;

using Rhino.Mocks;

namespace Cassandra.Tests.StorageCoreTests
{
    public class SerializeToRowsStorageTest : TestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            columnFamilyRegistry = GetMock<IColumnFamilyRegistry>();
            cassandraCluster = GetMock<ICassandraCluster>();
            serializer = GetMock<ISerializer>();
            columnFamilyNameGetter = GetMock<ISerializeToRowsStorageColumnFamilyNameGetter>();
            cassandraCoreSettings = GetMock<ICassandraCoreSettings>();
            serializeToRowsStorage = new SerializeToRowsStorage(columnFamilyRegistry, columnFamilyNameGetter, cassandraCluster, cassandraCoreSettings, serializer);
            columnFamilyConnection = GetMock<IColumnFamilyConnection>();
        }

        [Test]
        public void TestWrite()
        {
            var intvar = new Int {Intf = 5};
            columnFamilyNameGetter.Expect(items => items.GetColumnFamilyName(typeof(Int))).Return("IntType");
            columnFamilyRegistry.Expect(cfr => cfr.ContainsColumnFamily("IntType")).Return(true);
            cassandraCoreSettings.Expect(settings => settings.KeyspaceName).Return("KeyspaceName");
            cassandraCoreSettings.Expect(settings => settings.MaximalColumnsCount).Return(1234);
            cassandraCluster.Expect(cluster => cluster.RetrieveColumnFamilyConnection("KeyspaceName", "IntType")).Return(columnFamilyConnection);
            var collection = new NameValueCollection {{"A", "2"}, {"B", "4"}};
            serializer.Expect(writer => writer.SerializeToNameValueCollection(intvar)).Return(collection);
            columnFamilyConnection.Expect(connection => connection.GetRow("q", null, 1234)).Return(new Column[0]);
            columnFamilyConnection.Expect(
                familyConnection => familyConnection.AddBatch(ARG.EqualsTo("q"),
                                                              ARG.EqualsTo(new[]
                                                                  {
                                                                      new Column
                                                                          {
                                                                              Name = "A",
                                                                              Value = CassandraStringHelpers.StringToBytes("2")
                                                                          },
                                                                      new Column
                                                                          {
                                                                              Name = "B",
                                                                              Value = CassandraStringHelpers.StringToBytes("4")
                                                                          },
                                                                      new Column
                                                                          {
                                                                              Name = SerializeToRowsStorage.idColumn,
                                                                              Value = CassandraStringHelpers.StringToBytes("q")
                                                                          }
                                                                  })));
            columnFamilyConnection.Expect(connection => connection.Dispose());
            serializeToRowsStorage.Write("q", intvar);
        }

        [Test]
        public void TestWriteMultiple()
        {
            var int1 = new Int {Intf = 5};
            var int2 = new Int {Intf = 3};
            columnFamilyNameGetter.Expect(items => items.GetColumnFamilyName(typeof(Int))).Return("IntType");
            columnFamilyRegistry.Expect(cfr => cfr.ContainsColumnFamily("IntType")).Return(true);
            cassandraCoreSettings.Expect(settings => settings.KeyspaceName).Return("KeyspaceName");
            cassandraCoreSettings.Expect(settings => settings.MaximalColumnsCount).Return(1234);
            cassandraCluster.Expect(cluster => cluster.RetrieveColumnFamilyConnection("KeyspaceName", "IntType")).Return(columnFamilyConnection);
            var collection1 = new NameValueCollection {{"A", "2"}, {"B", "4"}};
            var collection2 = new NameValueCollection {{"C", "6"}, {"D", "8"}};
            serializer.Expect(writer => writer.SerializeToNameValueCollection(int1)).Return(collection1);
            serializer.Expect(writer => writer.SerializeToNameValueCollection(int2)).Return(collection2);
            columnFamilyConnection.Expect(connection => connection.GetRows(ARG.EqualsTo(new[] { "q", "z" }), ARG.EqualsTo<string>(null), ARG.EqualsTo(1234))).Return(new List<KeyValuePair<string, Column[]>>());
            IEnumerable<KeyValuePair<string, IEnumerable<Column>>> keyValuePairs = new[]
                {
                    new KeyValuePair<string, IEnumerable<Column>>("q",
                                                                  new[]
                                                                      {
                                                                          new Column
                                                                              {
                                                                                  Name = "A",
                                                                                  Value = CassandraStringHelpers.StringToBytes("2")
                                                                              },
                                                                          new Column
                                                                              {
                                                                                  Name = "B",
                                                                                  Value = CassandraStringHelpers.StringToBytes("4")
                                                                              },
                                                                          new Column
                                                                              {
                                                                                  Name = SerializeToRowsStorage.idColumn,
                                                                                  Value = CassandraStringHelpers.StringToBytes("q")
                                                                              }
                                                                      }),
                    new KeyValuePair<string, IEnumerable<Column>>("z",
                                                                  new[]
                                                                      {
                                                                          new Column
                                                                              {
                                                                                  Name = "C",
                                                                                  Value = CassandraStringHelpers.StringToBytes("6")
                                                                              },
                                                                          new Column
                                                                              {
                                                                                  Name = "D",
                                                                                  Value = CassandraStringHelpers.StringToBytes("8")
                                                                              },
                                                                          new Column
                                                                              {
                                                                                  Name = SerializeToRowsStorage.idColumn,
                                                                                  Value = CassandraStringHelpers.StringToBytes("z")
                                                                              }
                                                                      })
                };
            columnFamilyConnection.Expect(familyConnection => familyConnection.BatchInsert(ARG.MatchTo<IEnumerable<KeyValuePair<string, IEnumerable<Column>>>>(act => Check(keyValuePairs, act))));
            columnFamilyConnection.Expect(connection => connection.Dispose());
            serializeToRowsStorage.Write(new[] {new KeyValuePair<string, Int>("q", int1), new KeyValuePair<string, Int>("z", int2)});
        }

        [Test]
        public void TestWriteNull()
        {
            RunMethodWithException<ArgumentNullException>(() => serializeToRowsStorage.Write("id", (Int)null));
        }

        [Test]
        public void TestWriteIdNull()
        {
            RunMethodWithException<ArgumentNullException>(() => serializeToRowsStorage.Write(null, new Int {Intf = 10}));
        }

        [Test]
        public void TestDelete()
        {
            columnFamilyNameGetter.Expect(cfr => cfr.GetColumnFamilyName(typeof(Int))).Return("IntType");
            columnFamilyRegistry.Expect(cfr => cfr.ContainsColumnFamily("IntType")).Return(true);
            columnFamilyConnection.Expect(connection => connection.Dispose());
            cassandraCoreSettings.Expect(settings => settings.MaximalColumnsCount).Return(1234);
            columnFamilyConnection.Expect(connection => connection.GetRow("q", null, 1234)).Return(new[]
                {
                    new Column
                        {
                            Name = "A",
                            Value = CassandraStringHelpers.StringToBytes("2")
                        },
                    new Column
                        {
                            Name = "B",
                            Value = CassandraStringHelpers.StringToBytes("4")
                        }
                });
            columnFamilyConnection.Expect(connection => connection.DeleteBatch(ARG.AreSame("q"), ARG.MatchTo<IEnumerable<string>>(names => Check(new[] {"A", "B"}, names))));
            cassandraCoreSettings.Expect(settings => settings.KeyspaceName).Return("KeyspaceName");
            cassandraCluster.Expect(cc => cc.RetrieveColumnFamilyConnection("KeyspaceName", "IntType")).Return(columnFamilyConnection);
            serializeToRowsStorage.Delete<Int>("q");
        }

        [Test]
        public void TestRead()
        {
            columnFamilyNameGetter.Expect(items => items.GetColumnFamilyName(typeof(Int))).Return("IntType");
            columnFamilyRegistry.Expect(cfr => cfr.ContainsColumnFamily("IntType")).Return(true);
            cassandraCoreSettings.Expect(settings => settings.KeyspaceName).Return("KeyspaceName");
            cassandraCluster.Expect(cluster => cluster.RetrieveColumnFamilyConnection("KeyspaceName", "IntType")).Return(columnFamilyConnection);
            cassandraCoreSettings.Expect(settings => settings.MaximalColumnsCount).Return(1234);
            columnFamilyConnection.Expect(
                connection => connection.GetRow("q", null, 1234)).Return(new[]
                    {
                        new Column
                            {
                                Name = "A",
                                Value = CassandraStringHelpers.StringToBytes("2")
                            },
                        new Column
                            {
                                Name = "B",
                                Value = CassandraStringHelpers.StringToBytes("4")
                            }
                    });
            var collection = new NameValueCollection {{"A", "2"}, {"B", "4"}};
            serializer.Expect(reader => reader.Deserialize<Int>(ARG.EqualsTo(collection))).Return(new Int {Intf = 3});
            columnFamilyConnection.Expect(connection => connection.Dispose());
            Assert.AreEqual(3, serializeToRowsStorage.Read<Int>("q").Intf);
        }

        [Test]
        public void TestReadMultiple()
        {
            columnFamilyNameGetter.Expect(items => items.GetColumnFamilyName(typeof(Int))).Return("IntType");
            columnFamilyRegistry.Expect(cfr => cfr.ContainsColumnFamily("IntType")).Return(true);
            cassandraCoreSettings.Expect(settings => settings.KeyspaceName).Return("KeyspaceName");
            cassandraCluster.Expect(cluster => cluster.RetrieveColumnFamilyConnection("KeyspaceName", "IntType")).Return(columnFamilyConnection);
            cassandraCoreSettings.Expect(settings => settings.MaximalColumnsCount).Return(1234);
            columnFamilyConnection.Expect(
                connection => connection.GetRows(new[] {"q", "z"}, null, 1234)).Return(new List<KeyValuePair<string, Column[]>>
                    {
                        new KeyValuePair<string, Column[]>("q", new[]
                            {
                                new Column
                                    {
                                        Name = "A",
                                        Value = CassandraStringHelpers.StringToBytes("2")
                                    },
                                new Column
                                    {
                                        Name = "B",
                                        Value = CassandraStringHelpers.StringToBytes("4")
                                    }
                            }),
                        new KeyValuePair<string, Column[]>("z", new[]
                            {
                                new Column
                                    {
                                        Name = "C",
                                        Value = CassandraStringHelpers.StringToBytes("6")
                                    },
                                new Column
                                    {
                                        Name = "D",
                                        Value = CassandraStringHelpers.StringToBytes("8")
                                    }
                            })
                    });
            var collection1 = new NameValueCollection {{"A", "2"}, {"B", "4"}};
            var int1 = new Int {Intf = 3};
            var collection2 = new NameValueCollection {{"C", "6"}, {"D", "8"}};
            var int2 = new Int {Intf = 5};
            serializer.Expect(reader => reader.Deserialize<Int>(ARG.EqualsTo(collection1))).Return(int1);
            serializer.Expect(reader => reader.Deserialize<Int>(ARG.EqualsTo(collection2))).Return(int2);
            columnFamilyConnection.Expect(connection => connection.Dispose());
            serializeToRowsStorage.Read<Int>(new[] {"q", "z"}).AssertEqualsTo(new[] {int1, int2});
        }

        [Test]
        public void TestReadNotExisting()
        {
            columnFamilyNameGetter.Expect(items => items.GetColumnFamilyName(typeof(Int))).Return("IntType");
            columnFamilyRegistry.Expect(cfr => cfr.ContainsColumnFamily("IntType")).Return(true);
            cassandraCoreSettings.Expect(settings => settings.KeyspaceName).Return("KeyspaceName");
            cassandraCluster.Expect(cluster => cluster.RetrieveColumnFamilyConnection("KeyspaceName", "IntType")).Return(columnFamilyConnection);
            cassandraCoreSettings.Expect(settings => settings.MaximalColumnsCount).Return(1234);
            columnFamilyConnection.Expect(
                connection => connection.GetRow("q", null, 1234)).Return(new Column[0]);
            columnFamilyConnection.Expect(connection => connection.Dispose());
            RunMethodWithException<ObjectNotFoundException>(() => serializeToRowsStorage.Read<Int>("q"));
        }

        [Test]
        public void TestSearch()
        {
            columnFamilyNameGetter.Expect(items => items.GetColumnFamilyName(typeof(Int))).Return(typeof(Int).Name);
            columnFamilyRegistry.Expect(cfr => cfr.ContainsColumnFamily("Int")).Return(true);
            cassandraCoreSettings.Expect(settings => settings.KeyspaceName).Return("KeyspaceName");
            cassandraCluster.Expect(cluster => cluster.RetrieveColumnFamilyConnection("KeyspaceName", typeof(Int).Name)).Return(columnFamilyConnection);
            var collection = new NameValueCollection {{"A", "2"}, {"B", "4"}};
            var template = new IntIndexedFields {Intf = 0};
            serializer.Expect(writer => writer.SerializeToNameValueCollection(ARG.EqualsTo(template))).Return(collection);
            var indexes = new[]
                {
                    new IndexExpression
                        {
                            ColumnName = "A",
                            IndexOperator = IndexOperator.EQ,
                            Value = CassandraStringHelpers.StringToBytes("2")
                        },
                    new IndexExpression
                        {
                            ColumnName = "B",
                            IndexOperator = IndexOperator.EQ,
                            Value = CassandraStringHelpers.StringToBytes("4")
                        }
                };
            var keys = new[] {"key1", "key2"};
            cassandraCoreSettings.Expect(settings => settings.MaximalRowsCount).Return(4321);
            columnFamilyConnection.Expect(connection => connection.GetRowsWhere(ARG.EqualsTo(4321), ARG.EqualsTo(indexes), ARG.EqualsTo(new[] {SerializeToRowsStorage.idColumn}))).Return(keys);
            columnFamilyConnection.Expect(connection => connection.Dispose());
            serializeToRowsStorage.Search<Int, IntIndexedFields>(template).AssertEqualsTo(keys);
        }

        private static bool Check<T>(IEnumerable<T> expected, IEnumerable<T> actual)
        {
            actual.ToArray().AssertEqualsTo(expected.ToArray());
            return true;
        }

        private static bool Check(IEnumerable<KeyValuePair<string, IEnumerable<Column>>> expected, IEnumerable<KeyValuePair<string, IEnumerable<Column>>> actual)
        {
            var expectedArray = expected.ToArray();
            var actualArray = actual.ToArray();
            Assert.AreEqual(expectedArray.Length, actualArray.Length);
            for(int i = 0; i < expectedArray.Length; i++)
            {
                Assert.AreEqual(expectedArray[i].Key, actualArray[i].Key);
                Check(expectedArray[i].Value, actualArray[i].Value);
            }
            return true;
        }

        private IColumnFamilyRegistry columnFamilyRegistry;
        private ICassandraCluster cassandraCluster;
        private ISerializer serializer;
        private SerializeToRowsStorage serializeToRowsStorage;
        private IColumnFamilyConnection columnFamilyConnection;
        private ICassandraCoreSettings cassandraCoreSettings;
        private ISerializeToRowsStorageColumnFamilyNameGetter columnFamilyNameGetter;

        private class IntIndexedFields
        {
            public int Intf { get; set; }
        }

        private class Int : IntIndexedFields
        {
        }
    }
}