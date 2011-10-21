using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.CassandraClient.Helpers;

using GroboSerializer;

using NUnit.Framework;

using Rhino.Mocks;

using SKBKontur.Cassandra.StorageCore;
using SKBKontur.Cassandra.StorageCore.Exceptions;
using SKBKontur.Cassandra.StorageCore.RowsStorage;

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
            columnFamilyTypeMapper = GetMock<ISerializeToRowsStorageColumnFamilyTypeMapper>();
            cassandraCoreSettings = GetMock<ICassandraCoreSettings>();
            objectReader = GetMock<IObjectReader>();
            serializeToRowsStorage = new SerializeToRowsStorage(columnFamilyRegistry, columnFamilyTypeMapper, cassandraCluster, cassandraCoreSettings, serializer, objectReader);
            columnFamilyConnection = GetMock<IColumnFamilyConnection>();
        }

        [Test]
        public void TestWrite()
        {
            var intvar = new Int {Intf = 5};
            columnFamilyTypeMapper.Expect(items => items.GetColumnFamilyName(typeof(Int))).Return("IntType");
            columnFamilyRegistry.Expect(cfr => cfr.ContainsColumnFamily("IntType")).Return(true);
            cassandraCoreSettings.Expect(settings => settings.KeyspaceName).Return("KeyspaceName");
            cassandraCoreSettings.Expect(settings => settings.MaximalColumnsCount).Return(1234);
            cassandraCluster.Expect(cluster => cluster.RetrieveColumnFamilyConnection("KeyspaceName", "IntType")).Return(columnFamilyConnection);
            var collection = new NameValueCollection {{"A", "2"}, {"B", "4"}};
            serializer.Expect(writer => writer.SerializeToNameValueCollection(intvar)).Return(collection);
            serializer.Expect(s => s.SerializeToBytes(intvar, true, Encoding.UTF8)).Return(new byte[]{5,6,4,3});
            columnFamilyConnection.Expect(connection => connection.GetColumns("q", null, 1234)).Return(new Column[0]);
            columnFamilyConnection.Expect(
                familyConnection => familyConnection.AddBatch(ARG.EqualsTo("q"),
                                                              ARG.EqualsTo(new[]
                                                                  {
                                                                      new Column
                                                                          {
                                                                              Name = formatVersionColumnName,
                                                                              Value = StringHelpers.StringToBytes("v2")
                                                                          },
                                                                      new Column
                                                                          {
                                                                              Name = fullObjectColumnName,
                                                                              Value = new byte[]{5,6,4,3}
                                                                          },
                                                                      new Column
                                                                          {
                                                                              Name = idColumnName,
                                                                              Value = StringHelpers.StringToBytes("q")
                                                                          },
                                                                      new Column
                                                                          {
                                                                              Name = "A",
                                                                              Value = StringHelpers.StringToBytes("2")
                                                                          },
                                                                      new Column
                                                                          {
                                                                              Name = "B",
                                                                              Value = StringHelpers.StringToBytes("4")
                                                                          }
                                                                  })));
            serializeToRowsStorage.Write("q", intvar);
        }

        [Test]
        public void TestWriteMultiple()
        {
            var int1 = new Int {Intf = 5};
            var int2 = new Int {Intf = 3};
            columnFamilyTypeMapper.Expect(items => items.GetColumnFamilyName(typeof(Int))).Return("IntType");
            columnFamilyRegistry.Expect(cfr => cfr.ContainsColumnFamily("IntType")).Return(true);
            cassandraCoreSettings.Expect(settings => settings.KeyspaceName).Return("KeyspaceName");
            cassandraCoreSettings.Expect(settings => settings.MaximalColumnsCount).Return(1234);
            cassandraCluster.Expect(cluster => cluster.RetrieveColumnFamilyConnection("KeyspaceName", "IntType")).Return(columnFamilyConnection);
            var collection1 = new NameValueCollection {{"A", "2"}, {"B", "4"}};
            var collection2 = new NameValueCollection {{"C", "6"}, {"D", "8"}};
            serializer.Expect(s => s.SerializeToBytes(int1, true, Encoding.UTF8)).Return(new byte[]{1});
            serializer.Expect(s => s.SerializeToBytes(int2, true, Encoding.UTF8)).Return(new byte[]{2});
            serializer.Expect(writer => writer.SerializeToNameValueCollection(int1)).Return(collection1);
            serializer.Expect(writer => writer.SerializeToNameValueCollection(int2)).Return(collection2);
            columnFamilyConnection.Expect(connection => connection.GetRows(ARG.EqualsTo(new[] {"q", "z"}), ARG.EqualsTo<string>(null), ARG.EqualsTo(1234))).Return(new List<KeyValuePair<string, Column[]>>());
            IEnumerable<KeyValuePair<string, IEnumerable<Column>>> keyValuePairs = new[]
                {
                    new KeyValuePair<string, IEnumerable<Column>>("q",
                                                                  new[]
                                                                      {
                                                                          new Column
                                                                              {
                                                                                  Name = formatVersionColumnName,
                                                                                  Value = StringHelpers.StringToBytes("v2")
                                                                              },
                                                                          new Column
                                                                              {
                                                                                  Name = fullObjectColumnName,
                                                                                  Value = new byte[]{1}
                                                                              },
                                                                          new Column
                                                                              {
                                                                                  Name = idColumnName,
                                                                                  Value = StringHelpers.StringToBytes("q")
                                                                              },
                                                                          new Column
                                                                              {
                                                                                  Name = "A",
                                                                                  Value = StringHelpers.StringToBytes("2")
                                                                              },
                                                                          new Column
                                                                              {
                                                                                  Name = "B",
                                                                                  Value = StringHelpers.StringToBytes("4")
                                                                              }
                                                                      }),
                    new KeyValuePair<string, IEnumerable<Column>>("z",
                                                                  new[]
                                                                      {
                                                                          new Column
                                                                              {
                                                                                  Name = formatVersionColumnName,
                                                                                  Value = StringHelpers.StringToBytes("v2")
                                                                              },
                                                                          new Column
                                                                              {
                                                                                  Name = fullObjectColumnName,
                                                                                  Value = new byte[]{2}
                                                                              },
                                                                          new Column
                                                                              {
                                                                                  Name = idColumnName,
                                                                                  Value = StringHelpers.StringToBytes("z")
                                                                              },
                                                                          new Column
                                                                              {
                                                                                  Name = "C",
                                                                                  Value = StringHelpers.StringToBytes("6")
                                                                              },
                                                                          new Column
                                                                              {
                                                                                  Name = "D",
                                                                                  Value = StringHelpers.StringToBytes("8")
                                                                              }
                                                                      })
                };
            columnFamilyConnection.Expect(familyConnection => familyConnection.BatchInsert(ARG.MatchTo<IEnumerable<KeyValuePair<string, IEnumerable<Column>>>>(act => Check(keyValuePairs, act))));
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
            columnFamilyTypeMapper.Expect(cfr => cfr.GetColumnFamilyName(typeof(Int))).Return("IntType");
            columnFamilyRegistry.Expect(cfr => cfr.ContainsColumnFamily("IntType")).Return(true);
            cassandraCoreSettings.Expect(settings => settings.MaximalColumnsCount).Return(1234);
            columnFamilyConnection.Expect(connection => connection.GetColumns("q", null, 1234)).Return(new[]
                {
                    new Column
                        {
                            Name = "A",
                            Value = StringHelpers.StringToBytes("2")
                        },
                    new Column
                        {
                            Name = "B",
                            Value = StringHelpers.StringToBytes("4")
                        }
                });
            columnFamilyConnection.Expect(connection => connection.DeleteBatch(ARG.AreSame("q"), ARG.MatchTo<IEnumerable<string>>(names => Check(new[] {"A", "B"}, names)), ARG.EqualsTo((long?)null)));
            cassandraCoreSettings.Expect(settings => settings.KeyspaceName).Return("KeyspaceName");
            cassandraCluster.Expect(cc => cc.RetrieveColumnFamilyConnection("KeyspaceName", "IntType")).Return(columnFamilyConnection);
            serializeToRowsStorage.Delete<Int>("q");
        }

        [Test]
        public void TestRead()
        {
            columnFamilyTypeMapper.Expect(items => items.GetColumnFamilyName(typeof(Int))).Return("IntType");
            columnFamilyRegistry.Expect(cfr => cfr.ContainsColumnFamily("IntType")).Return(true);
            cassandraCoreSettings.Expect(settings => settings.KeyspaceName).Return("KeyspaceName");
            cassandraCluster.Expect(cluster => cluster.RetrieveColumnFamilyConnection("KeyspaceName", "IntType")).Return(columnFamilyConnection);
            cassandraCoreSettings.Expect(settings => settings.MaximalColumnsCount).Return(1234);
            var columns = new[]
                {
                    new Column
                        {
                            Name = "A",
                            Value = StringHelpers.StringToBytes("2")
                        },
                    new Column
                        {
                            Name = "B",
                            Value = StringHelpers.StringToBytes("4")
                        }
                };
            columnFamilyConnection.Expect(connection => connection.GetColumns("q", null, 1234)).Return(columns);
            var resultObject = new Int {Intf = 123};
            objectReader.Expect(reader => reader.TryReadObject(ARG.AreSame(columns), out ARG.Out(resultObject).Dummy)).Return(true);
            Assert.AreEqual(123, serializeToRowsStorage.Read<Int>("q").Intf);
        }

        [Test]
        public void TestReadMultiple()
        {
            columnFamilyTypeMapper.Expect(items => items.GetColumnFamilyName(typeof(Int))).Return("IntType");
            columnFamilyRegistry.Expect(cfr => cfr.ContainsColumnFamily("IntType")).Return(true);
            cassandraCoreSettings.Expect(settings => settings.KeyspaceName).Return("KeyspaceName");
            cassandraCluster.Expect(cluster => cluster.RetrieveColumnFamilyConnection("KeyspaceName", "IntType")).Return(columnFamilyConnection);
            cassandraCoreSettings.Expect(settings => settings.MaximalColumnsCount).Return(1234);
            var columnsForQ = new[]
                {
                    new Column
                        {
                            Name = "A",
                            Value = StringHelpers.StringToBytes("2")
                        },
                    new Column
                        {
                            Name = "B",
                            Value = StringHelpers.StringToBytes("4")
                        }
                };
            var columnsForZ = new[]
                {
                    new Column
                        {
                            Name = "C",
                            Value = StringHelpers.StringToBytes("6")
                        },
                    new Column
                        {
                            Name = "D",
                            Value = StringHelpers.StringToBytes("8")
                        }
                };
            columnFamilyConnection.Expect(
                connection => connection.GetRows(new[] {"q", "z"}, null, 1234)).Return(new List<KeyValuePair<string, Column[]>>
                    {
                        new KeyValuePair<string, Column[]>("q", columnsForQ),
                        new KeyValuePair<string, Column[]>("z", columnsForZ)
                    });
            var int1 = new Int {Intf = 3};
            var int2 = new Int {Intf = 5};
            objectReader.Expect(reader => reader.TryReadObject(ARG.AreSame(columnsForQ), out ARG.Out(int1).Dummy)).Return(true);
            objectReader.Expect(reader => reader.TryReadObject(ARG.AreSame(columnsForZ), out ARG.Out(int2).Dummy)).Return(true);
            serializeToRowsStorage.Read<Int>(new[] {"q", "z"}).AssertEqualsTo(new[] {int1, int2});
        }

        [Test]
        public void TestReadNotExisting()
        {
            columnFamilyTypeMapper.Expect(items => items.GetColumnFamilyName(typeof(Int))).Return("IntType");
            columnFamilyRegistry.Expect(cfr => cfr.ContainsColumnFamily("IntType")).Return(true);
            cassandraCoreSettings.Expect(settings => settings.KeyspaceName).Return("KeyspaceName");
            cassandraCluster.Expect(cluster => cluster.RetrieveColumnFamilyConnection("KeyspaceName", "IntType")).Return(columnFamilyConnection);
            cassandraCoreSettings.Expect(settings => settings.MaximalColumnsCount).Return(1234);
            var columns = new Column[0];
            columnFamilyConnection.Expect(connection => connection.GetColumns("q", null, 1234)).Return(columns);
            objectReader.Expect(reader => reader.TryReadObject(ARG.AreSame(columns), out ARG.Out<Int>(null).Dummy)).Return(false);
            RunMethodWithException<ObjectNotFoundException>(() => serializeToRowsStorage.Read<Int>("q"));
        }

        [Test]
        public void TestSearch()
        {
            columnFamilyTypeMapper.Expect(items => items.GetColumnFamilyName(typeof(Int))).Return(typeof(Int).Name);
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
                            Value = StringHelpers.StringToBytes("2")
                        },
                    new IndexExpression
                        {
                            ColumnName = "B",
                            IndexOperator = IndexOperator.EQ,
                            Value = StringHelpers.StringToBytes("4")
                        }
                };
            var keys = new[] {"key1", "key2"};
            columnFamilyConnection.Expect(connection => connection.GetRowsWhere(ARG.EqualsTo((string)null), ARG.EqualsTo(4321), ARG.EqualsTo(indexes), ARG.EqualsTo(new[] {idColumnName}))).Return(keys);
            serializeToRowsStorage.Search<Int, IntIndexedFields>(null, 4321, template).AssertEqualsTo(keys);
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
        private ISerializeToRowsStorageColumnFamilyTypeMapper columnFamilyTypeMapper;
        private IObjectReader objectReader;

        private class IntIndexedFields
        {
            public int Intf { get; set; }
        }

        private class Int : IntIndexedFields
        {
        }

        private const string idColumnName = "3BB854C5-53E8-4B78-99FA-CCE49B3CC759";
        private const string fullObjectColumnName = "7D9FA845-7866-4749-9509-81FF5C905C65";
        private const string formatVersionColumnName = "EC056F9E-6C0C-4F87-8244-6C5052E82F2C";
    }
}