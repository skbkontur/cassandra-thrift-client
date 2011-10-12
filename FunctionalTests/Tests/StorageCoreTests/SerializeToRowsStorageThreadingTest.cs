using System;
using System.Text;
using System.Threading;

using Cassandra.Tests;

using GroboSerializer;

using NUnit.Framework;

using SKBKontur.Cassandra.FunctionalTests.Tests;
using SKBKontur.Cassandra.StorageCore.RowsStorage;

namespace SKBKontur.Cassandra.FunctionalTests.StorageCoreTests
{
    public class SerializeToRowsStorageThreadingTest : CassandraFunctionalTestBase
    {
        #region Setup/Teardown

        public override void SetUp()
        {
            base.SetUp();

            serializer = new Serializer(new TestXmlNamespaceFactory());
            var columnFamilyRegistry = new TestColumnFamilyRegistry();
            CassandraInitializer.CreateNewKeyspace(cassandraCluster, columnFamilyRegistry);

            var cassandraCoreSettings = new TestCassandraCoreSettings();
            storage = new SerializeToRowsStorage(columnFamilyRegistry, columnFamilyRegistry, cassandraCluster, cassandraCoreSettings,
                                                 serializer, new ObjectReader(new VersionReaderCollection(serializer)));
        }

        #endregion

        [Test]
        public void TestReadReadsCorrectObjectWhenWriting()
        {
            var writeThread = new Thread(WriteLoop);
            var readThread = new Thread(ReadLoop);
            writeThread.Start();
            readThread.Start();
            isStarted = true;

            writeThread.Join();
            readThread.Join();

            if(lastWriteException != null)
                throw lastWriteException;
            if(lastReadException != null)
                throw lastReadException;

            storage.Read<TestObject>("id").AssertEqualsTo(GetTestObject(count - 1));
        }

        private void WriteLoop()
        {
            while(!isStarted)
            {
            }
            for(int i = 0; i < count; i++)
            {
                if (lastWriteException != null || lastReadException != null) break;
                try
                {
                    WriteObject(i);
                    if (i % 1000 == 0)
                        Console.WriteLine(i + " writes");
                }
                catch(Exception e)
                {
                    lastWriteException = e;
                    Console.WriteLine(e);
                    throw;
                }
            }
        }

        private void ReadLoop()
        {
            while(!isStarted)
            {
            }
            TestObject testObject;
            while (!storage.TryRead("id",out testObject)){}
            for(int i = 0; i < count; i++)
            {
                if (lastWriteException != null || lastReadException != null) break;
                try
                {
                    ReadAndCheck();
                    if (i % 1000 == 0)
                        Console.WriteLine(i + " reads");
                }
                catch(Exception e)
                {
                    lastReadException = e;
                    Console.WriteLine(e);
                    throw;
                }
            }
        }

        private TestObject GetTestObject(int index)
        {
            string value = "FieldValue_" + index;
            return new TestObject
                {
                    Field1 = value,
                    Field2 = value,
                    Field3 = value,
                    Field4 = value,
                    Field5 = value,
                    Field6 = value,
                    Field7 = value,
                    Field8 = value,
                    Field9 = value
                };
        }

        private void WriteObject(int index)
        {
            storage.Write("id", GetTestObject(index));
        }

        private void ReadAndCheck()
        {
            var obj = storage.Read<TestObject>("id");
            readsCount++;
            CheckObject(obj);
        }

        private void CheckObject(TestObject obj)
        {
            try
            {
                Assert.AreEqual(obj.Field1, obj.Field2);
                Assert.AreEqual(obj.Field1, obj.Field3);
                Assert.AreEqual(obj.Field1, obj.Field4);
                Assert.AreEqual(obj.Field1, obj.Field5);
                Assert.AreEqual(obj.Field1, obj.Field6);
                Assert.AreEqual(obj.Field1, obj.Field7);
                Assert.AreEqual(obj.Field1, obj.Field8);
                Assert.AreEqual(obj.Field1, obj.Field9);
            }
            catch(Exception)
            {
                Console.WriteLine("Bad object:\r\n" + serializer.SerializeToString(obj, true, new UTF8Encoding(false)));
                throw;
            }
        }

        private volatile int readsCount;

        private volatile bool isStarted;
        private volatile Exception lastReadException;
        private volatile Exception lastWriteException;

        private SerializeToRowsStorage storage;
        private Serializer serializer;
        private const int count = 10000;
    }
}