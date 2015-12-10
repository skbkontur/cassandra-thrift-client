using System;
using System.Collections;
using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    public class EnumerableFactory : IEnumerableFactory
    {
        public EnumerableFactory(IColumnFamilyConnectionImplementation implementation)
        {
            this.implementation = implementation;
        }

        public IEnumerable<byte[]> GetRowsEnumerator(int batchSize, byte[] initialStartKey = null)
        {
            return new ObjectsEnumerable<byte[]>(
                exclusiveStartKey => implementation.GetKeys(exclusiveStartKey, batchSize),
                x => x,
                initialStartKey);
        }

        public IEnumerable<RawColumn> GetColumnsEnumerator(byte[] key, int batchSize, byte[] initialStartKey = null)
        {
            return new ObjectsEnumerable<RawColumn>(
                startColumnName => implementation.GetRow(key, startColumnName, batchSize, false),
                col => col.Name,
                initialStartKey);
        }

        private class ObjectsEnumerator<T> : IEnumerator<T>
        {
            public ObjectsEnumerator(Func<byte[], IEnumerable<T>> getObjs, Func<T, byte[]> getKey, byte[] initialStartKey)
            {
                this.getObjs = getObjs;
                this.getKey = getKey;
                this.initialStartKey = initialStartKey;
                exclusiveStartKey = initialStartKey;
                Reset();
            }

            public void Dispose()
            {
                if (objectsEnumerator != null)
                    objectsEnumerator.Dispose();
            }

            public bool MoveNext()
            {
                if(objectsEnumerator == null || !objectsEnumerator.MoveNext())
                {
                    objectsEnumerator = getObjs(exclusiveStartKey).GetEnumerator();
                    if(!objectsEnumerator.MoveNext())
                        return false;
                    if(ByteArrayEqualityComparer.SimpleComparer.Equals(getKey(objectsEnumerator.Current), exclusiveStartKey) && !objectsEnumerator.MoveNext())
                        return false;
                }

                exclusiveStartKey = getKey(objectsEnumerator.Current);
                return true;
            }

            public void Reset()
            {
                exclusiveStartKey = initialStartKey;
                objectsEnumerator = null;
            }

            public T Current { get { return objectsEnumerator.Current; } }
            object IEnumerator.Current { get { return Current; } }

            private IEnumerator<T> objectsEnumerator; 
            private byte[] exclusiveStartKey;
            private readonly Func<T, byte[]> getKey;
            private readonly Func<byte[], IEnumerable<T>> getObjs;
            private readonly byte[] initialStartKey;
        }

        private class ObjectsEnumerable<T> : IEnumerable<T>
        {
            public ObjectsEnumerable(Func<byte[], IEnumerable<T>> getObjs, Func<T, byte[]> getKey, byte[] initialStartKey)
            {
                this.getObjs = getObjs;
                this.getKey = getKey;
                this.initialStartKey = initialStartKey;
            }

            public IEnumerator<T> GetEnumerator()
            {
                return new ObjectsEnumerator<T>(getObjs, getKey, initialStartKey);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private readonly Func<T, byte[]> getKey;
            private readonly Func<byte[], IEnumerable<T>> getObjs;
            private readonly byte[] initialStartKey;
        }

        private readonly IColumnFamilyConnectionImplementation implementation;
    }
}