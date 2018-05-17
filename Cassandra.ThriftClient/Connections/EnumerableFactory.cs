using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    internal class EnumerableFactory : IEnumerableFactory
    {
        public EnumerableFactory(IColumnFamilyConnection cfConnection)
        {
            this.cfConnection = cfConnection;
        }

        public IEnumerable<string> GetRowKeysEnumerator(int batchSize)
        {
            return new ObjectsEnumerable<string>(
                exclusiveStartKey => cfConnection.GetKeys(exclusiveStartKey, count: batchSize),
                x => x,
                initialExclusiveStartKey: null);
        }

        public IEnumerable<Column> GetColumnsEnumerator(string key, int batchSize, string initialExclusiveStartColumnName)
        {
            return new ObjectsEnumerable<Column>(
                exclusiveStartColumnName => cfConnection.GetColumns(key, exclusiveStartColumnName, count: batchSize, reversed: false),
                col => col.Name,
                initialExclusiveStartColumnName);
        }

        private readonly IColumnFamilyConnection cfConnection;

        private class ObjectsEnumerator<T> : IEnumerator<T>
        {
            public ObjectsEnumerator(Func<string, T[]> getObjs, Func<T, string> getKey, string initialExclusiveStartKey)
            {
                this.getObjs = getObjs;
                this.getKey = getKey;
                this.initialExclusiveStartKey = initialExclusiveStartKey;
                exclusiveStartKey = initialExclusiveStartKey;
            }

            public void Dispose()
            {
            }

            public bool MoveNext()
            {
                if (++index >= bulk.Length)
                {
                    index = 0;
                    bulk = getObjs(exclusiveStartKey);
                    if (bulk.Length == 0) return false;
                    exclusiveStartKey = getKey(bulk.Last());
                }
                return true;
            }

            public void Reset()
            {
                exclusiveStartKey = initialExclusiveStartKey;
                index = -1;
                bulk = new T[0];
            }

            public T Current { get { return bulk[index]; } }

            object IEnumerator.Current { get { return Current; } }
            private readonly Func<string, T[]> getObjs;
            private readonly Func<T, string> getKey;
            private readonly string initialExclusiveStartKey;
            private string exclusiveStartKey;
            private int index = -1;
            private T[] bulk = new T[0];
        }

        private class ObjectsEnumerable<T> : IEnumerable<T>
        {
            public ObjectsEnumerable(Func<string, T[]> getObjs, Func<T, string> getKey, string initialExclusiveStartKey)
            {
                this.getObjs = getObjs;
                this.getKey = getKey;
                this.initialExclusiveStartKey = initialExclusiveStartKey;
            }

            public IEnumerator<T> GetEnumerator()
            {
                return new ObjectsEnumerator<T>(getObjs, getKey, initialExclusiveStartKey);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private readonly Func<string, T[]> getObjs;
            private readonly Func<T, string> getKey;
            private readonly string initialExclusiveStartKey;
        }
    }
}