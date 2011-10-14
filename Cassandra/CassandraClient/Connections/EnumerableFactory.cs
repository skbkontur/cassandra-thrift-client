using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Connections
{
    public class EnumerableFactory : IEnumerableFactory
    {
        public IEnumerable<string> GetRowsEnumerator(int bulkSize, Func<string, int, string[]> getRows, string initialStartKey = null)
        {
            return new ObjectsEnumerable<string>(
                key => getRows(key, bulkSize).ToArray(),
                x => x,
                initialStartKey);
        }

        public IEnumerable<Column> GetColumnsEnumerator(string key, int bulkSize, Func<string, string, int, Column[]> getColumns, string initialStartKey = null)
        {
            return new ObjectsEnumerable<Column>(
                fromColumnName => getColumns(key, fromColumnName, bulkSize).ToArray(),
                col => col.Name,
                initialStartKey);
        }

        private class ObjectsEnumerator<T> : IEnumerator<T>
        {
            public ObjectsEnumerator(Func<string, T[]> getObjs, Func<T, string> getKey, string initialStartKey)
            {
                this.getObjs = getObjs;
                this.getKey = getKey;
                this.initialStartKey = initialStartKey;
                exclusiveStartKey = initialStartKey;
            }

            public void Dispose()
            {
            }

            public bool MoveNext()
            {
                if(++index >= bulk.Length)
                {
                    index = 0;
                    bulk = getObjs(exclusiveStartKey).ToArray();
                    if(bulk.Length == 0) return false;
                    exclusiveStartKey = getKey(bulk.Last());
                }
                return true;
            }

            public void Reset()
            {
                exclusiveStartKey = initialStartKey;
                index = -1;
                bulk = new T[0];
            }

            public T Current { get { return bulk[index]; } }

            object IEnumerator.Current { get { return Current; } }
            private readonly Func<string, T[]> getObjs;
            private readonly Func<T, string> getKey;
            private readonly string initialStartKey;
            private string exclusiveStartKey;
            private int index = -1;
            private T[] bulk = new T[0];
        }

        private class ObjectsEnumerable<T> : IEnumerable<T>
        {
            public ObjectsEnumerable(Func<string, T[]> getObjs, Func<T, string> getKey, string initialStartKey)
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

            private readonly Func<string, T[]> getObjs;
            private readonly Func<T, string> getKey;
            private readonly string initialStartKey;
        }
    }
}