using System;
using System.Collections.Generic;
using System.Linq;

using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Helpers;
using SkbKontur.Cassandra.TimeBasedUuid;

namespace SkbKontur.Cassandra.ThriftClient.Connections
{
    internal class TimeBasedColumnFamilyConnection : ITimeBasedColumnFamilyConnection
    {
        public TimeBasedColumnFamilyConnection(IColumnFamilyConnectionImplementation implementation)
        {
            this.implementation = implementation;
        }

        public void BatchInsert([NotNull] List<Tuple<string, List<TimeBasedColumn>>> data)
        {
            var rawData = data.Select(t => new KeyValuePair<byte[], List<RawColumn>>(StringExtensions.StringToBytes(t.Item1), t.Item2.Select(x => x.ToRawColumn()).ToList())).ToList();
            implementation.BatchInsert(rawData);
        }

        [CanBeNull]
        public TimeBasedColumn TryGetColumn([NotNull] string key, [NotNull] TimeGuid columnName)
        {
            var rawKey = StringExtensions.StringToBytes(key);
            if (!implementation.TryGetColumn(rawKey, columnName.ToByteArray(), out var rawColumn))
                return null;
            return rawColumn.ToTimeBasedColumn();
        }

        [NotNull]
        public TimeBasedColumn[] GetRange([NotNull] string key, [CanBeNull] TimeGuid exclusiveStartColumnName, [CanBeNull] TimeGuid inclusiveEndColumnName, int take, bool reversed)
        {
            if (take == int.MaxValue)
                take--;
            if (take <= 0)
                return new TimeBasedColumn[0];
            var rawKey = StringExtensions.StringToBytes(key);
            return implementation.GetRow(rawKey, exclusiveStartColumnName?.ToByteArray(), inclusiveEndColumnName?.ToByteArray(), take + 1, reversed)
                                 .Select(TimeBasedColumnExtensions.ToTimeBasedColumn)
                                 .Where(x => x.Name != exclusiveStartColumnName)
                                 .Take(take)
                                 .ToArray();
        }

        private readonly IColumnFamilyConnectionImplementation implementation;
    }
}