using System.Collections.Generic;
using System.Linq;

using CassandraClient.Abstractions;
using CassandraClient.Clusters;
using CassandraClient.Connections;
using CassandraClient.Helpers;

namespace StorageCore
{
    public abstract class CassandraTimedSortedStorageBase : ITimedSortedStorage
    {
        protected CassandraTimedSortedStorageBase(
            ICassandraCluster cassandraCluster,
            ICassandraLogManager logManager,
            ICassandraCoreSettings cassandraCoreSettings,
            string columnFamilyName)
        {
            this.cassandraCluster = cassandraCluster;
            this.cassandraCoreSettings = cassandraCoreSettings;
            this.columnFamilyName = columnFamilyName;
            logger = logManager.GetLogger(GetType());
        }

        public void Append(string category, ulong ticks, string id)
        {
            AppendBatch(category,
                        new[]
                            {
                                new TimedStorageElement
                                    {
                                        Ticks = ticks,
                                        Id = id
                                    }
                            });
        }

        public void AppendBatch(string category, IEnumerable<TimedStorageElement> elements)
        {
            using(IColumnFamilyConnection connection = cassandraCluster.RetrieveColumnFamilyConnection(cassandraCoreSettings.KeyspaceName, columnFamilyName))
                connection.BatchInsert(elements.Select(element => GetRow(category, element)));
        }

        public TimedStorageElement[] Get(string category, int count, TimedStorageElement greatThanElement = null)
        {
            greatThanElement = greatThanElement ?? new TimedStorageElement {Ticks = 0};
            using(IColumnFamilyConnection connection = cassandraCluster.RetrieveColumnFamilyConnection(cassandraCoreSettings.KeyspaceName, columnFamilyName))
            {
                return connection.GetRow(category, GetColumnName(greatThanElement), count).Select(column => new TimedStorageElement
                    {
                        Ticks = TicksFromColumnName(category, column.Name),
                        Id = StringHelpers.BytesToString(column.Value)
                    }).ToArray();
            }
        }

        private KeyValuePair<string, IEnumerable<Column>> GetRow(string category, TimedStorageElement element)
        {
            return new KeyValuePair<string, IEnumerable<Column>>(category, new[]
                {
                    new Column
                        {
                            Name = GetColumnName(element),
                            Value = StringHelpers.StringToBytes(element.Id)
                        }
                });
        }

        private ulong TicksFromColumnName(string key, string columnName)
        {
            string[] strings = columnName.Split('_');
            if(strings.Length < 2)
            {
                logger.Error("TicksFromColumnName({0},{1}). Invalid timed column name {1}. Number of tokens must be at least 2. Return 0.", key, columnName);
                return 0;
            }
            ulong result;
            if(!ulong.TryParse(strings[0], out result))
            {
                logger.Error("TicksFromColumnName({0},{1}). First token {2} is not long. Return 0.", key, columnName, strings[0]);
                return 0;
            }
            return result;
        }

        private string GetColumnName(TimedStorageElement timedStorageElement)
        {
            return string.Format("{0}_{1}", (timedStorageElement.Ticks).ToString("D20"), timedStorageElement.Id ?? "");
        }

        private readonly ICassandraCluster cassandraCluster;
        private readonly ICassandraCoreSettings cassandraCoreSettings;
        private readonly string columnFamilyName;
        private readonly ICassandraLogger logger;
    }
}