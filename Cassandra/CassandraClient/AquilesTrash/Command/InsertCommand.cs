using System.Diagnostics;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class InsertCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var column = ModelConverterHelper.Convert<AquilesColumn, Column>(Column);
            var columnParent = BuildColumnParent();
	    var stopwatch = Stopwatch.StartNew();
            cassandraClient.insert(Key, columnParent, column, GetCassandraConsistencyLevel());
	    logger.DebugFormat("Added key '{0}' to columnFamily '{1}' in {2}ms.", Key, ColumnFamily, stopwatch.ElapsedMilliseconds);
        }

        public override void ValidateInput()
        {
            base.ValidateInput();
            if(Column == null)
                throw new AquilesCommandParameterException("Column parameter must have a value.");
            Column.ValidateForInsertOperation();
        }

        public AquilesColumn Column { set; private get; }
    }
}