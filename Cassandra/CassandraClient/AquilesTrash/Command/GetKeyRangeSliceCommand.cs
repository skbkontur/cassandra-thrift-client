using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class GetKeyRangeSliceCommand : AbstractKeyspaceColumnFamilyDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ICassandraLogger logger)
        {
            Output = null;
            var columnParent = BuildColumnParent();
            var predicate = ModelConverterHelper.Convert<AquilesSlicePredicate, SlicePredicate>(Predicate);
            var keyRange = ModelConverterHelper.Convert<AquilesKeyRange, KeyRange>(KeyTokenRange);
            var result = cassandraClient.get_range_slices(columnParent, predicate, keyRange, GetCassandraConsistencyLevel());
            BuildOut(result);
        }

        public override void ValidateInput(ICassandraLogger logger)
        {
            base.ValidateInput(logger);
            if(KeyTokenRange == null)
                throw new AquilesCommandParameterException("A KeyTokenRange must be supplied.");

            KeyTokenRange.ValidateForQueryOperation();

            if(Predicate == null)
                throw new AquilesCommandParameterException("Predicate cannot be null.");
            Predicate.ValidateForQueryOperation();
        }

        public AquilesKeyRange KeyTokenRange { private get; set; }
        public AquilesSlicePredicate Predicate { private get; set; }
        public List<byte[]> Output { get; private set; }

        private void BuildOut(IEnumerable<KeySlice> output)
        {
            var returnObjs = output.Select(keySlice => keySlice.Key).ToList();
            Output = returnObjs;
        }
    }
}