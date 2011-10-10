using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class GetSliceCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Output = null;
            ColumnParent columnParent = BuildColumnParent();
            SlicePredicate predicate = ModelConverterHelper.Convert<AquilesSlicePredicate, SlicePredicate>(Predicate);
            List<ColumnOrSuperColumn> output = cassandraClient.get_slice(Key, columnParent, predicate, GetCassandraConsistencyLevel());
            BuildOut(output);
        }

        public override void ValidateInput()
        {
            base.ValidateInput();

            if(Predicate == null)
                throw new AquilesCommandParameterException("Predicate cannot be null.");
            Predicate.ValidateForQueryOperation();
        }

        public AquilesSlicePredicate Predicate { private get; set; }
        public List<AquilesColumn> Output { get; private set; }

        private void BuildOut(IEnumerable<ColumnOrSuperColumn> output)
        {
            Output = output.Select(x => x.Column)
                .Select(ModelConverterHelper.Convert<AquilesColumn, Column>)
                .ToList();
        }
    }
}