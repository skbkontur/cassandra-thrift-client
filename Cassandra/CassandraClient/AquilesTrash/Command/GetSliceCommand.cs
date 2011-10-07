using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Exceptions;
using CassandraClient.AquilesTrash.Model;

namespace CassandraClient.AquilesTrash.Command
{
    public class GetSliceCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand
    {
        public override void Execute(Cassandra.Client cassandraClient)
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