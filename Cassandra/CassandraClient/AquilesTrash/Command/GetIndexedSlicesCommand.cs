using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Model;

namespace CassandraClient.AquilesTrash.Command
{
    public class GetIndexedSlicesCommand : AbstractKeyspaceColumnFamilyDependantCommand
    {
        public override void Execute(Cassandra.Client cassandraClient)
        {
            var columnParent = BuildColumnParent();
            var indexClause = ModelConverterHelper.Convert<AquilesIndexClause, IndexClause>(IndexClause);
            var slicePredicate = ModelConverterHelper.Convert<AquilesSlicePredicate, SlicePredicate>(Predicate);
            var result = cassandraClient.get_indexed_slices(columnParent, indexClause, slicePredicate, GetCassandraConsistencyLevel());
            BuildOutput(result);
        }

        public override void ValidateInput()
        {
            base.ValidateInput();
            IndexClause.ValidateForQueryOperation();
            Predicate.ValidateForQueryOperation();
        }

        public AquilesSlicePredicate Predicate { private get; set; }
        public AquilesIndexClause IndexClause { private get; set; }
        public List<byte[]> Output { private set; get; }

        private void BuildOutput(IEnumerable<KeySlice> result)
        {
            var returnObjs = result.Select(keySlice => keySlice.Key).ToList();
            Output = returnObjs;
        }
    }
}