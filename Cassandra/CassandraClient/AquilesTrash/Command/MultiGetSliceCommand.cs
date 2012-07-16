using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class MultiGetSliceCommand : AbstractKeyspaceColumnFamilyDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Output = null;
            var columnParent = BuildColumnParent();
            var predicate = ModelConverterHelper.Convert<AquilesSlicePredicate, SlicePredicate>(Predicate);
            var output = cassandraClient.multiget_slice(Keys, columnParent, predicate, GetCassandraConsistencyLevel());
            BuildOut(output);
        }

        public override void ValidateInput()
        {
            base.ValidateInput();
            ValidateKeys();
            ValidatePredicate();
        }

        public List<byte[]> Keys { private get; set; }
        public AquilesSlicePredicate Predicate { private get; set; }
        public Dictionary<byte[], List<AquilesColumn>> Output { get; private set; }

        private void ValidatePredicate()
        {
            if(Predicate == null)
                throw new AquilesCommandParameterException("Predicate cannot be null.");
            Predicate.ValidateForQueryOperation();
        }

        private void ValidateKeys()
        {
            if(Keys == null || Keys.Count == 0)
                throw new AquilesCommandParameterException("No Keys found.");

            if(Keys.Any(key => key == null || key.Length == 0))
                throw new AquilesCommandParameterException("Key cannot be null or empty.");
        }

        private void BuildOut(Dictionary<byte[], List<ColumnOrSuperColumn>> output)
        {
            Output = new Dictionary<byte[], List<AquilesColumn>>();
            foreach(var outputKeyValuePair in output)
            {
                var columnOrSuperColumnList = outputKeyValuePair.Value.Select(x => x.Column)
                    .Select(ModelConverterHelper.Convert<AquilesColumn, Column>)
                    .ToList();
                Output.Add(outputKeyValuePair.Key, columnOrSuperColumnList);
            }
        }
    }
}