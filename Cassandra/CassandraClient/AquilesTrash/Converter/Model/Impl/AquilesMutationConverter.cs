using System;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;
using SKBKontur.Cassandra.CassandraClient.Core;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter.Model.Impl
{
    /// <summary>
    /// Converter for AquilesMutation
    /// </summary>
    public class AquilesMutationConverter : IThriftConverter<IAquilesMutation, Mutation>
    {
        /// <summary>
        /// Transform IAquilesMutation structure into Mutation
        /// </summary>
        /// <param name="objectA"></param>
        /// <returns></returns>
        public Mutation Transform(IAquilesMutation objectA)
        {
            Mutation mutation = null;
            Type objectAType = objectA.GetType();
            if (objectAType.Equals(typeof(AquilesSetMutation)))
            {
                mutation = this.convertSetMutation((AquilesSetMutation)objectA);
            }
            else if (objectAType.Equals(typeof(AquilesDeletionMutation)))
            {
                mutation = this.convertDeletionMutation((AquilesDeletionMutation)objectA);
            }
            else
            {
                throw new AquilesException("Mutation converter not implemented.");
            }
            return mutation;
        }

        /// <summary>
        /// Transform Mutation structure into IAquilesMutation
        /// Not implemented! Why would you need this??? 
        /// </summary>
        /// <param name="objectB"></param>
        /// <returns></returns>
        public IAquilesMutation Transform(Mutation objectB)
        {
            throw new NotImplementedException();
        }

        

        private Mutation convertSetMutation(AquilesSetMutation mutation)
        {
            ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
            if (mutation.Column != null)
            {
                columnOrSuperColumn.Column = ModelConverterHelper.Convert<AquilesColumn,Column>(mutation.Column);
            }

            if (mutation.SuperColumn != null)
            {
                columnOrSuperColumn.Super_column = ModelConverterHelper.Convert<AquilesSuperColumn,SuperColumn>(mutation.SuperColumn);
            }

            return new Mutation()
                {
                    Column_or_supercolumn = columnOrSuperColumn
                };
        }

        private Mutation convertDeletionMutation(AquilesDeletionMutation aquilesDeletionMutation)
        {
            Deletion deletion = new Deletion();
            deletion.Predicate = ModelConverterHelper.Convert<AquilesSlicePredicate,SlicePredicate>(aquilesDeletionMutation.Predicate);
            deletion.Super_column = aquilesDeletionMutation.SuperColumn;
            if (aquilesDeletionMutation.Timestamp.HasValue)
            {
                deletion.Timestamp = aquilesDeletionMutation.Timestamp.Value;
            }
            else
            {
                deletion.Timestamp = DateTimeService.UtcNow.Ticks;
            }

            return new Mutation() { Deletion = deletion };
        }
    }
}
