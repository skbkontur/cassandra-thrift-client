using Apache.Cassandra;

using CassandraClient.AquilesTrash.Model;

namespace CassandraClient.AquilesTrash.Converter.Model.Impl
{
    /// <summary>
    /// Converter for AquilesColumnDefinition
    /// </summary>
    public class AquilesColumnDefinitionConverter : IThriftConverter<AquilesColumnDefinition, ColumnDef>
    {
        /// <summary>
        /// Transform AquilesColumnDefinition structure into ColumnDef
        /// </summary>
        /// <param name="objectA"></param>
        /// <returns></returns>
        public ColumnDef Transform(AquilesColumnDefinition objectA)
        {
            ColumnDef columnDef = new ColumnDef();
            columnDef.Name = objectA.Name;
            columnDef.Index_name = objectA.IndexName;
            if (objectA.IsIndex)
            {
                columnDef.Index_type = IndexType.KEYS;
            }
            columnDef.Validation_class = objectA.ValidationClass;

            return columnDef;            
        }

        /// <summary>
        /// Transform ColumnDef structure into AquilesColumnDefinition
        /// </summary>
        /// <param name="objectB"></param>
        /// <returns></returns>
        public AquilesColumnDefinition Transform(ColumnDef objectB)
        {
            AquilesColumnDefinition columnDef = new AquilesColumnDefinition();
            columnDef.Name = objectB.Name;
            columnDef.IndexName = objectB.Index_name;
            columnDef.ValidationClass = objectB.Validation_class;
            columnDef.IsIndex = objectB.Index_type == IndexType.KEYS;
            return columnDef; 
        }

        
    }
}
