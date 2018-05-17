using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class ColumnComparatorType
    {
        private static readonly Regex pattern = new Regex(@"^(?<dataType>(([\w]+\.)*[\w]+))(\(((?<dataType>(([\w]+\.)*[\w]+)),)*(?<dataType>(([\w]+\.)*[\w]+))\))*$");

        public ColumnComparatorType(string input)
        {
            if(string.IsNullOrEmpty(input))
                throw new InvalidOperationException("input cannot be empty or null");

            var match = pattern.Match(input);
            if(!match.Success)
                throw new InvalidOperationException(string.Format("string '{0}' has invalid format", input));

            var dataTypes = new List<DataType>();
            var group = match.Groups["dataType"];
            if(group.Success)
            {
                for(var i = 0; i < group.Captures.Count; i++)
                    dataTypes.Add(group.Captures[i].Value.FromStringValue<DataType>());
            }

            IsComposite = dataTypes[0] == DataType.CompositeType;
            if(IsComposite)
            {
                dataTypes.Remove(DataType.CompositeType);
                if(dataTypes.Count < 2)
                    throw new InvalidOperationException(string.Format("invalid comparatorType: {0}.CompositeType must have more than one subtypes", input));

                if(dataTypes.Count(x => x == DataType.CompositeType) != 0)
                    throw new InvalidOperationException(string.Format("invalid comparatorType: {0}.Cannot be more than one compositeType", input));
            }

            Types = dataTypes.ToArray();
        }

        public ColumnComparatorType(DataType[] subTypes)
        {
            if(subTypes == null || subTypes.Length == 0)
                throw new InvalidOperationException("types cannot be empty");
            if(subTypes.Any(x => x == DataType.CompositeType))
                throw new InvalidOperationException("subType cannot be compositeType");
            IsComposite = subTypes.Length != 1;
            Types = subTypes;
        }

        public ColumnComparatorType(DataType type)
            : this(new[] {type})
        {
        }

        public bool IsComposite { get; private set; }
        public DataType[] Types { get; private set; }

        public override string ToString()
        {
            return IsComposite ? string.Format("{0}({1})", DataType.CompositeType.ToStringValue(), string.Join(",", Types.Select(x => x.ToStringValue()))) : Types.First().ToStringValue();
        }
    }
}