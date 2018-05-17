using System;
using System.Xml;

namespace SKBKontur.Cassandra.FunctionalTests.Utils.ObjComparer
{
    public class NullableTypeWriter : ITypeWriter
    {
        public NullableTypeWriter(ITypeWriter complexTypeWriter, ITypeWriter nullWriter)
        {
            this.complexTypeWriter = complexTypeWriter;
            this.nullWriter = nullWriter;
        }

        public bool TryWrite(Type type, object value, XmlWriter writer)
        {
            if(type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                var getMethodHasValue = type.GetProperty("HasValue").GetGetMethod();
                var hasValue = (bool)getMethodHasValue.Invoke(value, new object[0]);
                if(!hasValue)
                    nullWriter.Write(null, null, writer);
                else
                {
                    var getMethodValue = type.GetProperty("Value").GetGetMethod();
                    var nullableValue = getMethodValue.Invoke(value, new object[0]);
                    complexTypeWriter.Write(type.GetGenericArguments()[0], nullableValue, writer);
                }
                return true;
            }
            return false;
        }

        private readonly ITypeWriter complexTypeWriter;
        private readonly ITypeWriter nullWriter;
    }
}