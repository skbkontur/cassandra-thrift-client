using System;
using System.Xml;

namespace Cassandra.Tests.ObjComparer
{
    public class NullWriter : ITypeWriter
    {
        public bool TryWrite(Type type, object value, XmlWriter writer)
        {
            if(value == null)
            {
                writer.WriteAttributeString("type", "null");
                return true;
            }
            return false;
        }
    }
}