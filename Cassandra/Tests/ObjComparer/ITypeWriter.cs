using System;
using System.Xml;

namespace Cassandra.Tests.ObjComparer
{
    public interface ITypeWriter
    {
        bool TryWrite(Type type, object value, XmlWriter writer);
    }
}