using System;
using System.Xml;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Utils.ObjComparer
{
    public interface ITypeWriter
    {
        bool TryWrite(Type type, object value, XmlWriter writer);
    }
}