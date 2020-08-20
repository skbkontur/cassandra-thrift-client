using System;
using System.Xml;

namespace SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Utils.ObjComparer
{
    public interface ITypeWriter
    {
        bool TryWrite(Type type, object value, XmlWriter writer);
    }
}