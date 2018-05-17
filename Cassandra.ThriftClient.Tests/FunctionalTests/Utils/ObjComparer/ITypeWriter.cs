using System;
using System.Xml;

namespace SKBKontur.Cassandra.FunctionalTests.Utils.ObjComparer
{
    public interface ITypeWriter
    {
        bool TryWrite(Type type, object value, XmlWriter writer);
    }
}