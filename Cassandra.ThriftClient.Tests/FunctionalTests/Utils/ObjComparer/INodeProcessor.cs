using System;
using System.Reflection;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Utils.ObjComparer
{
    public interface INodeProcessor
    {
        bool TryProcess(object node, FieldInfo fieldInfo, out Type nodeType, out object nodeValue);
    }
}