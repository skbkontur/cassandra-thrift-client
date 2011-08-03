using System;
using System.Reflection;

namespace Cassandra.Tests.ObjComparer
{
    public interface INodeProcessor
    {
        bool TryProcess(object node, FieldInfo fieldInfo, out Type nodeType, out object nodeValue);
    }
}