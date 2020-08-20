using System;
using System.Reflection;

namespace SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Utils.ObjComparer
{
    public class NodeProcessor : INodeProcessor
    {
        public NodeProcessor(CompareTypeAs compareTypeAs, CompareInterfaceAs compareInterfaceAs)
        {
            this.compareTypeAs = compareTypeAs;
            this.compareInterfaceAs = compareInterfaceAs;
        }

        public bool TryProcess(object node, FieldInfo fieldInfo, out Type nodeType, out object nodeValue)
        {
            nodeType = fieldInfo.FieldType;
            nodeValue = null;
            if (fieldInfo.FieldType.IsInterface)
            {
                switch (compareInterfaceAs)
                {
                case CompareInterfaceAs.None:
                    return false;
                case CompareInterfaceAs.Hash:
                    nodeValue = fieldInfo.GetValue(node);
                    if (nodeValue != null)
                    {
                        nodeValue = "interface(" + nodeValue.GetHashCode() + ")";
                        nodeType = typeof(string);
                    }
                    return true;
                }
            }
            else
            {
                nodeValue = fieldInfo.GetValue(node);
                switch (compareTypeAs)
                {
                case CompareTypeAs.Actual:
                    if (nodeValue != null)
                        nodeType = nodeValue.GetType();
                    break;
                case CompareTypeAs.Declared:
                    nodeType = fieldInfo.FieldType;
                    break;
                }
            }
            return true;
        }

        private readonly CompareInterfaceAs compareInterfaceAs;
        private readonly CompareTypeAs compareTypeAs;
    }
}