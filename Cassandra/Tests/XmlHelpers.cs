using System;
using System.Text;
using System.Xml;

namespace Cassandra.Tests
{
    public static class XmlHelpers
    {
        public static T TryGetChildNode<T>(this XmlNode parent, string localName) where T : XmlNode
        {
            return TryGetChildNode<T>(parent, localName, null);
        }

        public static T TryGetChildNode<T>(this XmlNode parent, string localName, string namespaceUri) where T : XmlNode
        {
            foreach(XmlNode node in parent.ChildNodes)
            {
                if(localName.Equals(node.LocalName, StringComparison.OrdinalIgnoreCase)
                   && typeof(T).IsAssignableFrom(node.GetType())
                   && (namespaceUri == null || node.NamespaceURI == namespaceUri))
                    return (T)node;
            }
            return null;
        }

        public static string FormattedOuterXml(this XmlNode node)
        {
            var result = new StringBuilder();
            XmlWriter writer = XmlWriter.Create(result, new XmlWriterSettings
                {
                    Indent = true,
                    OmitXmlDeclaration = !node.HasXmlDeclaration()
                });
            node.WriteTo(writer);
            writer.Flush();
            return result.ToString();
        }

        public static string ReformatXml(this string xml)
        {
            return FormattedOuterXml(CreateXml(xml));
        }

        public static XmlDocument CreateXml(string xml)
        {
            return CreateXml(x => x.LoadXml(xml));
        }

        private static bool HasXmlDeclaration(this XmlNode node)
        {
            return node.TryGetChildNode<XmlDeclaration>("xml") != null;
        }

        private static XmlDocument CreateXml(Action<XmlDocument> loadAction)
        {
            var result = new XmlDocument();
            loadAction(result);
            return result;
        }
    }
}