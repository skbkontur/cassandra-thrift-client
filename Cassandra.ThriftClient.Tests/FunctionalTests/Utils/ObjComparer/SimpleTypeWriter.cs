using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;

namespace SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Utils.ObjComparer
{
    public class SimpleTypeWriter
    {
        public static string TryWrite(Type type, object value)
        {
            if (type.IsEnum || type.IsPrimitive)
                return value.ToString();
            if (!serializers.TryGetValue(type, out var func))
                return null;
            return func(value);
        }

        private static string ObjToString(object o)
        {
            return o.ToString();
        }

        private static string NameValueCollectionToString(object o)
        {
            return DumpCollection((NameValueCollection)o);
        }

        private static string DumpCollection(NameValueCollection collection)
        {
            var list = new List<CollectionSlot>();
            if (collection != null)
            {
                list.AddRange(
                    collection.Cast<object>().Select((t, i) => new CollectionSlot
                        {
                            Key = collection.GetKey(i),
                            Values = collection.GetValues(i)
                        }));
            }
            list.Sort((x, y) => String.CompareOrdinal(x.Key, y.Key));

            var result = new StringBuilder();
            foreach (var slot in list)
                result.Append((slot + "\r\n"));
            return result.ToString();
        }

        private static readonly Dictionary<Type, Func<object, string>> serializers =
            new Dictionary<Type, Func<object, string>>
                {
                    {typeof(string), o => (string)o},
                    {typeof(Guid), ObjToString},
                    {typeof(NameValueCollection), NameValueCollectionToString},
                    {typeof(IPEndPoint), ObjToString},
                    {
                        typeof(DateTime), o =>
                            {
                                var t = (DateTime)o;
                                return t.Ticks.ToString(CultureInfo.InvariantCulture);
                            }
                    }
                };

        private class CollectionSlot
        {
            public override string ToString()
            {
                var result = new StringBuilder();
                result.Append("{'" + Key + "':{");
                if (Values != null)
                {
                    Array.Sort(Values);
                    var notFirst = false;
                    foreach (var value in Values)
                    {
                        if (notFirst) result.Append(", ");
                        notFirst = true;
                        result.Append("'" + value + "'");
                    }
                }
                result.Append("}}");
                return result.ToString();
            }

            public string Key { get; set; }
            public string[] Values { get; set; }
        }
    }
}