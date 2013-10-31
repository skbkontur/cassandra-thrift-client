using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;

namespace Cassandra.Tests
{
    public static class CoreTestHelpers
    {
        public static string DumpCollection(NameValueCollection collection)
        {
            var list = new List<CollectionSlot>();
            if(collection != null)
            {
                list.AddRange(
                    collection.Cast<object>().
                               Select((t, i) => new CollectionSlot
                                   {
                                       Key = collection.GetKey(i),
                                       Values = collection.GetValues(i)
                                   }));
            }
            list.Sort((x, y) => String.CompareOrdinal(x.Key, y.Key));

            var result = new StringBuilder();
            foreach(var slot in list)
                result.Append((slot + "\r\n"));
            return result.ToString();
        }

        private class CollectionSlot
        {
            public override string ToString()
            {
                var result = new StringBuilder();
                result.Append("{'" + Key + "':{");
                if(Values != null)
                {
                    Array.Sort(Values);
                    var notFirst = false;
                    foreach(var value in Values)
                    {
                        if(notFirst) result.Append(", ");
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