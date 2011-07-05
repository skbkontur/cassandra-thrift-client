﻿using System.Collections.Specialized;
using System.Linq;

using CassandraClient.Abstractions;
using CassandraClient.StorageCore.Extender;

using GroboSerializer;
using GroboSerializer.Writing;

namespace CassandraClient.StorageCore
{
    public abstract class SearchQueryIndexesDefinition<TQuery> : IColumnFamilyIndexesDefinition where TQuery : new()
    {
        protected SearchQueryIndexesDefinition(ISerializer serializer)
        {
            this.serializer = serializer;
            extender = new PublicPropertiesExtender();
        }

        public IndexDefinition[] IndexDefinitions { get { return indexDefinitions ?? (indexDefinitions = GetIndexDefinitions()); } }

        private IndexDefinition[] GetIndexDefinitions()
        {
            var query = new TQuery();
            extender.Extend(query);
            var writer = new NameValueCollectionWriter();
            serializer.Serialize(query, writer);
            NameValueCollection collection = writer.GetResult();
            return collection.AllKeys.Select(key => new IndexDefinition {Name = key, ValidationClass = ValidationClass.UTF8Type}).ToArray();
        }

        private readonly ISerializer serializer;
        private readonly PublicPropertiesExtender extender;
        private IndexDefinition[] indexDefinitions;
    }
}