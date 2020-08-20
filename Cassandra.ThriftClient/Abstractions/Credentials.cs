using System;

using JetBrains.Annotations;

namespace SkbKontur.Cassandra.ThriftClient.Abstractions
{
    public class Credentials
    {
        public Credentials([NotNull] string username, [NotNull] string password)
        {
            if (string.IsNullOrEmpty(username))
                throw new ArgumentException("Username must be not null and not empty", nameof(username));

            if (string.IsNullOrEmpty(password))
                throw new ArgumentException("Password must be not null and not empty", nameof(password));

            Username = username;
            Password = password;
        }

        [NotNull]
        public string Username { get; }

        [NotNull]
        public string Password { get; }
    }
}