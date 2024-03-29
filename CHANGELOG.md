# Changelog

## v2.4.29 - 2021.12.01
- Update dependencies.
- Run tests against net6.0 tfm.

## v2.4.25 - 2021.03.13
- Update dependencies.
- Run tests against net5.0 tfm.

## v2.4.23 - 2020.10.27
- Add new 'timer' metric measures new connection aquirement time (PR [#14](https://github.com/skbkontur/cassandra-thrift-client/pull/14)).

## v2.4.16 - 2020.10.05
- Fix connection leak (PR [#13](https://github.com/skbkontur/cassandra-thrift-client/pull/13)).

## v2.4.9 - 2020.08.21
- Add `Credentials` to `ICassandraClusterSettings`. PasswordAuthenticator now supported (PR [#11](https://github.com/skbkontur/cassandra-thrift-client/pull/11)).

## v2.3.6 - 2020.04.21
- Fix `CassandraSchemaActualizer` logging context to be `"CassandraThriftClient"` as it was before making this class public.

## v2.3.3 - 2020.04.14
- Separate `ICassandraSchemaActualizer` API from `ICassandraCluster` (PR [#10](https://github.com/skbkontur/cassandra-thrift-client/pull/10)).

Note that `ICassandraCluster.ActualizeKeyspaces()` method was removed. Use `CassandraSchemaActualizer` directly to modify thrift db schema.

## v2.2.19 - 2020.04.10
- Add ability to pass custom timeout to `SchemeActualizer` (PR [#9](https://github.com/skbkontur/cassandra-thrift-client/pull/9)).

## v2.2.14 - 2019.12.23
- Adjust root namespace name to match assembly name.

## v2.2.9 - 2019.11.13
- Add `TimeBasedColumnFamilyConnection` to support `TimeUUID` clustering keys.
- Use monotonic timestamp from [SkbKontur.Cassandra.TimeGuid](https://github.com/skbkontur/cassandra-time-guid) package.
- Use [SourceLink](https://github.com/dotnet/sourcelink) to help ReSharper decompiler show actual code.

## v2.1.7 - 2019.10.11
- Target .NET Standard 2.0 (PR [#8](https://github.com/skbkontur/cassandra-thrift-client/pull/8)).
- Use Moq instead of Rhino.Mocks to be able to target netstandard2.0 (PR [#7](https://github.com/skbkontur/cassandra-thrift-client/pull/7)).
- Inline Apache Thrift library source code of [v0.12.0](https://github.com/apache/thrift/tree/v0.12.0/lib/csharp/src).
- Use cassandra.thrift from [v2.2.11](https://github.com/apache/cassandra/tree/cassandra-2.2.11/interface).
- Generate source code for cassandra.thrift using `./thrift-0.12.0.exe --gen csharp cassandra.thrift` command.

## v2.0 - 2018.09.12
- Prepare SkbKontur.Cassandra.ThriftClient to work with Cassandra v3.11.x 
  (see [CASSANDRA-9839](https://issues.apache.org/jira/browse/CASSANDRA-9839)).
- Use [SkbKontur.Cassandra.Local](https://github.com/skbkontur/cassandra-local) module for integration testing.
- Switch to SDK-style project format and dotnet core build tooling 
  (PR [#1](https://github.com/skbkontur/cassandra-thrift-client/pull/1)).
- Set TargetFramework to net471.
- Use [Vostok.Logging.Abstractions](https://github.com/vostok/logging.abstractions) as a logging framework facade.
- Use [Nerdbank.GitVersioning](https://github.com/AArnott/Nerdbank.GitVersioning) to automate generation of assembly 
  and nuget package versions.
- Implement workaround for "ThreadAbortException not re-thrown by the runtime" 
  [issue](https://github.com/dotnet/coreclr/issues/16122) on net471.
- Fix comparison of disabled compaction strategies 
  (PR [#2](https://github.com/skbkontur/cassandra-thrift-client/pull/2)).
- Add org.apache.cassandra.db.marshal.SimpleDateType to supported DataTypes.
- Add retry logic for MultigetSlice/MultigetCount queries to avoid data skip that caused by the bug [CASSANDRA-14812](https://issues.apache.org/jira/browse/CASSANDRA-14812) 
(PR [#3](https://github.com/skbkontur/cassandra-thrift-client/pull/3)).
