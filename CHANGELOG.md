# Changelog

## 2.0 - 2018.09.11
- Prepare SkbKontur.Cassandra.ThriftClient to work with Cassandra v3.11.x 
  (see [CASSANDRA-9839](https://issues.apache.org/jira/browse/CASSANDRA-9839)).
- Use [SkbKontur.Cassandra.Local](https://github.com/skbkontur/cassandra-local) module for integration testing.
- Switch to SDK-style project format and dotnet core build tooling 
  ([PR#1](https://github.com/skbkontur/cassandra-thrift-client/pull/1)).
- Set TargetFramework to net472.
- Use [Vostok.Logging.Abstractions](https://github.com/vostok/logging.abstractions) as a logging framework facade.
- Use [Nerdbank.GitVersioning](https://github.com/AArnott/Nerdbank.GitVersioning) to automate generation of assembly 
  and nuget package versions.
- Implement workaround for "ThreadAbortException not re-thrown by the runtime" 
  [issue](https://github.com/dotnet/coreclr/issues/16122) on net471.
- Fix comparison of disabled compaction strategies 
  ([PR#2](https://github.com/skbkontur/cassandra-thrift-client/pull/2)).
- Add org.apache.cassandra.db.marshal.SimpleDateType to supported DataTypes.
