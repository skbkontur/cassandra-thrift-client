set OUT_DIR=%~dp0Assemblies\merged
set OUT_ASSSEMBLY=Cassandra.CassandraClient.dll
set PRIMARY_ASSSEMBLY=Cassandra.CassandraClient.dll
set DEPS=Apache.Cassandra.209.dll Metrics.dll Thrift.dll
set SRC_DIR=%~dp0Assemblies\
call %~dp0ilmerge\merge.cmd || exit /b 1