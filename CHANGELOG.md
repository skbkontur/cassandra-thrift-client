# Changelog

## v1.3.3 - 2019.11.14
- Update Cassandra v3.11.x to [v3.11.5](http://archive.apache.org/dist/cassandra/3.11.5/).
- Use [SourceLink](https://github.com/dotnet/sourcelink) to help ReSharper decompiler show actual code.

## v1.2.2 - 2019.05.02
- Possible NullReferenceException bug fix.

## v1.2.1 - 2018.09.18
- Set TargetFramework to netstandard2.0. Run tests against net472 and netcoreapp2.1.

## v1.1.1 - 2018.09.13
- Set TargetFramework to net472.
- Use [Nerdbank.GitVersioning](https://github.com/AArnott/Nerdbank.GitVersioning) to automate generation of assembly 
  and nuget package versions.

## v1.0.10 - 2018.05.24
- Initial release with Cassandra [v2.2.12](http://archive.apache.org/dist/cassandra/2.2.12/) and 
  [v3.11.2](http://archive.apache.org/dist/cassandra/3.11.2/) support.
- Both Cassandra versions are patched to use "old" thread pool due to low system timer resolution on Windows.
  Sources for patched Cassandra v2.2.12 are [here](https://github.com/skbkontur/cassandra/tree/cassandra-2.2.12-oldThreadPool)
  and for v3.11.2 are [there](https://github.com/skbkontur/cassandra/tree/cassandra-3.11.2-oldThreadPool).
