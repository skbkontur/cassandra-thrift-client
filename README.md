# Apache Cassandra binaries for local testing on Windows

[![NuGet Status](https://img.shields.io/nuget/v/SkbKontur.Cassandra.Local.svg)](https://www.nuget.org/packages/SkbKontur.Cassandra.Local/)
[![Build status](https://ci.appveyor.com/api/projects/status/fxjye45x38hgvamu?svg=true)](https://ci.appveyor.com/project/skbkontur/cassandra-local)

## Overview

Simplest usage:
```
var templateRelativePath = "cassandra-local/cassandra/v3.11.x";
var templateDir = DirectoryHelpers.FindDirectory(AppDomain.CurrentDomain.BaseDirectory, templateRelativePath);
var deployDir = Path.Combine(Path.GetTempPath(), "deployed_cassandra_v3.11.x");
var node = new LocalCassandraNode(templateDir, deployDir)
{
    RpcPort = 9160,
    CqlPort = 9042,
    HeapSize = "256M"
};
node.Retart();
...
node.Stop();
```

You will need to install Java 8 64-bit either JDK or JRE to run Cassandra.

## Release Notes

See [CHANGELOG](CHANGELOG.md).
