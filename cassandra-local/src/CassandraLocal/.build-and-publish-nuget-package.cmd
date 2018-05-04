if exist "./CassandraLocal/bin" (
    rd "./CassandraLocal/bin" /Q /S || exit /b 1
)
dotnet build --force --no-incremental --configuration Release ./CassandraLocal.sln
dotnet pack --no-build --configuration Release ./CassandraLocal.sln
cd ./CassandraLocal/bin/Release
dotnet nuget push *.nupkg -s https://nuget.org
pause