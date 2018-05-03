del /Q Assemblies\Cassandra.*.dll
del /Q Assemblies\Cassandra.*.pdb

FOR %%S IN (Cassandra, CassandraClient.FunctionalTests) DO (
	pushd %%S
	FOR /F "tokens=*" %%G IN ('DIR /B /AD /S bin') DO DEL /S /Q "%%G\*.dll" > nul 2> nul
	FOR /F "tokens=*" %%G IN ('DIR /B /AD /S bin') DO DEL /S /Q "%%G\*.pdb" > nul 2> nul
	FOR /F "tokens=*" %%G IN ('DIR /B /AD /S obj') DO DEL /S /Q "%%G\*.dll" > nul 2> nul
	FOR /F "tokens=*" %%G IN ('DIR /B /AD /S obj') DO DEL /S /Q "%%G\*.pdb"	> nul 2> nul
	
	"../.nuget/nuget.exe" restore "%%S.sln"
	"C:\Program Files (x86)\Microsoft Visual Studio\2017\Professional\MSBuild\15.0\Bin\MsBuild.exe" "%%S.sln" /target:Clean;Rebuild /p:Configuration=Release /verbosity:m /p:zip=false

	popd
		if errorlevel 1 goto fail
)

ColorPrint Green "BUILD SUCCEEDED"
goto end

:fail

ColorPrint Red "BUILD FAILED"

:end

pause
