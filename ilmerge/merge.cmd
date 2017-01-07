if exist %OUT_DIR% (
	rd %OUT_DIR% /Q /S || exit /b 1
)
mkdir %OUT_DIR% || exit /b 1

set TARGET_PLATFORM="v4, %ProgramFiles(x86)%\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5"
%~dp0ILMerge.exe /wildcards /log:ilmerge.log %ADDITIONAL_ARGS% /targetplatform:%TARGET_PLATFORM% /lib:%SRC_DIR% /out:%OUT_DIR%\%OUT_ASSSEMBLY% %PRIMARY_ASSSEMBLY% %DEPS% || exit /b 1