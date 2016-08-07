@ECHO OFF

REM Find the path to "java.exe".
IF DEFINED JAVA_HOME (
  SET JAVA=%JAVA_HOME%\bin\java.exe
) ELSE (
  SET JAVA=java.exe
)

REM Find the path to the JAR file. This will not work if junction points are
REM used in a funny way, but in contrast to symbol links on *NIX platforms,
REM this is very unlikely.
SET BASE_DIR=%~dp0..
SET JAR_FILE=%BASE_DIR%\lib\${project.groupId}.${project.parent.artifactId}-server-app-${project.version}.jar

REM The paths to "java.exe" and to the JAR file might contain spaces, so we have
REM to use quotes around them.
"%JAVA%" %JAVA_OPTS% -Dcom.aquenos.cassandra.pvarchiver.server.spring.ArchiveServerApplication.defaultConfigurationFileLocation=%BASE_DIR%\conf\cassandra-pv-archiver.yaml -jar "%JAR_FILE%" %*
