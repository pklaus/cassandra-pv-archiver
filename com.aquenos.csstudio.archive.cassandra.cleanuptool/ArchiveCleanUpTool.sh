#!/bin/sh

cd "`dirname "$0"`"
java ${JAVA_OPTS} -jar plugins/org.eclipse.equinox.launcher_1.1.1.R36x_v20101122_1400.jar "$@"

