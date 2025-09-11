#!/usr/bin/env bash

usage() {
  echo "Usage: $0 [install|reset] <BENCHMARK_PROJECT_DIR>"
}

restore_destination_jar() {
  local destination_jar="$1"
  local backup_jar="${JMETER_DESTINATION_JAR}.ori"

  if [[ ! -f "$backup_jar" ]]; then
    echo "Error: backup jar not found at $backup_jar"
    return 1
  fi

  cp "$backup_jar" "$destination_jar"
  echo "Restored $destination_jar from $backup_jar"
}

check_destination_jar() {
  local destination_jar="$1"
  local error_message="$2"

  if [[ ! -f "$destination_jar" ]]; then
    echo "$error_message"
    exit 1
  fi

  ls -l "$destination_jar"
}

if [[ $# -eq 1 ]]; then
  declare COMMAND="install"
  declare BENCHMARK_PROJECT_DIR="$1"
elif [[ $# -eq 2 ]]; then
  declare COMMAND="$1"
  declare BENCHMARK_PROJECT_DIR="$2"
else
  usage
  exit 1
fi

if [[ "$COMMAND" != "install" && "$COMMAND" != "reset" ]]; then
  usage
  exit 1
fi

declare VERSION=$(xmllint --xpath "/*[local-name()='project']/*[local-name()='version']/text()" ./pom.xml)
declare JAR_NAME="jmeter.backendlistener.elasticsearch-$VERSION.jar"
declare SOURCE_JAR="./target/$JAR_NAME"
declare JMETER_DESTINATION_JAR="$HOME/.sdkman/candidates/jmeter/current/lib/ext/$JAR_NAME"
declare BENCHMARK_DESTINATION_JAR="$BENCHMARK_PROJECT_DIR/benchmarks/tools/benchmark-deployment/src/main/resources/$JAR_NAME"

if [[ "$COMMAND" == "install" ]]; then
  echo "Building JMeter Elasticsearch Backend Listener plugin version: $VERSION"
  declare TEMP_LOGS="$(mktemp)"
  mvn -q test >"${TEMP_LOGS}" 2>&1
  if [[ $? -ne 0 ]]; then
    echo "Maven tests failed. See details below:"
    cat "${TEMP_LOGS}"
    rm "${TEMP_LOGS}"
    exit 1
  fi

  mvn package >"${TEMP_LOGS}" 2>&1
  if [[ $? -ne 0 ]]; then
    echo "Maven build failed. See details below:"
    cat "${TEMP_LOGS}"
    rm "${TEMP_LOGS}"
    exit 1
  fi
  rm "${TEMP_LOGS}"

  echo "Copying JMeter Elasticsearch Backend Listener plugin to JMeter lib/ext directory and benchmark resources"
  cp "$SOURCE_JAR" "$JMETER_DESTINATION_JAR"
  cp "$SOURCE_JAR" "$BENCHMARK_DESTINATION_JAR"

  echo "Checking installation..."
  check_destination_jar "$JMETER_DESTINATION_JAR" "Error: JMeter Elasticsearch Backend Listener plugin not found in JMeter lib/ext directory"
  check_destination_jar "$BENCHMARK_DESTINATION_JAR" "Error: JMeter Elasticsearch Backend Listener plugin not found in benchmark resources"

  echo "JMeter Elasticsearch Backend Listener plugin installed successfully in $HOME/.sdkman/candidates/jmeter/current/lib/ext and in $BENCHMARK_PROJECT_DIR/benchmarks/tools/benchmark-deployment/src/main/resources"
else
  echo "Resetting JMeter Elasticsearch Backend Listener plugin from .jar.ori backups"
  restore_destination_jar "$JMETER_DESTINATION_JAR" || exit 1
  restore_destination_jar "$BENCHMARK_DESTINATION_JAR" || exit 1

  echo "Checking reset..."
  check_destination_jar "$JMETER_DESTINATION_JAR" "Error: JMeter Elasticsearch Backend Listener plugin not restored in JMeter lib/ext directory"
  check_destination_jar "$BENCHMARK_DESTINATION_JAR" "Error: JMeter Elasticsearch Backend Listener plugin not restored in benchmark resources"

  echo "JMeter Elasticsearch Backend Listener plugin reset successfully from .jar.ori backups"
fi