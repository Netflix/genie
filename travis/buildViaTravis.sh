#!/bin/bash
# This script will build the project.

# Stop at the first error
set -e

GRADLE=./gradlew
GRADLE_OPTIONS='--stacktrace'

if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
  echo -e "Build Pull Request #$TRAVIS_PULL_REQUEST => Branch [$TRAVIS_BRANCH]"
  # Build and run all tests, create coverage report
  ${GRADLE} ${GRADLE_OPTIONS} build codeCoverageReport coveralls
  # Re-run genie-web integration tests with MySQL...
  INTEGRATION_TEST_DB=mysql ${GRADLE} ${GRADLE_OPTIONS} genie-web:integrationTest
  # ... and PostgreSQL
  INTEGRATION_TEST_DB=postgresql ${GRADLE} ${GRADLE_OPTIONS} genie-web:integrationTest
  # Build Docker images and compile documentation
  ${GRADLE} ${GRADLE_OPTIONS} javadoc asciidoc dockerBuildAllImages
fi
