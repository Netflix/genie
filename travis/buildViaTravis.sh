#!/bin/bash
# This script will build the project.

# Stop at the first error
set -e

if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
  echo -e "Build Pull Request #$TRAVIS_PULL_REQUEST => Branch [$TRAVIS_BRANCH]"
  # Build and run all tests, create coverage report
  ./gradlew --no-daemon build codeCoverageReport coveralls
  # Re-run genie-web integration tests with MySQL...
  INTEGRATION_TEST_DB=mysql ./gradlew --no-daemon genie-web:integrationTests
  # ... and PostgreSQL
  INTEGRATION_TEST_DB=postgresql ./gradlew --no-daemon genie-web:integrationTests
  # Build Docker images and compile documentation
  ./gradlew --no-daemon javadoc asciidoc dockerBuildAllImages
elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" == "" ]; then
  echo -e 'Build Branch with Snapshot => Branch ['$TRAVIS_BRANCH']'
  ./gradlew --no-daemon -Prelease.travisBranch=$TRAVIS_BRANCH -Prelease.travisci=true -PbintrayUser="${bintrayUser}" -PbintrayKey="${bintrayKey}" -PsonatypeUsername="${sonatypeUsername}" -PsonatypePassword="${sonatypePassword}" snapshot codeCoverageReport coveralls publishGhPages dockerPush
elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" != "" ]; then
  echo -e 'Build Branch for Release => Branch ['$TRAVIS_BRANCH']  Tag ['$TRAVIS_TAG']'
  case "$TRAVIS_TAG" in
  *-rc\.*)
    ./gradlew --no-daemon -Prelease.travisci=true -Prelease.useLastTag=true -PbintrayUser="${bintrayUser}" -PbintrayKey="${bintrayKey}" -PsonatypeUsername="${sonatypeUsername}" -PsonatypePassword="${sonatypePassword}" candidate codeCoverageReport coveralls publishGhPages dockerPush
    ;;
  *)
    ./gradlew --no-daemon -Prelease.travisci=true -Prelease.useLastTag=true -PbintrayUser="${bintrayUser}" -PbintrayKey="${bintrayKey}" -PsonatypeUsername="${sonatypeUsername}" -PsonatypePassword="${sonatypePassword}" final codeCoverageReport coveralls publishGhPages dockerPush
    ;;
  esac
else
  echo -e 'WARN: Should not be here => Branch ['$TRAVIS_BRANCH']  Tag ['$TRAVIS_TAG']  Pull Request ['$TRAVIS_PULL_REQUEST']'
  ./gradlew --no-daemon build codeCoverageReport coveralls
fi
