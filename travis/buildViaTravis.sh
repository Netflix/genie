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
elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" == "" ] && [ "$TRAVIS_BRANCH" == "dev-snapshot" ]; then
  echo -e 'Build Development Snapshot'
  ${GRADLE} ${GRADLE_OPTIONS} --stacktrace -Prelease.travisBranch=$TRAVIS_BRANCH -Prelease.travisci=true -PnetflixOss.username=$NETFLIX_OSS_REPO_USERNAME -PnetflixOss.password=$NETFLIX_OSS_REPO_PASSWORD  -Psonatype.signingPassword=$NETFLIX_OSS_SIGNING_PASSWORD snapshot dockerPush -x check
elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" == "" ]; then
  echo -e 'Build Branch with Snapshot => Branch ['$TRAVIS_BRANCH']'
  ${GRADLE} ${GRADLE_OPTIONS} --stacktrace -Prelease.travisBranch=$TRAVIS_BRANCH -Prelease.travisci=true -PnetflixOss.username=$NETFLIX_OSS_REPO_USERNAME -PnetflixOss.password=$NETFLIX_OSS_REPO_PASSWORD  -Psonatype.signingPassword=$NETFLIX_OSS_SIGNING_PASSWORD snapshot codeCoverageReport coveralls gitPublishPush dockerPush
elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" != "" ]; then
  echo -e 'Build Branch for Release => Branch ['$TRAVIS_BRANCH']  Tag ['$TRAVIS_TAG']'
  case "$TRAVIS_TAG" in
  *-rc\.*)
    ${GRADLE} ${GRADLE_OPTIONS} --stacktrace -Prelease.travisci=true -Prelease.useLastTag=true  -PnetflixOss.username=$NETFLIX_OSS_REPO_USERNAME -PnetflixOss.password=$NETFLIX_OSS_REPO_PASSWORD  -Psonatype.signingPassword=$NETFLIX_OSS_SIGNING_PASSWORD candidate codeCoverageReport coveralls gitPublishPush dockerPush
    ;;
  *)
    ${GRADLE} ${GRADLE_OPTIONS} --stacktrace -Prelease.travisci=true -Prelease.useLastTag=true  -PnetflixOss.username=$NETFLIX_OSS_REPO_USERNAME -PnetflixOss.password=$NETFLIX_OSS_REPO_PASSWORD  -Psonatype.signingPassword=$NETFLIX_OSS_SIGNING_PASSWORD final codeCoverageReport coveralls gitPublishPush dockerPush
    ;;
  esac
else
  echo -e 'WARN: Should not be here => Branch ['$TRAVIS_BRANCH']  Tag ['$TRAVIS_TAG']  Pull Request ['$TRAVIS_PULL_REQUEST']'
  ${GRADLE} ${GRADLE_OPTIONS} build codeCoverageReport coveralls
fi
