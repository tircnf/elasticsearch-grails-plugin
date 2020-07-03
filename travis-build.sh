#!/bin/bash

set -e

rm -rf *.zip

./gradlew clean check assemble

filename=$(find build/libs -name "*.jar" | head -1)
filename=$(basename "$filename")

echo "branch: $TRAVIS_BRANCH"
echo "pullrequest: $TRAVIS_PULL_REQUEST"
echo "travis tag: $TRAVIS_TAG"

EXIT_STATUS=0
if [[ -n $TRAVIS_TAG ]] || [[ $TRAVIS_BRANCH == 'master' && $TRAVIS_PULL_REQUEST == 'false' ]]; then

  echo "Publishing archives"

  if [[ -n $TRAVIS_TAG ]]; then
      ./gradlew bintrayUpload || EXIT_STATUS=$?
  else
      ./gradlew publish || EXIT_STATUS=$?
  fi

  ./publish-docs.sh

elif [[ $TRAVIS_PULL_REQUEST == 'false' && $TRAVIS_BRANCH =~ ^master|[234]\..\.x$ ]]; then
    echo "Publishing archives for branch $TRAVIS_BRANCH"
    ./gradlew publish --no-daemon --stacktrace || EXIT_STATUS=$?
else
  echo "Skip publishing"
fi

exit $EXIT_STATUS
