#!/bin/bash

#for version in 3.0.0 3.0.1 3.0.2 3.0.3 3.1.0 3.1.1 3.1.2 3.1.3 3.2.0 3.2.1 3.2.2 3.2.3 3.2.4 3.2.5 3.2.6; do
#  wget -S https://oss.aquenos.com/cassandra-pv-archiver/download/cassandra-pv-archiver-$version-src.tar.gz
#done
#for version in 1.0.0 1.0.1 1.1.0 1.2.0 2.0.0 2.0.1 2.1.0 2.2.0 2.3.0 2.3.1; do
#  wget -S https://oss.aquenos.com/epics/cassandra-archiver/download/cassandra-archiver-$version-src.tar.gz
#done

# ^ doesn't preserve timestamps for the older files (they seem to be off on the server)

./download-release-files.py https://oss.aquenos.com/cassandra-pv-archiver/    https://oss.aquenos.com/cassandra-pv-archiver/download/cassandra-pv-archiver-VERSION-src.tar.gz
./download-release-files.py https://oss.aquenos.com/epics/cassandra-archiver/ https://oss.aquenos.com/epics/cassandra-archiver/download/cassandra-archiver-VERSION-src.tar.gz


mkdir repo

pushd repo

git init
git config user.name "Git Bot"
git config user.email mail@example.com

cat <<"EOF" > README.md
# cassandra-pv-archiver

This repository is a Git mirror of the [Cassandra PV Archiver][] published
by aquenos GmbH und the terms of the [Eclipse Public License v1.0][].

The individual release files of the source code were added as individual
commits to the repository.
A separate branch (`mirror-tools`) contains the tools used to programmatically
create this mirror repository.
Although this repository is mostly a mirror of the "Cassandra PV Archiver" versions,
the releases of the older Cassandra CSS Archiver were also added as first commits
as a reference.
The dates of the commits are forged to be equivalent to the original publication
of the respective release.

[Cassandra PV Archiver]: https://oss.aquenos.com/cassandra-pv-archiver/
[Eclipse Public License v1.0]: https://oss.aquenos.com/cassandra-pv-archiver/
EOF

git add .
export GIT_COMMITTER_DATE="1262304000 0000"
git commit -m "Mirror initialized with README.md" --date "1262304000 0000"

for tarball in ../*.tar.gz; do
  git rm -rf . > /dev/null
  rm -rf *
  git checkout HEAD -- README.md
  tar -xf $tarball --strip-components=1
  git add .
  version=$(echo $tarball | sed -ne 's/-[^-]*$//;s/.*-//;p')
  export GIT_COMMITTER_DATE="$(stat -c %Y $tarball) +0100"
  git commit -m "$(basename $tarball)" --date "$(stat -c %Y $tarball) +0100"
  git tag v${version}
done

popd >/dev/null 2>&1
