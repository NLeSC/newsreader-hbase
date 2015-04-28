Loader and dumper for newsreader naf documents in HBase.

[![Build Status](https://travis-ci.org/NLeSC/newsreader-hbase.svg)](https://travis-ci.org/NLeSC/newsreader-hbase)

Build
=====

Make sure the hadoop and hbase versions in `build.gradle` file are the same as the HBase cluster. 

  ./gradlew build

Running
=======

  ./gradle run -Pargs="<arguments>"

Or unpack distro zip/tarball from `build/distributions` directory and run with

  bin/newsreader-hbase <arguments>

This will connect to HBase server running on localhost.
Connecting to another HBase cluster can be done with the distro by,
adding the config files (eg. hbase-site.xml) to a `conf` directory (placed next to the `bin` and `lib` directories).

