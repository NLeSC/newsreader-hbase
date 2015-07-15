Loader and dumper for newsreader naf documents in HBase.

[![Build Status](https://travis-ci.org/NLeSC/newsreader-hbase.svg)](https://travis-ci.org/NLeSC/newsreader-hbase)

Build
=====

Make sure the hadoop and hbase versions in `build.gradle` file are the same as the HBase cluster. 
Add `hbase-site.xml` to `src/main/resources` and create a jar with dependencies by executing:

    ./gradlew shadowJar

Running
=======

Initialize hadoop/hbase environment.

Several sub commands are available, to see the help run:

     java -classpath `hbase classpath`:`hadoop classpath`:build/libs/newsreader-hbase-*-all.jar

### Map reduce

Submit job with:

    yarn jar $PWD/newsreader-hbase-1.0-all.jar sizer

