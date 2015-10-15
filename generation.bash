#!/bin/bash -e
#package: thrift-compiler
mkdir -p src-gen/main/java
./thrift-compiler -gen java -out src-gen/main/java -v -r src/main/resources/hadoopfs.thrift
