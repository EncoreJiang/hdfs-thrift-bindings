package org.apache.hadoop.fs.thrift;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Minimal example to access a provided hdfs file system in simple standard
 * configuration. If nothing else works, this should (make sure your dfs is not
 * empty, to avoid confusion).
 *
 * Do not forget to specify in your environment:
 *
 * HADOOP_HOME
 *
 * LD_LIBRARY_PATH (as in ${HADOOP_HOME}/lib/native)
 *
 * A simple log4 configuration (INFO) is provided.
 *
 * @author Axel Mannhardt
 */
public class TestHdfsSetup {

    public static void main( final String[] args ) throws IOException {
        final Configuration configuration = new Configuration();
        configuration.set( FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost:9000" );
        final FileSystem fs = FileSystem.get( configuration );
        final FileStatus[] contents = fs.listStatus( new Path( "hdfs://localhost:9000/" ) );
        for ( final FileStatus content : contents ) {
            System.out.println( content.getPath() );
        }
    }
}
