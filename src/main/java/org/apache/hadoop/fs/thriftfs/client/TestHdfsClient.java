package org.apache.hadoop.fs.thriftfs.client;

import java.util.List;

import org.apache.hadoop.thriftfs.api.FileStatus;
import org.apache.hadoop.thriftfs.api.Pathname;
import org.apache.hadoop.thriftfs.api.ThriftHadoopFileSystem;
import org.apache.hadoop.thriftfs.api.ThriftHadoopFileSystem.Client;
import org.apache.hadoop.thriftfs.api.ThriftHadoopFileSystem.Client.Factory;
import org.apache.hadoop.thriftfs.api.ThriftIOException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * A simple usage demo of the generated thrift client, for development purposes.
 *
 * @author Axel Mannhardt
 */
public class TestHdfsClient {

    public static void main( final String[] args ) throws ThriftIOException, TException {
        final Factory factory = new ThriftHadoopFileSystem.Client.Factory();
        final TTransport transport = new TSocket( "localhost", 44444 );
        try {
            transport.open();
            final Client client = factory.getClient( new TBinaryProtocol.Factory().getProtocol( transport ) );
            final List<FileStatus> contents = client.listStatus( new Pathname( "hdfs://localhost:9000/" ) );
            for ( final FileStatus content : contents ) {
                System.out.println( content.getPath() );
            }
        } finally {
            transport.close();
        }
    }

}
