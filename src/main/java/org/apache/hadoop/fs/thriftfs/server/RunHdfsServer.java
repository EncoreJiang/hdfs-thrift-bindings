package org.apache.hadoop.fs.thriftfs.server;

import java.io.IOError;

import org.apache.hadoop.fs.thriftfs.server.HdfsService.HdfsConfig;
import org.apache.hadoop.thriftfs.api.ThriftHadoopFileSystem;
import org.apache.hadoop.thriftfs.api.ThriftHadoopFileSystem.Iface;
import org.apache.hadoop.thriftfs.api.ThriftHadoopFileSystem.Processor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Runs the service (on port 55555).
 *
 * @author Axel Mannhardt
 */
public class RunHdfsServer {

    public static void main( final String[] args ) {

        final HdfsService service = new HdfsService( new HdfsConfig( "localhost", 8020 ) );
        final Processor<Iface> processor = new ThriftHadoopFileSystem.Processor<Iface>( service );

        new Thread( new Runnable() {
            @Override
            public void run() {
                runServerProcess( processor );
            }
        } ).start();

    }

    private static void runServerProcess( final Processor<Iface> processor ) {

        TServerTransport serverTransport;
        try {
            serverTransport = new TServerSocket( 55555 );
        } catch ( final TTransportException e ) {
            throw new IOError( e );
        }
        final TServer server = new TSimpleServer( new Args( serverTransport ).processor( processor ) );
        // TODO threading
        // TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
        server.serve();
    }

}
