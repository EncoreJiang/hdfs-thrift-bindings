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

        if ( args.length != 3 ) {
            System.out.println( "Usage: " + RunHdfsServer.class.getSimpleName() + " <service-port> <hdfs-host> <hdfs-port>" );
            System.exit( 1 );
        }

        final int servicePort = Integer.parseInt( args[0] );
        final String hdfsHost = args[1];
        final int hdfsPort = Integer.parseInt( args[2] );

        final HdfsService service = new HdfsService( new HdfsConfig( hdfsHost, hdfsPort ) );
        final Processor<Iface> processor = new ThriftHadoopFileSystem.Processor<Iface>( service );

        new Thread( new Runnable() {
            @Override
            public void run() {
                runServerProcess( processor, servicePort );
            }
        } ).start();

    }

    private static void runServerProcess( final Processor<Iface> processor, final int port ) {

        TServerTransport serverTransport;
        try {
            serverTransport = new TServerSocket( port );
        } catch ( final TTransportException e ) {
            throw new IOError( e );
        }
        final TServer server = new TSimpleServer( new Args( serverTransport ).processor( processor ) );
        // TODO threading
        // TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
        server.serve();
    }

}
