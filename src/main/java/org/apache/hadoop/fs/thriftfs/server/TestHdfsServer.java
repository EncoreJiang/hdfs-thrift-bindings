package org.apache.hadoop.fs.thriftfs.server;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thriftfs.api.BlockLocation;
import org.apache.hadoop.thriftfs.api.FileStatus;
import org.apache.hadoop.thriftfs.api.Pathname;
import org.apache.hadoop.thriftfs.api.ThriftHadoopFileSystem;
import org.apache.hadoop.thriftfs.api.ThriftHadoopFileSystem.Iface;
import org.apache.hadoop.thriftfs.api.ThriftHadoopFileSystem.Processor;
import org.apache.hadoop.thriftfs.api.ThriftHandle;
import org.apache.hadoop.thriftfs.api.ThriftIOException;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * A thrift server implementing hdfs bindings.
 *
 * Disclaimer: I still feel this implementation is redundant, but I have not yet
 * found an official variant nor an already used community addition.
 *
 * @author Axel Mannhardt
 */
public class TestHdfsServer {

    public static void main( final String[] args ) {

        final HdfsService service = new HdfsService();
        final Processor<Iface> processor = new ThriftHadoopFileSystem.Processor<Iface>( service );

        new Thread( new Runnable() {
            @Override
            public void run() {
                doIt( processor );
            }
        } ).start();

    }

    private static void doIt( final Processor<Iface> processor ) {

        TServerTransport serverTransport;
        try {
            serverTransport = new TServerSocket( 44444 );
        } catch ( final TTransportException e ) {
            throw new IOError( e );
        }
        final TServer server = new TSimpleServer( new Args( serverTransport ).processor( processor ) );
        // TODO threading
        // TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
        server.serve();
    }

    private static final class HdfsService implements ThriftHadoopFileSystem.Iface {

        @Override
        public ThriftHandle append( final Pathname path ) throws ThriftIOException, TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

        @Override
        public void chmod( final Pathname path, final short mode ) throws ThriftIOException, TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

        @Override
        public void chown( final Pathname path, final String owner, final String group ) throws ThriftIOException, TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

        @Override
        public boolean close( final ThriftHandle out ) throws ThriftIOException, TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

        @Override
        public ThriftHandle create( final Pathname path ) throws ThriftIOException, TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

        @Override
        public ThriftHandle createFile( final Pathname path, final short mode, final boolean overwrite, final int bufferSize,
                final short block_replication, final long blocksize ) throws ThriftIOException, TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

        @Override
        public boolean exists( final Pathname path ) throws ThriftIOException, TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

        @Override
        public List<BlockLocation> getFileBlockLocations( final Pathname path, final long start, final long length )
            throws ThriftIOException, TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

        @Override
        public List<FileStatus> listStatus( final Pathname pathname ) throws ThriftIOException, TException {
            System.out.println( "receiving" );
            final Configuration configuration = new Configuration();
            configuration.set( FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost:9000" );
            FileSystem fs;
            try {
                fs = FileSystem.get( configuration );
            } catch ( final IOException e ) {
                throw new ThriftIOException( printStacktraceToString( e ) );
            }
            final List<FileStatus> result = new ArrayList<FileStatus>();
            try {
                for ( final org.apache.hadoop.fs.FileStatus f : fs.listStatus( new Path( pathname.getPathname() ) ) ) {
                    final String path = f.getPath().toString();
                    final long length = f.getLen();
                    final boolean isdir = f.isDirectory();
                    final short block_replication = f.getReplication();
                    final long blocksize = f.getBlockSize();
                    final long modification_time = f.getModificationTime();
                    final String permission = f.getPermission().toString();
                    final String owner = f.getOwner();
                    final String group = f.getGroup();
                    result.add( new FileStatus( path, length, isdir, block_replication, blocksize, modification_time, permission,
                            owner, group ) );
                }
            } catch ( final FileNotFoundException e ) {
                e.printStackTrace();
                return null;
            } catch ( IllegalArgumentException | IOException e ) {
                throw new ThriftIOException( printStacktraceToString( e ) );
            }
            return result;
        }

        private static String printStacktraceToString( final Exception e ) {
            PrintStream printStream = null;
            try {
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                printStream = new PrintStream( out, true );
                e.printStackTrace( printStream );
                printStream.flush();
                final String msg = out.toString();
                return msg;
            } finally {
                IOUtils.closeQuietly( printStream );
            }
        }

        @Override
        public boolean mkdirs( final Pathname path ) throws ThriftIOException, TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

        @Override
        public ThriftHandle open( final Pathname path ) throws ThriftIOException, TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

        @Override
        public String read( final ThriftHandle handle, final long offset, final int size ) throws ThriftIOException, TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

        @Override
        public boolean rename( final Pathname path, final Pathname dest ) throws ThriftIOException, TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

        @Override
        public boolean rm( final Pathname path, final boolean recursive ) throws ThriftIOException, TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

        @Override
        public void setInactivityTimeoutPeriod( final long periodInSeconds ) throws TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

        @Override
        public void setReplication( final Pathname path, final short replication ) throws ThriftIOException, TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

        @Override
        public void shutdown( final int status ) throws TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

        @Override
        public FileStatus stat( final Pathname path ) throws ThriftIOException, TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

        @Override
        public boolean write( final ThriftHandle handle, final String data ) throws ThriftIOException, TException {
            throw new UnsupportedOperationException( "Not supported by thrift service." );
        }

    }

}
