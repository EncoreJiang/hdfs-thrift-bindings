package org.apache.hadoop.fs.thriftfs.server;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thriftfs.api.BlockLocation;
import org.apache.hadoop.thriftfs.api.FileStatus;
import org.apache.hadoop.thriftfs.api.Pathname;
import org.apache.hadoop.thriftfs.api.ThriftHadoopFileSystem;
import org.apache.hadoop.thriftfs.api.ThriftHandle;
import org.apache.hadoop.thriftfs.api.ThriftIOException;
import org.apache.thrift.TException;

/**
 * A thrift server implementing hdfs bindings.
 *
 * Disclaimer: I still feel this implementation is redundant, but I have not yet
 * found an official variant nor an already used community addition.
 *
 * TODO: At the moment, this assumes it is run as a singleton and is not written
 * with concurrency in mind.
 *
 * @author Axel Mannhardt
 */
final class HdfsService implements ThriftHadoopFileSystem.Iface {

    private final HdfsConfig _config;
    private final ResourceByIdStore<FSDataInputStream> _readStreamStore = new ResourceByIdStore<FSDataInputStream>();

    public HdfsService( final HdfsConfig config ) {
        _config = config;
    }

    @Override
    public ThriftHandle append( final Pathname pathname ) throws ThriftIOException, TException {
        throw new UnsupportedOperationException( "Not supported by thrift service." );
    }

    @Override
    public void chmod( final Pathname pathname, final short mode ) throws ThriftIOException, TException {
        throw new UnsupportedOperationException( "Not supported by thrift service." );
    }

    @Override
    public void chown( final Pathname pathname, final String owner, final String group ) throws ThriftIOException, TException {
        throw new UnsupportedOperationException( "Not supported by thrift service." );
    }

    @Override
    public boolean close( final ThriftHandle out ) throws ThriftIOException, TException {
        // FIXME is this meant only for writng data? how can we distinguish resources for reading and writing??? (API problem?)
        System.out.println( "releasing handle stream: " + out.getId() );
        return _readStreamStore.release( out.getId() );
        // TODO additional hdfs close necessary?
    }

    @Override
    public ThriftHandle create( final Pathname pathname ) throws ThriftIOException, TException {
        throw new UnsupportedOperationException( "Not supported by thrift service." );
    }

    @Override
    public ThriftHandle createFile( final Pathname pathname, final short mode, final boolean overwrite, final int bufferSize,
            final short block_replication, final long blocksize ) throws ThriftIOException, TException {
        throw new UnsupportedOperationException( "Not supported by thrift service." );
    }

    @Override
    public boolean exists( final Pathname pathname ) throws ThriftIOException, TException {
        throw new UnsupportedOperationException( "Not supported by thrift service." );
    }

    @Override
    public List<BlockLocation> getFileBlockLocations( final Pathname pathname, final long start, final long length )
        throws ThriftIOException, TException {
        throw new UnsupportedOperationException( "Not supported by thrift service." );
    }

    @Override
    public List<FileStatus> listStatus( final Pathname pathname ) throws ThriftIOException, TException {
        System.out.println( "doing listStatus on " + pathname );
        final FileSystem fs = Utils.tryToGetFileSystem( _config );
        final List<FileStatus> result = new ArrayList<FileStatus>();
        try {
            for ( final org.apache.hadoop.fs.FileStatus f : fs.listStatus( Utils.toPath( pathname ) ) ) {
                result.add( convertFileStatus( f ) );
            }
        } catch ( final FileNotFoundException e ) {
            e.printStackTrace();
            return null;
        } catch ( IllegalArgumentException | IOException e ) {
            throw Utils.wrapAsThriftException( e );
        }
        return result;
    }

    private static FileStatus convertFileStatus( final org.apache.hadoop.fs.FileStatus f ) {
        final String path = f.getPath().toString();
        final long length = f.getLen();
        final boolean isdir = f.isDirectory();
        final short block_replication = f.getReplication();
        final long blocksize = f.getBlockSize();
        final long modification_time = f.getModificationTime();
        final String permission = f.getPermission().toString();
        final String owner = f.getOwner();
        final String group = f.getGroup();
        final FileStatus fileStatus =
                new FileStatus( path, length, isdir, block_replication, blocksize, modification_time, permission, owner, group );
        return fileStatus;
    }

    @Override
    public boolean mkdirs( final Pathname pathname ) throws ThriftIOException, TException {
        throw new UnsupportedOperationException( "Not supported by thrift service." );
    }

    @Override
    public ThriftHandle open( final Pathname pathname ) throws ThriftIOException, TException {
        final FileSystem fs = Utils.tryToGetFileSystem( _config );
        try {
            final FSDataInputStream stream = fs.open( Utils.toPath( pathname ) );
            System.out.println( "creating new handle stream" );
            return new ThriftHandle( _readStreamStore.storeNew( stream ) );
        } catch ( final IOException e ) {
            throw Utils.wrapAsThriftException( e );
        }
    }

    // FIXME: API: shouldn't this be of some binary return type (string encoding)???
    @Override
    public String read( final ThriftHandle handle, final long offset, final int size ) throws ThriftIOException, TException {
        final FSDataInputStream stream = _readStreamStore.getResource( handle.getId() );
        final byte[] res = new byte[size];
        if ( offset > Integer.MAX_VALUE ) {
            throw new IllegalArgumentException( "long offset not supported yet by thrift service" );
        }
        final int off = Long.valueOf( offset ).intValue();
        final int numRead;
        try {
            numRead = stream.read( res, off, size );
        } catch ( final IOException e ) {
            throw Utils.wrapAsThriftException( e );
        }
        final byte[] result = Arrays.copyOf( res, numRead );
        return new String( result );
    }

    @Override
    public boolean rename( final Pathname pathname, final Pathname dest ) throws ThriftIOException, TException {
        throw new UnsupportedOperationException( "Not supported by thrift service." );
    }

    @Override
    public boolean rm( final Pathname pathname, final boolean recursive ) throws ThriftIOException, TException {
        throw new UnsupportedOperationException( "Not supported by thrift service." );
    }

    @Override
    public void setInactivityTimeoutPeriod( final long periodInSeconds ) throws TException {
        throw new UnsupportedOperationException( "Not supported by thrift service." );
    }

    @Override
    public void setReplication( final Pathname pathname, final short replication ) throws ThriftIOException, TException {
        throw new UnsupportedOperationException( "Not supported by thrift service." );
    }

    @Override
    public void shutdown( final int status ) throws TException {
        throw new UnsupportedOperationException( "Not supported by thrift service." );
    }

    @Override
    public FileStatus stat( final Pathname pathname ) throws ThriftIOException, TException {
        throw new UnsupportedOperationException( "Not supported by thrift service." );
    }

    @Override
    public boolean write( final ThriftHandle handle, final String data ) throws ThriftIOException, TException {
        throw new UnsupportedOperationException( "Not supported by thrift service." );
    }

    static class HdfsConfig {
        private final String _host;
        private final int _port;

        public HdfsConfig( final String host, final int port ) {
            _host = host;
            _port = port;
        }

        String hdfsPath() {
            // TODO Auto-generated method stub
            return "hdfs://" + _host + ":" + _port;
        }

    }

    private static final class Utils {
        private Utils() {
            // no instances for utils
        }

        static Path toPath( final Pathname pathname ) {
            return new Path( pathname.getPathname() );
        }

        static FileSystem tryToGetFileSystem( final HdfsConfig config ) throws ThriftIOException {
            FileSystem fs;
            final Configuration configuration = new Configuration();
            configuration.set( FileSystem.FS_DEFAULT_NAME_KEY, config.hdfsPath() );
            try {
                fs = FileSystem.get( configuration );
            } catch ( final IOException e ) {
                throw new ThriftIOException( printStacktraceToString( e ) );
            }
            return fs;
        }

        static ThriftIOException wrapAsThriftException( final Exception e ) {
            return new ThriftIOException( Utils.printStacktraceToString( e ) );
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
    }

    /**
     * Simple resource store - not thread safe.
     *
     * @author Axel Mannhardt
     */
    private static final class ResourceByIdStore<RESOURCE_TYPE extends Closeable> {

        private static final long FIRST_ID = 1;

        private final Map<Long, RESOURCE_TYPE> _store = new HashMap<Long, RESOURCE_TYPE>();
        private long _lastAssignedId = FIRST_ID - 1;

        long storeNew( final RESOURCE_TYPE res ) {
            final long nextId = nextId();
            final Long nextKey = keyOf( nextId );
            if ( _store.containsKey( nextKey ) ) {
                throw new IllegalStateException( "tried to overwrite resource" );
            }
            final RESOURCE_TYPE previous = _store.put( nextKey, res );
            if ( previous != null ) {
                System.err.println( "destroyed other resource while creating new!" );
            }
            return nextId;
        }

        RESOURCE_TYPE getResource( final long id ) {
            return _store.get( keyOf( id ) );
        }

        boolean release( final long id ) {
            final RESOURCE_TYPE res = _store.get( keyOf( id ) );
            if ( res == null ) {
                return false;
            }
            IOUtils.closeQuietly( res );
            return true;
        }

        private long nextId() {
            long nextId = _lastAssignedId + 1;
            while ( _store.containsKey( keyOf( nextId ) ) ) {
                if ( nextId < Long.MAX_VALUE ) {
                    nextId++;
                } else {
                    nextId = FIRST_ID;
                }
            }
            _lastAssignedId = nextId;
            return nextId;
        }

        private static Long keyOf( final long l ) {
            return Long.valueOf( l );
        }

    }

}