package org.apache.hadoop.fs.thriftfs.server;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.thriftfs.api.BlockLocation;
import org.apache.hadoop.thriftfs.api.FileStatus;
import org.apache.hadoop.thriftfs.api.Pathname;
import org.apache.hadoop.thriftfs.api.ThriftHadoopFileSystem;
import org.apache.hadoop.thriftfs.api.ThriftHandle;
import org.apache.hadoop.thriftfs.api.ThriftIOException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger( HdfsService.class );

    private final HdfsConfig _config;
    private final ResourceByIdStore<InputStream> _readStreamStore = new ResourceByIdStore<InputStream>();
    private final ResourceByIdStore<OutputStream> _writeStreamStore = new ResourceByIdStore<OutputStream>();

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
    public boolean closeReadHandle( final ThriftHandle out ) throws ThriftIOException, TException {
        System.out.println( "releasing handle stream: " + out.getId() );
        return _readStreamStore.release( out.getId() );
    }

    @Override
    public boolean closeWriteHandle( final ThriftHandle in ) throws ThriftIOException, TException {
        System.out.println( "releasing handle stream: " + in.getId() );
        return _writeStreamStore.release( in.getId() );
    }

    @Override
    public ThriftHandle create( final Pathname pathname ) throws ThriftIOException, TException {
        final FileSystem fs = Utils.tryToGetFileSystem( _config );
        final Path path = Utils.toPath( pathname );
        try {
            LOG.info( "creating new write handle on " + _config + " for: " + pathname.getPathname() );
            final OutputStream stream = openOutputStream( pathname, fs, path );
            return new ThriftHandle( _writeStreamStore.storeNew( stream ) );
        } catch ( final IOException e ) {
            throw Utils.wrapAsThriftException( e );
        }
    }

    private OutputStream openOutputStream( final Pathname pathname, final FileSystem fs, final Path fullPath ) throws IOException {
        final FSDataOutputStream stream = fs.create( fullPath );
        final Configuration conf = new Configuration();
        final CompressionCodecFactory factory = new CompressionCodecFactory( conf );
        final CompressionCodec codec = factory.getCodec( fullPath );
        if ( codec == null ) {
            return stream;
        }
        final CompressionOutputStream compressedStream = codec.createOutputStream( stream );
        return compressedStream;
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
        final Path path = Utils.toPath( pathname );
        final FileSystem fs = Utils.tryToGetFileSystem( _config );
        final List<BlockLocation> result = new ArrayList<BlockLocation>();
        try {
            for ( final org.apache.hadoop.fs.BlockLocation bl : fs.getFileBlockLocations( path, start, length ) ) {
                result.add( convertBlockLocation( bl ) );
            }
        } catch ( final IOException e ) {
            throw Utils.wrapAsThriftException( e );
        }
        return result;
    }

    private static BlockLocation convertBlockLocation( final org.apache.hadoop.fs.BlockLocation bl ) throws IOException {
        final BlockLocation blockLocation = new BlockLocation();
        blockLocation.setHosts( Arrays.asList( bl.getHosts() ) );
        blockLocation.setHostsIsSet( true );
        blockLocation.setLength( bl.getLength() );
        blockLocation.setNames( Arrays.asList( bl.getNames() ) );
        blockLocation.setNamesIsSet( true );
        blockLocation.setOffset( bl.getOffset() );
        blockLocation.setOffsetIsSet( true );
        return blockLocation;
    }

    @Override
    public List<FileStatus> listStatus( final Pathname pathname ) throws ThriftIOException, TException {
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

    /**
     * See thrift definition: open for reading only.
     */
    @Override
    public ThriftHandle open( final Pathname pathname ) throws ThriftIOException, TException {
        final FileSystem fs = Utils.tryToGetFileSystem( _config );
        final Path path = Utils.toPath( pathname );
        try {
            final Path fullPath = fs.resolvePath( path ); //  optional: check early (before doing it implicit while opening) that the path exists
            LOG.info( "creating new read handle on " + _config + " for: " + pathname.getPathname() );
            final InputStream stream = openInputStream( pathname, fs, fullPath );
            return new ThriftHandle( _readStreamStore.storeNew( stream ) );
        } catch ( final IOException e ) {
            throw Utils.wrapAsThriftException( e );
        }
    }

    private static InputStream openInputStream( final Pathname pathname, final FileSystem fs, final Path fullPath )
        throws IOException {
        final FSDataInputStream stream = fs.open( fullPath );
        final Configuration conf = new Configuration();
        final CompressionCodecFactory factory = new CompressionCodecFactory( conf );
        final CompressionCodec codec = factory.getCodec( fullPath );
        if ( codec == null ) {
            return stream;
        }
        final InputStream uncompressedStream = codec.createInputStream( stream );
        return uncompressedStream;
    }

    // FIXME: API: shouldn't this be of some binary return type (string encoding)???
    @Override
    public String read( final ThriftHandle handle, final long offset, final int size ) throws ThriftIOException, TException {
        final InputStream stream = _readStreamStore.getResource( handle.getId() );
        if ( stream == null ) {
            throw new ThriftIOException( "unknown read handle: " + handle.getId() );
        }
        if ( offset > Integer.MAX_VALUE ) {
            throw new IllegalArgumentException( "long offset not supported yet by thrift service" );
        }
        byte[] res;
        try {
            res = IOUtils.toByteArray( stream );
        } catch ( final IOException e ) {
            throw Utils.wrapAsThriftException( e );
        }
        final byte[] result = Arrays.copyOf( res, res.length );
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

    /**
     * As the common exceptions are handled as such, I am quite unsure, what the
     * return value is meant for in that stolen API description. Returns false
     * if the handle id is unknown.
     */
    @Override
    public boolean write( final ThriftHandle handle, final String data ) throws ThriftIOException, TException {
        final OutputStream stream = _writeStreamStore.getResource( handle.getId() );
        if ( stream == null ) {
            return false;
        }
        try {
            stream.write( data.getBytes() );
        } catch ( final IOException e ) {
            throw Utils.wrapAsThriftException( e );
        }
        return true;
    }

    static class HdfsConfig {
        private final String _host;
        private final int _port;

        public HdfsConfig( final String host, final int port ) {
            _host = host;
            _port = port;
        }

        String hdfsPath() {
            return "hdfs://" + _host + ":" + _port;
        }

        @Override
        public String toString() {
            return _host + ":" + _port;
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

        ResourceByIdStore() {
            Runtime.getRuntime().addShutdownHook( new Thread() {
                @Override
                public void run() {
                    cleanup();
                }
            } );
        }

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
            System.out.println( "Allocated resources: " + _store.size() );
            return nextId;
        }

        RESOURCE_TYPE getResource( final long id ) {
            return _store.get( keyOf( id ) );
        }

        boolean release( final long id ) {
            final RESOURCE_TYPE res = _store.get( keyOf( id ) );
            if ( res == null ) {
                System.out.println( "Cannot close unknown resource: " + id );
                return false;
            }
            IOUtils.closeQuietly( res );
            _store.remove( keyOf( id ) );
            System.out.println( "Remaining resources: " + _store.size() );
            return true;
        }

        private long nextId() {
            long nextId = _lastAssignedId + 1;
            boolean overflow = false;
            while ( _store.containsKey( keyOf( nextId ) ) ) {
                if ( nextId < Long.MAX_VALUE ) {
                    nextId++;
                } else {
                    if ( !overflow ) {
                        nextId = FIRST_ID;
                        overflow = true;
                    } else {
                        System.err.println( "FATAL: no free ids remaining" );
                        System.exit( 1 );
                    }
                }
            }
            _lastAssignedId = nextId;
            return nextId;
        }

        private static Long keyOf( final long l ) {
            return Long.valueOf( l );
        }

        private void cleanup() {
            for ( final Long id : _store.keySet() ) {
                release( id.longValue() );
            }
        }

    }

}