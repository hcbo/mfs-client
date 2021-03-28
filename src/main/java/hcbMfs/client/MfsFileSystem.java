package hcbMfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MfsFileSystem extends org.apache.hadoop.fs.FileSystem {

    public static int BUFFERSIZE;

    public static String KAFKA_SERVERS;

    public static String ZK_SERVERS;

    public static final int FS_MFS_DEFAULT_PORT = 8888;

    public static final Logger LOG = LoggerFactory.getLogger(MfsFileSystem.class);

    public static int partNum ;

    private URI uri;
    
    private Path workingDirectory = new Path("/");

    private NeuUnderFileSystem neuUnderFileSystem;

    public URI getUri() {
        return uri;
    }

    public String getScheme() {
        return "hcbMfs";
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException { // get
        super.initialize(uri, conf);
        PropertyConfigurator.configure(getLog4jProps());
        if (conf.get("bufferSize") == null || conf.get("bufferSize") == "") {
            BUFFERSIZE = PropertyUtils.getBufferSize();
        } else {
            BUFFERSIZE = Integer.parseInt(conf.get("bufferSize"));
        }
        if (conf.get("partNum") == null || conf.get("partNum") == "") {
            partNum = PropertyUtils.getPartNum();
        } else {
            partNum = Integer.parseInt(conf.get("partNum"));
        }
        ZK_SERVERS = conf.get("zkServers");
        KAFKA_SERVERS = conf.get("kafkaServers");
        this.uri = uri;
        neuUnderFileSystem = new NeuUnderFileSystem(uri, conf);
        LOG.info("initialized " + uri);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        LOG.info("open path: {} bufferSize:{}", path, bufferSize);
        try {
            InputStream inputStream = neuUnderFileSystem.open(path.toString());
            return new FSDataInputStream(inputStream);
        } catch (Exception ex) {
            throw new FileNotFoundException();
        }
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, final boolean overwrite, final int bufferSize,
                                     final short replication, final long blockSize, final Progressable progress) throws IOException {

        LOG.info("create path: {} bufferSize:{} blockSize:{}", path, bufferSize, blockSize);
        OutputStream outputStream = neuUnderFileSystem.create(path.toString());
        return new FSDataOutputStream(outputStream, statistics);

    }

    /**
     * @throws FileNotFoundException if the parent directory is not present or
     * is not a directory.
     */
    @Override
    public FSDataOutputStream createNonRecursive(Path path,
                                                 FsPermission permission,
                                                 EnumSet<CreateFlag> flags,
                                                 int bufferSize,
                                                 short replication,
                                                 long blockSize,
                                                 Progressable progress) throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            // expect this to raise an exception if there is no parent
            if (!getFileStatus(parent).isDirectory()) {
                throw new FileAlreadyExistsException("Not a directory: " + parent);
            }
        }
        return create(path, permission,
                flags.contains(CreateFlag.OVERWRITE), bufferSize,
                replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progressable) {
        return neuUnderFileSystem.nop();
    }

    @Override
    public boolean rename(Path src, Path dst) {

        try {
            return neuUnderFileSystem.renameFile(src.toString(),dst.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    @Override
    public boolean delete(Path path, boolean recursive) {
         return neuUnderFileSystem.deleteFile(path.toString());
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return  neuUnderFileSystem.listStatus(path.toString());
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDirectory;
    }

    @Override
    public void setWorkingDirectory(Path path) {
        if (path.isAbsolute()) {
            workingDirectory = path;
        } else {
            workingDirectory = new Path(workingDirectory, path);
        }
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        return neuUnderFileSystem.mkdirs(path.toString());
    }

    @Override
    public FileStatus getFileStatus(Path path) {
        return neuUnderFileSystem.getFileStatus(path);
    }


    @Override
    public void setOwner(Path path, final String owner, final String group) {}



    @Override
    public void setPermission(Path path, final FsPermission permission) throws IOException {
        if (permission == null) {
            throw new IllegalArgumentException("The permission can't be null");
        }

    }


    /**
     * Concat existing files together.
     *
     * @param trg   the path to the target destination.
     * @param psrcs the paths to the sources to use for the concatenation.
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default).
     */
    @Override
    public void concat(final Path trg, final Path[] psrcs) throws IOException {
        throw new UnsupportedOperationException("Not implemented by the " +
                getClass().getSimpleName() + " FileSystem implementation");
    }

    /**
     * Truncate the file in the indicated path to the indicated size.
     * <ul>
     * <li>Fails if path is a directory.</li>
     * <li>Fails if path does not exist.</li>
     * <li>Fails if path is not closed.</li>
     * <li>Fails if new size is greater than current size.</li>
     * </ul>
     *
     * @param f         The path to the file to be truncated
     * @param newLength The size the file is to be truncated to
     * @return <code>true</code> if the file has been truncated to the desired
     * <code>newLength</code> and is immediately available to be reused for
     * write operations such as <code>append</code>, or
     * <code>false</code> if a background process of adjusting the length of
     * the last block has been started, and clients should wait for it to
     * complete before proceeding with further file updates.
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default).
     */
    @Override
    public boolean truncate(Path f, long newLength) throws IOException {
        throw new UnsupportedOperationException("Not implemented by the " +
                getClass().getSimpleName() + " FileSystem implementation");
    }

    @Override
    public void createSymlink(final Path target, final Path link,
                              final boolean createParent) throws AccessControlException,
            FileAlreadyExistsException, FileNotFoundException,
            ParentNotDirectoryException, UnsupportedFileSystemException,
            IOException {
        throw new UnsupportedOperationException(
                "Filesystem does not support symlinks!");
    }

    public boolean supportsSymlinks() {
        return false;
    }

    /**
     * Create a snapshot.
     *
     * @param path         The directory where snapshots will be taken.
     * @param snapshotName The name of the snapshot
     * @return the snapshot path.
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     */
    @Override
    public Path createSnapshot(Path path, String snapshotName)
            throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support createSnapshot");
    }

    /**
     * Rename a snapshot.
     *
     * @param path            The directory path where the snapshot was taken
     * @param snapshotOldName Old name of the snapshot
     * @param snapshotNewName New name of the snapshot
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void renameSnapshot(Path path, String snapshotOldName,
                               String snapshotNewName) throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support renameSnapshot");
    }

    /**
     * Delete a snapshot of a directory.
     *
     * @param path         The directory that the to-be-deleted snapshot belongs to
     * @param snapshotName The name of the snapshot
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void deleteSnapshot(Path path, String snapshotName)
            throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support deleteSnapshot");
    }

    /**
     * Modifies ACL entries of files and directories.  This method can add new ACL
     * entries or modify the permissions on existing ACL entries.  All existing
     * ACL entries that are not specified in this call are retained without
     * changes.  (Modifications are merged into the current ACL.)
     *
     * @param path    Path to modify
     * @param aclSpec List&lt;AclEntry&gt; describing modifications
     * @throws IOException                   if an ACL could not be modified
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
            throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support modifyAclEntries");
    }

    /**
     * Removes ACL entries from files and directories.  Other ACL entries are
     * retained.
     *
     * @param path    Path to modify
     * @param aclSpec List describing entries to remove
     * @throws IOException                   if an ACL could not be modified
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void removeAclEntries(Path path, List<AclEntry> aclSpec)
            throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support removeAclEntries");
    }

    /**
     * Removes all default ACL entries from files and directories.
     *
     * @param path Path to modify
     * @throws IOException                   if an ACL could not be modified
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void removeDefaultAcl(Path path)
            throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support removeDefaultAcl");
    }

    /**
     * Removes all but the base ACL entries of files and directories.  The entries
     * for user, group, and others are retained for compatibility with permission
     * bits.
     *
     * @param path Path to modify
     * @throws IOException                   if an ACL could not be removed
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void removeAcl(Path path)
            throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support removeAcl");
    }

    /**
     * Fully replaces ACL of files and directories, discarding all existing
     * entries.
     *
     * @param path    Path to modify
     * @param aclSpec List describing modifications, which must include entries
     *                for user, group, and others for compatibility with permission bits.
     * @throws IOException                   if an ACL could not be modified
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support setAcl");
    }

    /**
     * Gets the ACL of a file or directory.
     *
     * @param path Path to get
     * @return AclStatus describing the ACL of the file or directory
     * @throws IOException                   if an ACL could not be read
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public AclStatus getAclStatus(Path path) throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support getAclStatus");
    }

    /**
     * Set an xattr of a file or directory.
     * The name must be prefixed with the namespace followed by ".". For example,
     * "user.attr".
     * <p>
     * Refer to the HDFS extended attributes user documentation for details.
     *
     * @param path  Path to modify
     * @param name  xattr name.
     * @param value xattr value.
     * @param flag  xattr set flag
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void setXAttr(Path path, String name, byte[] value,
                         EnumSet<XAttrSetFlag> flag) throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support setXAttr");
    }

    /**
     * Get an xattr name and value for a file or directory.
     * The name must be prefixed with the namespace followed by ".". For example,
     * "user.attr".
     * <p>
     * Refer to the HDFS extended attributes user documentation for details.
     *
     * @param path Path to get extended attribute
     * @param name xattr name.
     * @return byte[] xattr value.
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public byte[] getXAttr(Path path, String name) throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support getXAttr");
    }

    /**
     * Get all of the xattr name/value pairs for a file or directory.
     * Only those xattrs which the logged-in user has permissions to view
     * are returned.
     * <p>
     * Refer to the HDFS extended attributes user documentation for details.
     *
     * @param path Path to get extended attributes
     * @return Map describing the XAttrs of the file or directory
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public Map<String, byte[]> getXAttrs(Path path) throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support getXAttrs");
    }

    /**
     * Get all of the xattrs name/value pairs for a file or directory.
     * Only those xattrs which the logged-in user has permissions to view
     * are returned.
     * <p>
     * Refer to the HDFS extended attributes user documentation for details.
     *
     * @param path  Path to get extended attributes
     * @param names XAttr names.
     * @return Map describing the XAttrs of the file or directory
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public Map<String, byte[]> getXAttrs(Path path, List<String> names)
            throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support getXAttrs");
    }

    /**
     * Get all of the xattr names for a file or directory.
     * Only those xattr names which the logged-in user has permissions to view
     * are returned.
     * <p>
     * Refer to the HDFS extended attributes user documentation for details.
     *
     * @param path Path to get extended attributes
     * @return List{@literal <String>} of the XAttr names of the file or directory
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public List<String> listXAttrs(Path path) throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support listXAttrs");
    }

    /**
     * Remove an xattr of a file or directory.
     * The name must be prefixed with the namespace followed by ".". For example,
     * "user.attr".
     * <p>
     * Refer to the HDFS extended attributes user documentation for details.
     *
     * @param path Path to remove extended attribute
     * @param name xattr name
     * @throws IOException                   IO failure
     * @throws UnsupportedOperationException if the operation is unsupported
     *                                       (default outcome).
     */
    @Override
    public void removeXAttr(Path path, String name) throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName()
                + " doesn't support removeXAttr");
    }


    private Properties getLog4jProps(){
        Properties props = new Properties();
        String logPath = PropertyUtils.getLogPath();
        props.put("log4j.appender.FileAppender","org.apache.log4j.RollingFileAppender");
        props.put("log4j.appender.FileAppender.File",logPath);
        props.put("log4j.appender.FileAppender.layout","org.apache.log4j.PatternLayout");
        props.put("log4j.appender.FileAppender.layout.ConversionPattern","%-4r [%t] %-5p %c %x - %m%n");
        props.put("log4j.rootLogger","INFO, FileAppender");
        return props;
    }

}
