/*
 * tksCommons / galangal
 *
 * Author : Thomas Kuhlmann (ThK-Systems, http://www.thk-systems.de)
 * License : LGPL (https://www.gnu.org/licenses/lgpl.html)
 */
package de.thksystems.network.ssh.sftp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Stack;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.ChannelSftp.LsEntrySelector;
import com.jcraft.jsch.HostKey;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

import de.thksystems.network.ssh.sftp.SftpFile.SftpFileType;
import de.thksystems.util.text.RandomStringUtils;

/**
 * A simple SFTP client (using jsch).
 */
public final class SftpClient {

    public static final int DEFAULT_PORT = 22;
    public static final int DEFAULT_TIMEOUT = 30 * 1000;
    protected static final String SFTP_DIRECTORY_SEPARATOR = "/";
    private static final Logger LOG = LoggerFactory.getLogger(SftpClient.class);
    private static final boolean DEFAULT_STRICTMODE = true;
    private static final OverwriteMode DEFAULT_OVERWRITEMODE = OverwriteMode.NEVER;
    private static final boolean DEFAULT_TRANSACTIONAL = true;
    private static final boolean DEFAULT_CREATEDIRSAUTOMATICALLY = false;
    private static final boolean DEFAULT_KEEPALIVE = false;
    private static final Long DEFAULT_KEEPALIVEINTERVALL = 5000L;
    private static final boolean DEFAULT_DISABLEHOSTKEYCHECK = false;
    private final FileExistsCall FILEEXISTSCALL_LOCAL = new FileExistsCall() {

        @Override
        public boolean fileExists(String fileName) {
            return new File(fileName).exists();
        }
    };
    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final File privateKeyFile;
    private final String privateKeyPassphrase;
    private int timeout = DEFAULT_TIMEOUT;
    private boolean strictMode = DEFAULT_STRICTMODE;
    private OverwriteMode overwriteMode = DEFAULT_OVERWRITEMODE;
    private boolean transactional = DEFAULT_TRANSACTIONAL;
    private boolean createDirsAutomatically = DEFAULT_CREATEDIRSAUTOMATICALLY;
    private boolean keepAlive = DEFAULT_KEEPALIVE;
    private long keepAliveIntervall = DEFAULT_KEEPALIVEINTERVALL;
    private long keepAliveLastSent = 0;
    private KeepAliveThread keepAliveThread;
    private boolean disableHostkeyCheck = DEFAULT_DISABLEHOSTKEYCHECK;
    private File knownHostsFile;
    private String hostKey;
    private HostKeyType hostKeyType;
    private ChannelSftp sftpChannel;
    private final FileExistsCall FILEEXISTSCALL_REMOTE = new FileExistsCall() {

        @Override
        public boolean fileExists(String fileName) throws SftpClientException {
            return remoteFileExists(fileName);
        }
    };

    /**
     * Create a sftp client (no connection is done while creating).
     *
     * @param host     Host name or ip address
     * @param port     Port (default 22)
     * @param user     User name
     * @param password Password
     */
    public SftpClient(String host, int port, String user, String password) {
        super();
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.privateKeyFile = null;
        this.privateKeyPassphrase = null;
        LOG.info("Created sftp client '{}@{}:{}'", user, host, port);
    }

    /**
     * Create a sftp client (no connection is done while creating) with a default port of 22 and a default timeout of five seconds.
     *
     * @param host     Host name or ip address
     * @param user     User name
     * @param password Password
     */
    public SftpClient(String host, String user, String password) {
        this(host, DEFAULT_PORT, user, password);
    }

    /**
     * Create a sftp client (no connection is done while creating).
     *
     * @param host                 Host name or ip address
     * @param port                 Port (default 22)
     * @param user                 User name
     * @param privateKeyFile       Key file with private key
     * @param privateKeyPassphrase Passphrase for private key (may be null)
     */
    public SftpClient(String host, int port, String user, File privateKeyFile, String privateKeyPassphrase) {
        super();
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = null;
        this.privateKeyFile = privateKeyFile;
        this.privateKeyPassphrase = privateKeyPassphrase;
        LOG.info("Created sftp client '{}@{}:{}' with private key file '{}' and passphrase (hidden)", user, host, port, privateKeyFile);
    }

    /**
     * Create a sftp client (no connection is done while creating) with a default port of 22 and a default timeout of five seconds.
     *
     * @param host                 Host name or ip address
     * @param user                 User name
     * @param privateKeyFile       Key file with private key
     * @param privateKeyPassphrase Passphrase for private key (may be null)
     */
    public SftpClient(String host, String user, File privateKeyFile, String privateKeyPassphrase) {
        this(host, DEFAULT_PORT, user, privateKeyFile, privateKeyPassphrase);
    }

    // ================================================================================
    // ============================= CONSTRUCTOR ======================================
    // ================================================================================

    private void checkForActiveConnectionWhileInitializing() {
        if (sftpChannel != null) {
            throw new IllegalStateException("There is already an active sftp connection.");
        }
    }

    /**
     * Sets the timeout for connecting.
     *
     * @param timeoutMilliseconds Timeout in milliseconds.
     */
    public synchronized SftpClient withTimeout(int timeoutMilliseconds) {
        checkForActiveConnectionWhileInitializing();
        this.timeout = timeoutMilliseconds < 0 ? DEFAULT_TIMEOUT : timeoutMilliseconds;
        return this;
    }

    /**
     * Sets the strict mode (Default: Enabled).
     * <p>
     * With enabled strict mode, a {@link FileNotFoundException} is thrown, if an expected local or remote files or folder does not exists. Disabling strict
     * mode speeds up everything a very little bit and makes it possible to use wildcards where file names are expected (e.g.
     * <code>deleteRemoteFile("/tmp/*.tmp")</code>).
     */
    public synchronized SftpClient withStrictMode(boolean strictMode) {
        this.strictMode = strictMode;
        return this;
    }

    /**
     * Sets the overwrite mode (Default: {@link OverwriteMode#NEVER}).
     */
    public synchronized SftpClient withOverwriteMode(OverwriteMode overwriteMode) {
        this.overwriteMode = overwriteMode;
        return this;
    }

    /**
     * Sets the transactional flag. If set to <code>true</code>, all files will be transfered to a temporary filename and renamed after the transfer. Used for
     * upload and download. (Default is <code>true</code>.)
     */
    public synchronized SftpClient withTransactional(boolean transactional) {
        this.transactional = transactional;
        return this;
    }

    /**
     * Disable host key checking. (Default: <code>false</code>)
     */
    public synchronized SftpClient withDisableHostkeyCheck(boolean disableHostkeyCheck) {
        checkForActiveConnectionWhileInitializing();
        this.disableHostkeyCheck = disableHostkeyCheck;
        return this;
    }

    /**
     * Sets the host key (and enables host key checking).
     */
    public synchronized SftpClient withHostKey(String hostKey, HostKeyType hostKeyType) {
        checkForActiveConnectionWhileInitializing();
        this.hostKey = hostKey;
        this.hostKeyType = hostKeyType;
        withDisableHostkeyCheck(false);
        return this;
    }

    /**
     * Sets the 'known_hosts' file (and enables host key checking).
     */
    public synchronized SftpClient withKnownHostsFile(File knownHostsFile) {
        checkForActiveConnectionWhileInitializing();
        this.knownHostsFile = knownHostsFile;
        withDisableHostkeyCheck(false);
        return this;
    }

    /**
     * Create needed local and remote directories on uploading and downloading and moving (if possible).
     */
    public synchronized SftpClient withCreateDirsAutomatically(boolean createDirsOnUpload) {
        this.createDirsAutomatically = createDirsOnUpload;
        return this;
    }

    /**
     * Disable active keep alive at all.
     *
     * @see #withActiveKeepAlive(long)
     */
    public synchronized SftpClient withNoKeepAlive() {
        checkForActiveConnectionWhileInitializing();
        this.keepAlive = false;
        this.keepAliveIntervall = DEFAULT_KEEPALIVEINTERVALL;
        this.keepAliveLastSent = 0;
        return this;
    }

    /**
     * Enable keep alive. A packet is sent every n milliseconds to keep the connection alive. This is done in a separate thread.
     */
    public synchronized SftpClient withActiveKeepAlive(long keepAliveIntervallMilliseconds) {
        checkForActiveConnectionWhileInitializing();
        this.keepAlive = true;
        this.keepAliveIntervall = keepAliveIntervallMilliseconds;
        this.keepAliveLastSent = 0;
        return this;
    }

    /**
     * Gets current connection. Creates a new one, if it does not exists. Reconnects, if the connections is not connected any more.
     */
    protected synchronized ChannelSftp getConnection() throws SftpClientException {
        if (sftpChannel == null) {
            connect();
        } else if (sftpChannel.isClosed() || !sftpChannel.isConnected() || sftpChannel.isEOF() || (!sendKeepAlive())) {
            reconnect();
        }
        return sftpChannel;
    }

    /**
     * Sends a keep alive signal to the server.
     *
     * @return <code>true</code>, if the server could reached, <code>false</code> in case of ANY exception.
     */
    protected boolean sendKeepAlive() {
        return sendKeepAlive(false);
    }

    /**
     * Sends a keep alive signal to the server.
     *
     * @param forced If <code>true</code>, sent the signal, even if the time since the last keep alive signal is less than 'keepAliveIntervall'
     * @return <code>true</code>, if the server could reached, <code>false</code> in case of ANY exception.
     */
    protected boolean sendKeepAlive(boolean forced) {
        try {
            long currentTime = System.currentTimeMillis();
            if (currentTime > (keepAliveLastSent + keepAliveIntervall) || forced) {
                LOG.debug("Sending keep alive");
                sftpChannel.getSession().sendKeepAliveMsg();
                keepAliveLastSent = currentTime;
            }
            return true;
        } catch (Exception e) {
            return false;
        }

    }

    /**
     * Connects to sftp server, creates a session and a channel.
     *
     * @throws SftpClientException In case of any sftp exception
     */
    protected synchronized void connect() throws SftpClientException {
        LOG.debug("Connecting to '{}@{}:{}'", user, host, port);
        try {
            JSch jsch = new JSch();
            JSch.setLogger(new JSchLogger());

            LOG.debug("Creating session");
            final Session session = jsch.getSession(user, host, port);

            if (privateKeyFile != null && privateKeyFile.canRead()) {
                LOG.debug("Using private key authentication with private key file: {}", privateKeyFile);
                jsch.addIdentity(privateKeyFile.getAbsolutePath(), privateKeyPassphrase);
            } else if (StringUtils.isNotEmpty(password)) {
                LOG.debug("Using password-authentication (hidden)");
                session.setPassword(password);
            } else {
                throw new SftpClientException("No credentials given for authentication");
            }

            if (disableHostkeyCheck) {
                LOG.warn("Disabling host key check.");
                java.util.Properties sftpConfig = new java.util.Properties();
                sftpConfig.put("StrictHostKeyChecking", "no");
                session.setConfig(sftpConfig);
            } else {
                if (knownHostsFile != null && knownHostsFile.canRead()) {
                    LOG.debug("Using 'known_hosts' file: {}", knownHostsFile);
                    jsch.setKnownHosts(knownHostsFile.getAbsolutePath());
                } else if (StringUtils.isNotEmpty(hostKey)) {
                    LOG.debug("Using host key '{}' (host key type: '{}')", hostKey, hostKeyType);
                    // jsch.setKnownHosts(new ByteArrayInputStream(hostKey.getBytes()));
                    HostKey hostKeyEntry = new HostKey(host, Base64.decodeBase64(hostKey));
                    jsch.getHostKeyRepository().add(hostKeyEntry, null);
                } else {
                    LOG.warn("Host key check is NOT disabled, but not host key or 'known_hosts' file is provided.");
                }

            }

            LOG.debug("Connecting");
            session.setTimeout(timeout);
            session.connect();

            LOG.debug("Creating sftp channel");
            sftpChannel = (ChannelSftp) session.openChannel("sftp");
            sftpChannel.connect();

            stopKeepAliveThread();
            sendKeepAlive();
            startKeepAliveThread();

        } catch (JSchException e) {
            handleSftpException(e, String.format("Error while connecting to '%s@%s':%d", user, host, port));
        }
    }

    // =======================================================================================
    // ============================= CONNECTION-HANDLING ======================================
    // =======================================================================================

    /**
     * Handles sftp exception.
     */
    protected <T> T handleSftpException(Exception e, String message) throws SftpClientException {
        LOG.error((message != null ? (message + " - {}") : "{}"), e.getMessage(), e);
        keepAliveLastSent = 0; // To force sending a keep alive, next time called
        if (e instanceof SftpClientException) {
            throw (SftpClientException) e;
        } else {
            throw new SftpClientException((message != null ? message + " - " : "") + e.getMessage(), e);
        }
    }

    /**
     * Starts the keep alive thread, if configured.
     */
    protected void startKeepAliveThread() {
        if (keepAlive) {
            LOG.debug("Starting keep alive thread.");
            keepAliveThread = new KeepAliveThread();
            keepAliveThread.start();
        }
    }

    /**
     * Stops the thread sending keep alive signals, if running.
     */
    private void stopKeepAliveThread() {
        if (keepAliveThread != null && keepAliveThread.isAlive()) {
            LOG.debug("Stopping keep alive thread");
            keepAliveThread.stopThread();
            keepAliveThread = null;
            this.keepAliveLastSent = 0;
        }
    }

    /**
     * Disconnect from sftp server.
     */
    public synchronized void disconnect() {
        try {
            if (sftpChannel == null) {
                return;
            }
            LOG.debug("Disconnection from sftp server.");
            stopKeepAliveThread();
            Session session = sftpChannel.getSession();
            sftpChannel.disconnect();
            session.disconnect();
            sftpChannel = null;
            keepAliveLastSent = 0;
        } catch (JSchException e) {
            LOG.warn("Error while disconnecting: {}", e.getMessage(), e);
        }
    }

    /**
     * Disconnects and connects again.
     */
    protected synchronized void reconnect() throws SftpClientException {
        disconnect();
        connect();
    }

    /**
     * Stats remote file.
     *
     * @param remoteFileName Name of the remote file
     * @return SftpFile or <code>null</code>, if the file does not exists.
     */
    public SftpFile statRemoteFile(String remoteFileName) throws SftpClientException {
        try {
            if (remoteFileExists(remoteFileName)) {
                SftpATTRS attrs = getConnection().stat(remoteFileName);
                return new SftpFile(host, getParentRemoteFolderName(remoteFileName), FilenameUtils.getName(remoteFileName), attrs);
            } else {
                return null;
            }
        } catch (SftpException e) {
            return handleSftpException(e, "Stat failed: " + remoteFileName);
        }
    }

    /**
     * Lists all remote files at given remote folder.
     *
     * @param remoteFolderName Name of the remote folder.
     * @return List of files (may be empty, but not null)
     */
    public Collection<SftpFile> listRemoteFiles(String remoteFolderName) throws SftpClientException, FileNotFoundException {
        return listRemoteFiles(remoteFolderName, "*");
    }

    /**
     * Lists remote files at remote folder matching given wildcard (like '*.txt').
     *
     * @param remoteFolderName Name of the remote folder.
     * @param givenwildcard    wildcard, e.g. '*.txt', '*', '*.*', 'myfile.txt'. Must not contain the directory separator '/'.
     * @return List of files (may be empty, but not null)
     */
    public Collection<SftpFile> listRemoteFiles(final String remoteFolderName, final String givenwildcard) throws SftpClientException, FileNotFoundException {
        LOG.debug("Listening remote files of '{}' with wildcard '{}'", remoteFolderName, givenwildcard);
        assertRemoteFolderExists(remoteFolderName);
        final String validWildcard = assertValidWildcard(givenwildcard);
        try {
            final ArrayList<SftpFile> files = new ArrayList<>();
            LsEntrySelector selector = new LsEntrySelector() {
                @Override
                public int select(LsEntry entry) {
                    if (FilenameUtils.wildcardMatch(entry.getFilename(), validWildcard)
                            && !("..".equals(entry.getFilename()) || ".".equals(entry.getFilename()))) {
                        files.add(new SftpFile(host, remoteFolderName, entry));
                    }
                    return LsEntrySelector.CONTINUE;
                }
            };
            getConnection().ls(remoteFolderName, selector);
            return Collections.unmodifiableList(files);
        } catch (SftpException e) {
            return handleSftpException(e, "Error listening remote files");
        }
    }

    /**
     * Move remote files (and anything else) to new folder.
     *
     * @param remoteSourceFolder      Remote source folder
     * @param remoteDestinationFolder Remote destination folder.
     * @param wildcard                Wildcard of remote files to move.
     */
    public void moveRemoteFiles(String remoteSourceFolder, String remoteDestinationFolder, String wildcard) throws SftpClientException, FileNotFoundException {
        wildcard = assertValidWildcard(wildcard);
        assertRemoteFolderExists(remoteSourceFolder);
        assertRemoteFolderExists(remoteDestinationFolder, createDirsAutomatically);
        for (SftpFile remoteFile : listRemoteFiles(remoteSourceFolder, wildcard)) {
            renameRemoteFile(remoteDestinationFolder + SFTP_DIRECTORY_SEPARATOR + remoteFile.getFileName(),
                    remoteDestinationFolder + SFTP_DIRECTORY_SEPARATOR + remoteFile.getFileName());
        }
    }

    // ===================================================================================
    // ============================ FILE-OPERATIONS ======================================
    // ===================================================================================

    /**
     * Renames remote file (can also be used to move a single file).
     *
     * @param oldFileName Old file name (full path)
     * @param newFileName New file name (full path)
     */
    public void renameRemoteFile(String oldFileName, String newFileName) throws SftpClientException, FileNotFoundException {
        try {
            assertRemoteFileExists(oldFileName);
            assertRemoteFolderExists(getParentRemoteFolderName(newFileName));
            if (createDirsAutomatically) {
                String parentRemoteFolderName = getParentRemoteFolderName(newFileName);
                if (parentRemoteFolderName != null) {
                    createRemoteFolder(parentRemoteFolderName);
                }
            }
            LOG.debug("Renaming '{}' to '{}'", oldFileName, newFileName);
            getConnection().rename(oldFileName, newFileName);
        } catch (SftpException e) {
            handleSftpException(e, "Rename failed");
        }
    }

    /**
     * Deletes the remote files.
     */
    public void deleteRemoteFiles(String remoteFolderName, String wildcard) throws SftpClientException, FileNotFoundException {
        wildcard = assertValidWildcard(wildcard);
        assertRemoteFolderExists(remoteFolderName);
        LOG.debug("Deleting remote files in '{}' with wildcard '{}'", remoteFolderName, wildcard);
        Collection<SftpFile> remoteFiles = listRemoteFiles(remoteFolderName, wildcard);
        for (SftpFile remoteFile : remoteFiles) {
            if (remoteFile.getType() == SftpFileType.FILE) {
                deleteRemoteFile(remoteFile.getFullFileName());
            }
        }
    }

    /**
     * Deletes the remote file.
     */
    public void deleteRemoteFile(String remoteFileName) throws SftpClientException, FileNotFoundException {
        assertRemoteFileExists(remoteFileName);
        try {
            LOG.debug("Deleting remote file: {}", remoteFileName);
            getConnection().rm(remoteFileName);
        } catch (SftpException e) {
            handleSftpException(e, "Delete failed: " + remoteFileName);
        }
    }

    /**
     * Deletes the remote folder recursively including any content and any sub folders!!!!
     *
     * @param remoteFolderName Name of the remote folder to create.
     */
    public void deleteRemoteFolder(String remoteFolderName) throws SftpClientException, FileNotFoundException {
        assertRemoteFolderExists(remoteFolderName);
        try {
            Collection<SftpFile> listRemoteFiles = listRemoteFiles(remoteFolderName);
            for (SftpFile remoteFile : listRemoteFiles) {
                switch (remoteFile.getType()) {
                    case FOLDER:
                        LOG.debug("Deleting remote folder: {}", remoteFolderName);
                        deleteRemoteFolder(remoteFile.getFullFileName());
                        break;
                    default:
                        deleteRemoteFile(remoteFile.getFullFileName());
                }
            }
            getConnection().rmdir(remoteFolderName);
        } catch (SftpException e) {
            handleSftpException(e, "Delete failed: " + remoteFolderName);
        }
    }

    /**
     * Creates remote folder (recursively).
     *
     * @param remoteFolderName Name of the remote folder to create.
     */
    public void createRemoteFolder(String remoteFolderName) throws SftpClientException {
        try {
            Stack<String> hierarchicalFolderNames = new Stack<String>();
            String parentFolderName = remoteFolderName;
            while (!remoteFileExists(parentFolderName) && parentFolderName != null) {
                hierarchicalFolderNames.push(parentFolderName);
                parentFolderName = getParentRemoteFolderName(parentFolderName);
            }
            while (!hierarchicalFolderNames.isEmpty()) {
                String remoteFolderNamePart = hierarchicalFolderNames.pop();
                LOG.debug("Creating remote folder: {}", remoteFolderNamePart);
                getConnection().mkdir(remoteFolderNamePart);
            }
        } catch (SftpException e) {
            handleSftpException(e, "Creating remote folder failed: " + remoteFolderName);
        }
    }

    /**
     * Gets parent remote file name or <code>null</code>, if there is none (root folder).
     */
    protected String getParentRemoteFolderName(String remoteFileName) {
        int lofSlash = remoteFileName.lastIndexOf('/');
        if (lofSlash < 0) {
            return null;
        }
        return remoteFileName.substring(0, lofSlash);
    }

    /**
     * Returns <code>true</code>, if the remote file already exists.
     */
    protected boolean remoteFileExists(String remoteFileName) throws SftpClientException {
        try {
            return getConnection().stat(remoteFileName) != null;
        } catch (SftpException e) {
            return false;
        }
    }

    /**
     * Upload files to sftp server.
     *
     * @param remoteFolderName Remote folder to put local files to (remote files will become the same name as the local files).
     * @param localFiles       List of local files.
     * @throws SftpClientException        In case of any sftp exception
     * @throws FileAlreadyExistsException If the remote file already exists and overwriting is disabled.
     */
    public void uploadFiles(String remoteFolderName, File... localFiles) throws SftpClientException, FileNotFoundException {
        assertRemoteFolderExists(remoteFolderName, createDirsAutomatically);
        assertLocalFilesExists(localFiles);
        if (createDirsAutomatically) {
            createRemoteFolder(remoteFolderName);
        }
        for (File localFile : localFiles) {
            uploadFile(remoteFolderName + SFTP_DIRECTORY_SEPARATOR + localFile.getName(), localFile);
        }
    }

    /**
     * Upload file to sftp server.
     *
     * @param remoteFileName Remote file name.
     * @param localFile      Local file.
     * @throws SftpClientException        In case of any sftp exception
     * @throws FileAlreadyExistsException If the remote file already exists and overwriting is disabled.
     */
    public void uploadFile(String remoteFileName, File localFile) throws SftpClientException, FileNotFoundException {
        assertLocalFileExists(localFile);
        FileInputStream fis = null;
        try {
            LOG.info("Uploading file '{}' to '{}'", localFile, remoteFileName);
            if (localFile.isFile()) {
                fis = new FileInputStream(localFile);
                uploadFileInternal(remoteFileName, fis);
            } else {
                throw new SftpClientException(localFile + " is not a valid local file. (may be a folder?)");
            }
        } catch (IOException e) {
            handleSftpException(e, "Upload failed: " + localFile);
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }

    /**
     * Upload data of input stream to sftp server.
     *
     * @param remoteFileName   Remote file name.
     * @param localInputStream Input stream uploaded to
     * @throws SftpClientException        In case of any sftp exception
     * @throws FileAlreadyExistsException If the remote file already exists and overwriting is disabled.
     */
    public void uploadFile(String remoteFileName, InputStream localInputStream) throws SftpClientException, FileNotFoundException {
        LOG.info("Uploading input stream '{}' to '{}'", localInputStream.toString(), remoteFileName);
        uploadFileInternal(remoteFileName, localInputStream);
    }

    /**
     * Upload data to sftp server.
     * <p>
     * <b>Use this method only for a small amount of data!!!</b>
     *
     * @param remoteFileName Remote file name.
     * @param data           Byte array to upload
     * @throws SftpClientException        In case of any sftp exception
     * @throws FileAlreadyExistsException If the remote file already exists and overwriting is disabled.
     */
    public void uploadData(String remoteFileName, byte[] data) throws SftpClientException, FileNotFoundException {
        ByteArrayInputStream bais = null;
        try {
            LOG.info("Uploading data to '{}'", remoteFileName);
            bais = new ByteArrayInputStream(data);
            uploadFileInternal(remoteFileName, bais);
        } finally {
            IOUtils.closeQuietly(bais);
        }
    }

    // ===================================================================================
    // ========================== UPLOAD / DOWNLOAD ======================================
    // ===================================================================================

    /**
     * Internal upload local stream -> remote file.
     */
    protected void uploadFileInternal(String remoteFileName, InputStream localInputStream)
            throws SftpClientException, FileAlreadyExistsException, FileNotFoundException {
        String temporaryFileName = null;
        assertRemoteFolderExists(getParentRemoteFolderName(remoteFileName), createDirsAutomatically);
        try {
            remoteFileName = handlePossibleOverwrite(remoteFileName, FILEEXISTSCALL_REMOTE);
            if (transactional) {
                temporaryFileName = FilenameUtils.getFullPath(remoteFileName) + "." + RandomStringUtils.randomAlphanumeric(25);
                LOG.debug("Uploading to temporary file: {}", temporaryFileName);
                getConnection().put(localInputStream, temporaryFileName);
                // While uploading the file another process may be written the same file, so it may now exists
                remoteFileName = handlePossibleOverwrite(remoteFileName, FILEEXISTSCALL_REMOTE);
                renameRemoteFile(temporaryFileName, remoteFileName);
            } else {
                getConnection().put(localInputStream, remoteFileName);
            }
        } catch (SftpException e) {
            if (temporaryFileName != null) {
                deleteRemoteFile(temporaryFileName);
            }
            handleSftpException(e, "Upload failed");
        }
    }

    /**
     * Downloads remote file to output stream.
     *
     * @param remoteFileName    Name of the remote file.
     * @param localOutputStream Local output stream.
     */
    public void downloadFile(String remoteFileName, OutputStream localOutputStream) throws SftpClientException, FileNotFoundException {
        try {
            LOG.info("Downloading remote file '{}' to local stream '{}'", remoteFileName, localOutputStream);
            assertRemoteFileExists(remoteFileName);
            getConnection().get(remoteFileName, localOutputStream);
        } catch (SftpException e) {
            handleSftpException(e, "Download failed: " + remoteFileName);
        }
    }

    /**
     * Downloads remote file to byte array.
     * <p>
     * <b>Use this method only for small files!!!</b>
     *
     * @param remoteFileName Name of the remote file.
     */
    public byte[] downloadFile(String remoteFileName) throws SftpClientException, FileNotFoundException {
        ByteArrayOutputStream baos = null;
        try {
            LOG.info("Downloading remote file '{}'", remoteFileName);
            assertRemoteFileExists(remoteFileName);
            baos = new ByteArrayOutputStream();
            downloadFile(remoteFileName, baos);
            return baos.toByteArray();
        } finally {
            IOUtils.closeQuietly(baos);
        }
    }

    /**
     * Downloads all files (and only files) of the given remote folder (non-recursive) to the given local folder.
     *
     * @param remoteFolderName Name of the remote folder
     * @param localFolderName  Name of the local folder.
     */
    public void downloadFiles(String remoteFolderName, String localFolderName) throws SftpClientException, FileNotFoundException {
        downloadFiles(remoteFolderName, localFolderName, "*");
    }

    /**
     * Downloads all files (and only files) of the given remote folder (non-recursive) matching the given wildcard to the given local folder.
     *
     * @param remoteFolderName Name of the remote folder
     * @param localFolderName  Name of the local folder.
     * @param wildCard         Wildcard to match files
     */
    public void downloadFiles(String remoteFolderName, String localFolderName, String wildCard) throws SftpClientException, FileNotFoundException {
        try {
            LOG.info("Downloading remote files from '{}' to '{}' with wildcard '{}", remoteFolderName, wildCard, localFolderName);
            assertRemoteFolderExists(remoteFolderName);
            assertLocalFolderExists(new File(localFolderName), createDirsAutomatically);
            assertValidWildcard(wildCard);
            Collection<SftpFile> remoteFiles = listRemoteFiles(remoteFolderName, wildCard);
            for (SftpFile remoteFile : remoteFiles) {
                if (remoteFile.getType() == SftpFileType.FILE) {
                    downloadFileInternal(remoteFolderName + SFTP_DIRECTORY_SEPARATOR + remoteFile.getFileName(),
                            localFolderName + IOUtils.DIR_SEPARATOR + remoteFile.getFileName());
                }
            }
        } catch (SftpException e) {
            handleSftpException(e, "Download failed: " + remoteFolderName);
        }
    }

    /**
     * Download remote file -> local file.
     */
    protected void downloadFileInternal(String remoteFileName, String localFileName) throws SftpException, SftpClientException, FileNotFoundException {
        LOG.debug("Downloading file '{}' to '{}'", remoteFileName, localFileName);
        assertLocalFolderExists(new File(localFileName).getParentFile(), createDirsAutomatically);
        String temporaryFileName = null;
        try {
            localFileName = handlePossibleOverwrite(localFileName, FILEEXISTSCALL_LOCAL);
            if (transactional) {
                temporaryFileName = FilenameUtils.getFullPath(localFileName) + "." + RandomStringUtils.randomAlphanumeric(25);
                getConnection().get(remoteFileName, temporaryFileName);
                // While downloading the file another process may be written the same file, so it may now exists
                localFileName = handlePossibleOverwrite(localFileName, FILEEXISTSCALL_LOCAL);
                new File(temporaryFileName).renameTo(new File(localFileName));
            } else {
                getConnection().get(remoteFileName, localFileName);
            }
        } finally {
            FileUtils.deleteQuietly(temporaryFileName != null ? new File(temporaryFileName) : null);
        }
    }

    /**
     * Handles overwriting of file, if relevant.
     */
    protected String handlePossibleOverwrite(String fileName, FileExistsCall feh) throws FileAlreadyExistsException, SftpClientException {
        if (feh.fileExists(fileName)) {
            switch (overwriteMode) {
                case ALWAYS:
                    return fileName;
                case NEVER:
                    throw new FileAlreadyExistsException("File already exists: " + fileName);
                case ADD_COUNTING_SUFFIX_AFTER_EXISTING_SUFFIX:
                case ADD_COUNTING_SUFFIX_BEFORE_EXISTING_SUFFIX:
                    int suffixCounter = 1;
                    String fileNameBase = FilenameUtils.getFullPath(fileName) + FilenameUtils.getBaseName(fileName);
                    String fileNameSuffix = FilenameUtils.getExtension(fileName);
                    fileNameSuffix = StringUtils.isEmpty(fileNameSuffix) ? "" : "." + fileNameSuffix;
                    String newFileName = null;
                    do {
                        String incrementingSuffix = "." + suffixCounter++;
                        if (overwriteMode == OverwriteMode.ADD_COUNTING_SUFFIX_AFTER_EXISTING_SUFFIX) {
                            newFileName = fileNameBase + fileNameSuffix + incrementingSuffix;
                        } else if (overwriteMode == OverwriteMode.ADD_COUNTING_SUFFIX_BEFORE_EXISTING_SUFFIX) {
                            newFileName = fileNameBase + incrementingSuffix + fileNameSuffix;
                        } else {
                            throw new IllegalStateException("Illegal overwrite mode here.");
                        }
                    } while (feh.fileExists(newFileName));
                    return newFileName;
            }
        }
        return fileName;
    }

    /**
     * In strict mode, check for remote file.
     */
    protected void assertRemoteFileExists(String remoteFileName) throws SftpClientException, FileNotFoundException {
        if (strictMode) {
            SftpFile remoteFile = statRemoteFile(remoteFileName);
            if (remoteFile == null || remoteFile.getType() != SftpFileType.FILE) {
                throw new FileNotFoundException("Remote file not found or not a valid file: " + remoteFileName);
            }
        }
    }

    /**
     * In strict mode, check for remote folder.
     */
    protected void assertRemoteFolderExists(String remoteFolderName) throws SftpClientException, FileNotFoundException {
        if (strictMode) {
            assertRemoteFolderExists(remoteFolderName, false);
        }
    }

    /**
     * In strict mode, check for remote folder. Create it, if createDir is <code>true</code> (independet from strict mode)
     */
    protected void assertRemoteFolderExists(String remoteFolderName, boolean createDir) throws SftpClientException, FileNotFoundException {
        if (createDir || strictMode) {
            SftpFile remoteFolder = statRemoteFile(remoteFolderName);
            if (remoteFolder == null && createDir) {
                createRemoteFolder(remoteFolderName);
            }
            if (strictMode && remoteFolder != null && remoteFolder.getType() != SftpFileType.FOLDER) {
                throw new FileNotFoundException("Remote folder not found or not a valid folder: " + remoteFolderName);
            }
        }
    }

    /**
     * Check for valid wildcard.
     */
    protected String assertValidWildcard(final String givenWildcard) throws SftpClientException {
        final String validWildcard;
        if (StringUtils.isEmpty(givenWildcard)) {
            validWildcard = "*";
        } else {
            validWildcard = givenWildcard;
        }
        if (validWildcard.contains(SFTP_DIRECTORY_SEPARATOR)) {
            throw new SftpClientException("Invalid wildcard: " + validWildcard);
        }
        return validWildcard;
    }

    // ===================================================================================
    // ============================== ASSERTS ============================================
    // ===================================================================================

    /**
     * In strict mode, check for local files.
     */
    protected void assertLocalFilesExists(File... localFiles) throws FileNotFoundException {
        if (strictMode) {
            for (File localFile : localFiles) {
                assertLocalFileExists(localFile);
            }
        }
    }

    /**
     * In strict mode, check for local file.
     */
    protected void assertLocalFileExists(File localFile) throws FileNotFoundException {
        if (strictMode && !localFile.canRead()) {
            throw new FileNotFoundException("Cannot read local file: " + localFile);
        }
    }

    /**
     * In strict mode, check for local folder. Create it, if createDir is <code>true</code> (independet from strict mode)
     */
    protected void assertLocalFolderExists(File localFolder, boolean createDirs) throws FileNotFoundException {
        if (strictMode || createDirs) {
            if (!localFolder.isDirectory()) {
                if (createDirs) {
                    localFolder.mkdirs();
                }
            }
        }
        if (strictMode && !createDirs) {
            throw new FileNotFoundException("Cannot found local folder: " + localFolder);
        }
    }

    /**
     * Type of host keys. Only SSH_RSA is currently supported (by underlying jsch).
     */
    public enum HostKeyType {
        SSH_RSA
    }

    /**
     * Mode of overwrite. Controls how to handle requests to overwrite existing files.
     */
    public enum OverwriteMode {
        /**
         * Disables overwriting of files at all. An {@link FileAlreadyExistsException} is thrown on requesting to overwrite an existing file.
         */
        NEVER,

        /**
         * Enables adding a numeric suffix, if a file already exists, like 'myfile.txt', 'myfiles.txt.0', 'myfiles.txt.1', ...
         */
        ADD_COUNTING_SUFFIX_AFTER_EXISTING_SUFFIX,

        /**
         * Enables adding a numeric suffix, if a file already exists, like 'myfile.txt', 'myfiles.0.txt', 'myfiles.1.txt', ...
         */
        ADD_COUNTING_SUFFIX_BEFORE_EXISTING_SUFFIX,

        /**
         * Enables overwriting of files without any warning..
         */
        ALWAYS
    }

    /**
     * Used as a callback method for handlePossibleOverwrite .
     */
    private interface FileExistsCall {
        boolean fileExists(String fileName) throws SftpClientException;
    }

    /**
     * Thread type used to keep the session alive.
     */
    private final class KeepAliveThread extends Thread {
        private boolean runIt = true;

        private void stopThread() {
            runIt = false;
            setName("Thread-" + getId());
        }

        @Override
        public void run() {
            setName(String.format("sftp-keepalive (%s@%s:%d)", user, host, port));
            while (runIt) {
                try {
                    Thread.sleep(keepAliveIntervall + 1);
                    if (!sendKeepAlive()) {
                        LOG.warn("Connection lost. Stopping keep alive thread.");
                        stopThread();
                    }

                } catch (InterruptedException e) {
                    LOG.warn("Interrupted.");
                    stopThread();
                }
            }
        }
    }

}
