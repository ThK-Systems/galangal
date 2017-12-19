/*
 * tksCommons / galangal
 *
 * Author : Thomas Kuhlmann (ThK-Systems, http://www.thk-systems.de)
 * License : LGPL (https://www.gnu.org/licenses/lgpl.html)
 */
package de.thksystems.network.ssh.sftp;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.SftpATTRS;

/**
 * Represents a remote sftp file.
 */
public class SftpFile {

    private final String host;
    private final String path;
    private final String fileName;
    private Long size;
    private SftpFileType type;

    SftpFile(String host, String remotePath, LsEntry lsEntry) {
        super();
        this.host = host;
        this.path = remotePath;
        this.fileName = lsEntry.getFilename();
        readAttrs(lsEntry.getAttrs());
    }

    SftpFile(String host, String remotePath, String remoteFileName, SftpATTRS attrs) {
        this.host = host;
        this.path = remotePath;
        this.fileName = remoteFileName;
        readAttrs(attrs);
    }

    private void readAttrs(SftpATTRS attrs) {
        this.size = attrs != null ? attrs.getSize() : null;

        if (attrs != null && attrs.isDir()) {
            type = SftpFileType.FOLDER;
        } else if (attrs != null && attrs.isLink()) {
            type = SftpFileType.LINK;
        } else if (attrs != null && attrs.isReg()) {
            type = SftpFileType.FILE;
        } else if (attrs != null) {
            type = SftpFileType.SPECIAL;
        }
    }

    /**
     * Gets host of file.
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets filename (without path).
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * Gets path (without filename).
     */
    public String getPath() {
        return path;
    }

    /**
     * Gets path and filename.
     */
    public String getFullFileName() {
        return path + SftpClient.SFTP_DIRECTORY_SEPARATOR + fileName;
    }

    /**
     * Gets size.
     */
    public Long getSize() {
        return size;
    }

    /**
     * Gets type.
     */
    public SftpFileType getType() {
        return type;
    }

    /**
     * Returns <code>true</code>, if it is a file.
     */
    public boolean isFile() {
        return type == SftpFileType.FILE;
    }

    /**
     * Returns <code>true</code>, if it is a folder.
     */
    public boolean isFolder() {
        return type == SftpFileType.FOLDER;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    public enum SftpFileType {
        FILE, FOLDER, LINK, SPECIAL
    }

}
