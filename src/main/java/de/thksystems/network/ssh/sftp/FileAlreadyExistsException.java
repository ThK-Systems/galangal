/*
 * tksCommons / galangal
 *
 * Author : Thomas Kuhlmann (ThK-Systems, http://www.thk-systems.de)
 * License : LGPL (https://www.gnu.org/licenses/lgpl.html)
 */
package de.thksystems.network.ssh.sftp;

public class FileAlreadyExistsException extends SftpClientException {

    private static final long serialVersionUID = -5527817126472395858L;

    public FileAlreadyExistsException(String msg) {
        super(msg);
    }

}
