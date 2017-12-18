/*
 * tksCommons / galangal
 * 
 * Author : Thomas Kuhlmann (ThK-Systems, http://www.thk-systems.de)
 * License : LGPL (https://www.gnu.org/licenses/lgpl.html)
 */
package de.thksystems.network.ssh.sftp;

import java.io.IOException;

public class SftpClientException extends IOException {
	private static final long serialVersionUID = -2388085885439893510L;

	public SftpClientException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public SftpClientException(final String message) {
		super(message);
	}

	public SftpClientException(final Throwable cause) {
		super(cause);
	}

}
