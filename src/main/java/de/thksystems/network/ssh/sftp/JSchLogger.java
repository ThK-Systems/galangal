/*
 * tksCommons / galangal
 * 
 * Author : Thomas Kuhlmann (ThK-Systems, http://www.thk-systems.de) 
 * License : LGPL (https://www.gnu.org/licenses/lgpl.html)
 */
package de.thksystems.network.ssh.sftp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JSchLogger implements com.jcraft.jsch.Logger {

	static final Logger LOG = LoggerFactory.getLogger(JSchLogger.class);

	@Override
	public void log(int level, String message) {
		switch (level) {
			case com.jcraft.jsch.Logger.DEBUG:
				LOG.debug(message);
				break;
			case com.jcraft.jsch.Logger.INFO:
				LOG.info(message);
				break;
			case com.jcraft.jsch.Logger.WARN:
				LOG.warn(message);
				break;
			case com.jcraft.jsch.Logger.ERROR:
				LOG.error(message);
				break;
			case com.jcraft.jsch.Logger.FATAL:
				LOG.error("FATAL: " + message);
				break;
		}
	}

	@Override
	public boolean isEnabled(int level) {
		switch (level) {
			case com.jcraft.jsch.Logger.DEBUG:
				return LOG.isDebugEnabled();
			case com.jcraft.jsch.Logger.INFO:
				return LOG.isInfoEnabled();
			case com.jcraft.jsch.Logger.WARN:
				return LOG.isWarnEnabled();
			case com.jcraft.jsch.Logger.ERROR:
				return LOG.isErrorEnabled();
			case com.jcraft.jsch.Logger.FATAL:
				return LOG.isErrorEnabled();
			default:
				return false;
		}
	}
}