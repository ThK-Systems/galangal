/*
 * tksCommons / galangal
 * 
 * Author : Thomas Kuhlmann (ThK-Systems, http://www.thk-systems.de) 
 * License : LGPL (https://www.gnu.org/licenses/lgpl.html)
 */
package de.thksystems.network.ssh.sftp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.junit.Test;

import de.thksystems.network.ssh.sftp.SftpClient.HostKeyType;
import de.thksystems.network.ssh.sftp.SftpClient.OverwriteMode;
import de.thksystems.util.text.RandomStringUtils;

public class SftpClientTest {

	// Private key auth
	@Test
	public void testWithPrivateKeyAuth() throws Exception, IOException {
		File privateKeyFile = new File(getClass().getClassLoader().getResource("testfiles/private_key").getFile());
		SftpClient sftpClient = new SftpClient("hamenthotep", "cumin", privateKeyFile, "c8m1.j4-L4").withDisableHostkeyCheck(true);
		sftpClient.listRemoteFiles("/tmp");
	}

	// Password auth, upload (overwrite always), download, delete, list, hostkey check off
	@Test
	public void testWithUserPassAuthUploadAndDownload() throws Exception, IOException {
		SftpClient sftpClient = new SftpClient("hamenthotep", "cumin", "c8M1.n4-L3").withDisableHostkeyCheck(true);

		// Upload single file
		URL resource = getClass().getClassLoader().getResource("testfiles/ssh_upload_test.txt");
		File localFile = new File(resource.getFile());
		sftpClient.uploadFile("/home/cumin/test_file.txt", localFile);

		// Upload multiple files
		URL res1 = getClass().getClassLoader().getResource("testfiles/ssh_upload_test.txt");
		File localFile1 = new File(res1.getFile());
		URL res2 = getClass().getClassLoader().getResource("testfiles/ssh_upload_test2.txt");
		File localFile2 = new File(res2.getFile());
		sftpClient.uploadFiles("/home/cumin", localFile1, localFile2);

		// Upload from stream
		InputStream filestream = getClass().getClassLoader().getResourceAsStream("testfiles/ssh_upload_test.txt");
		sftpClient.uploadFile("/home/cumin/test_as_stream.txt", filestream);
		filestream.close();

		// Upload and overwrite
		sftpClient.withOverwriteMode(OverwriteMode.ALWAYS);
		sftpClient.uploadFile("/home/cumin/test_file.txt", localFile);

		// List remote files
		Collection<SftpFile> remoteFiles = sftpClient.listRemoteFiles("/home/cumin/", "*.txt");
		assertNotNull(remoteFiles);
		assertEquals(4, remoteFiles.size());

		// Download single file to output stream
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		sftpClient.downloadFile("/home/cumin/test_file.txt", baos);
		assertNotNull(baos.toString());
		String localFileContent = FileUtils.readFileToString(localFile, Charset.defaultCharset());
		assertEquals(localFileContent, baos.toString());

		// Download multiple files to temp directory
		File localTempDir = Files.createTempDirectory("cumin-test", new FileAttribute<?>[0]).toFile();
		sftpClient.withCreateDirsAutomatically(true);
		sftpClient.downloadFiles("/home/cumin", localTempDir.getAbsolutePath(), "*.txt");
		String[] localTempFileNames = localTempDir.list();
		for (String localTempFileName : localTempFileNames) {
			File localTempFile = new File(localTempDir, localTempFileName);
			assertEquals(localFile.length(), localTempFile.length());
		}
		assertEquals(4, localTempFileNames.length);
		FileUtils.deleteDirectory(localTempDir);

		// Delete all files
		sftpClient.withStrictMode(false);
		sftpClient.deleteRemoteFile("/home/cumin/*.txt");

		// Disconnect
		sftpClient.disconnect();
	}

	// Upload: overwrite never
	@Test
	public void testOverwriteNever() throws Exception {
		SftpClient sftpClient = new SftpClient("hamenthotep", "cumin", "c8M1.n4-L3").withHostKey(
				"AAAAB3NzaC1yc2EAAAADAQABAAABAQDSLeI+NWe+tsS/N/mm7+K3apAE2+fA7nwTY+WFTYRx7/yjM86Ajij16dGngm3PXViUkyOH3NYO2ECcC303PVZdoop8Eaxr9i1F/x/no55ZWwxHtyWARgVsJxt0mEcdjyaRm2k7qYJEaPxqJ6K3ZUCuSCmYzaWNo9D3JahUIjlAXaaVraejtgjNaSBPL5y7aVRMTJr4h5kyLFalTj61CmMSwZHbv0j4x5mI8mXmGsIZsH/pO8ONmaNjKtbzuyO6AJ0aYOBUyfSUwgtBTmZKfudOL+3l8djbGJwE8wfYqReTmR/UxZ7je3CqLX++mV5FXXSFXyrHbBtHmFk5noWu2KE/",
				HostKeyType.SSH_RSA).withOverwriteMode(OverwriteMode.NEVER).withStrictMode(true);

		File localFile = new File(getClass().getClassLoader().getResource("testfiles/ssh_upload_test.txt").getFile());
		sftpClient.uploadFile("/home/cumin/test_file.txt", localFile);
		try {
			sftpClient.uploadFile("/home/cumin/test_file.txt", localFile);
			fail();
		} catch (FileAlreadyExistsException e) {

		}

		sftpClient.deleteRemoteFile("/home/cumin/*.txt");
		sftpClient.disconnect();
	}

	// Upload: overwrite counter before suffix
	@Test
	public void testOverwriteCounterBeforeSuffix() throws Exception {
		SftpClient sftpClient = new SftpClient("hamenthotep", "cumin", "c8M1.n4-L3").withHostKey(
				"AAAAB3NzaC1yc2EAAAADAQABAAABAQDSLeI+NWe+tsS/N/mm7+K3apAE2+fA7nwTY+WFTYRx7/yjM86Ajij16dGngm3PXViUkyOH3NYO2ECcC303PVZdoop8Eaxr9i1F/x/no55ZWwxHtyWARgVsJxt0mEcdjyaRm2k7qYJEaPxqJ6K3ZUCuSCmYzaWNo9D3JahUIjlAXaaVraejtgjNaSBPL5y7aVRMTJr4h5kyLFalTj61CmMSwZHbv0j4x5mI8mXmGsIZsH/pO8ONmaNjKtbzuyO6AJ0aYOBUyfSUwgtBTmZKfudOL+3l8djbGJwE8wfYqReTmR/UxZ7je3CqLX++mV5FXXSFXyrHbBtHmFk5noWu2KE/",
				HostKeyType.SSH_RSA).withOverwriteMode(OverwriteMode.ADD_COUNTING_SUFFIX_BEFORE_EXISTING_SUFFIX);

		File localFile = new File(getClass().getClassLoader().getResource("testfiles/ssh_upload_test.txt").getFile());
		for (int i = 0; i < 27; i++) {
			sftpClient.uploadFile("/home/cumin/test_file.txt", localFile);
		}

		assertTrue(sftpClient.remoteFileExists("/home/cumin/" + "test_file.txt"));
		for (int i = 1; i < 26; i++) {
			assertTrue(sftpClient.remoteFileExists("/home/cumin/" + "test_file." + i + ".txt"));
		}

		Collection<SftpFile> remoteFiles = sftpClient.listRemoteFiles("/home/cumin/", "*.txt");
		assertEquals(27, remoteFiles.size());

		sftpClient.deleteRemoteFiles("/home/cumin/", "*.txt");
		sftpClient.disconnect();
	}

	// Upload: overwrite counter after suffix
	@Test
	public void testOverwriteCounterAfterSuffix() throws Exception {
		SftpClient sftpClient = new SftpClient("hamenthotep", "cumin", "c8M1.n4-L3").withHostKey(
				"AAAAB3NzaC1yc2EAAAADAQABAAABAQDSLeI+NWe+tsS/N/mm7+K3apAE2+fA7nwTY+WFTYRx7/yjM86Ajij16dGngm3PXViUkyOH3NYO2ECcC303PVZdoop8Eaxr9i1F/x/no55ZWwxHtyWARgVsJxt0mEcdjyaRm2k7qYJEaPxqJ6K3ZUCuSCmYzaWNo9D3JahUIjlAXaaVraejtgjNaSBPL5y7aVRMTJr4h5kyLFalTj61CmMSwZHbv0j4x5mI8mXmGsIZsH/pO8ONmaNjKtbzuyO6AJ0aYOBUyfSUwgtBTmZKfudOL+3l8djbGJwE8wfYqReTmR/UxZ7je3CqLX++mV5FXXSFXyrHbBtHmFk5noWu2KE/",
				HostKeyType.SSH_RSA).withOverwriteMode(OverwriteMode.ADD_COUNTING_SUFFIX_AFTER_EXISTING_SUFFIX);

		File localFile = new File(getClass().getClassLoader().getResource("testfiles/ssh_upload_test.txt").getFile());
		for (int i = 0; i < 12; i++) {
			sftpClient.uploadFile("/home/cumin/test_file.txt", localFile);
		}

		assertTrue(sftpClient.remoteFileExists("/home/cumin/" + "test_file.txt"));
		for (int i = 1; i < 12; i++) {
			assertTrue(sftpClient.remoteFileExists("/home/cumin/" + "test_file.txt." + i));
		}

		Collection<SftpFile> remoteFiles = sftpClient.listRemoteFiles("/home/cumin/", "*.txt*");
		assertEquals(12, remoteFiles.size());

		sftpClient.deleteRemoteFiles("/home/cumin/", "*.txt*");
		sftpClient.disconnect();
	}

	// Explicit host key
	@Test
	public void testWithHostKey() throws Exception, IOException {
		SftpClient sftpClient = new SftpClient("hamenthotep", "cumin", "c8M1.n4-L3").withHostKey(
				"AAAAB3NzaC1yc2EAAAADAQABAAABAQDSLeI+NWe+tsS/N/mm7+K3apAE2+fA7nwTY+WFTYRx7/yjM86Ajij16dGngm3PXViUkyOH3NYO2ECcC303PVZdoop8Eaxr9i1F/x/no55ZWwxHtyWARgVsJxt0mEcdjyaRm2k7qYJEaPxqJ6K3ZUCuSCmYzaWNo9D3JahUIjlAXaaVraejtgjNaSBPL5y7aVRMTJr4h5kyLFalTj61CmMSwZHbv0j4x5mI8mXmGsIZsH/pO8ONmaNjKtbzuyO6AJ0aYOBUyfSUwgtBTmZKfudOL+3l8djbGJwE8wfYqReTmR/UxZ7je3CqLX++mV5FXXSFXyrHbBtHmFk5noWu2KE/",
				HostKeyType.SSH_RSA);
		sftpClient.listRemoteFiles("/usr/lib");
	}

	// Know host file
	@Test
	public void testWithKnownHostFile() throws Exception, IOException {
		File knownHostsFile = new File(getClass().getClassLoader().getResource("testfiles/known_hosts").getFile());
		SftpClient sftpClient = new SftpClient("hamenthotep", "cumin", "c8M1.n4-L3").withKnownHostsFile(knownHostsFile);
		sftpClient.listRemoteFiles("/var/run");
	}

	// No host key, no knownhost file -> Fail
	@Test
	public void testHostKeyCheckFailed() throws Exception, IOException {
		File privateKeyFile = new File(getClass().getClassLoader().getResource("testfiles/private_key").getFile());
		SftpClient sftpClient = new SftpClient("hamenthotep", "cumin", privateKeyFile, "c8m1.j4-L4");
		try {
			sftpClient.connect();
		} catch (SftpClientException e) {
			assertTrue(e.getMessage().contains("UnknownHostKey"));
		}
	}

	@Test
	public void testCreateRemoteFolder() throws Exception {
		SftpClient sftpClient = new SftpClient("hamenthotep", "cumin", "c8M1.n4-L3").withDisableHostkeyCheck(true);
		String remoteFolderName = "/home/cumin/" + RandomStringUtils.randomAlphanumeric(10) + "/test";
		sftpClient.createRemoteFolder(remoteFolderName);

		File localFile = new File(getClass().getClassLoader().getResource("testfiles/ssh_upload_test.txt").getFile());
		sftpClient.uploadFile(remoteFolderName + "/" + "test_file.txt", localFile);

		sftpClient.deleteRemoteFolder(sftpClient.getParentRemoteFolderName(remoteFolderName));
		assertFalse(sftpClient.remoteFileExists(sftpClient.getParentRemoteFolderName(remoteFolderName)));
	}

	@Test
	@Ignore
	public void testUploadMkdirsAndDownloadRemoteFolder() throws Exception, IOException {
		SftpClient sftpClient = new SftpClient("hamenthotep", "cumin", "c8M1.n4-L3").withDisableHostkeyCheck(true).withCreateDirsAutomatically(true);
		String remoteFileName = "/home/cumin/" + RandomStringUtils.randomAlphanumeric(10) + "/test" + "/" + "test_file.txt";

		File localFile = new File(getClass().getClassLoader().getResource("testfiles/ssh_upload_test.txt").getFile());
		sftpClient.uploadFile(remoteFileName, localFile);

		File localTempDir = Files.createTempDirectory("cumin-test", new FileAttribute<?>[0]).toFile();
		String parentParent = sftpClient.getParentRemoteFolderName(sftpClient.getParentRemoteFolderName(remoteFileName));
		sftpClient.downloadFiles(parentParent, localTempDir.getAbsolutePath());
	}

	// Upload: folder
	@Test
	@Ignore
	public void testUploadFolder() throws Exception, FileNotFoundException {
		SftpClient sftpClient = new SftpClient("hamenthotep", "cumin", "c8M1.n4-L3").withHostKey(
				"AAAAB3NzaC1yc2EAAAADAQABAAABAQDSLeI+NWe+tsS/N/mm7+K3apAE2+fA7nwTY+WFTYRx7/yjM86Ajij16dGngm3PXViUkyOH3NYO2ECcC303PVZdoop8Eaxr9i1F/x/no55ZWwxHtyWARgVsJxt0mEcdjyaRm2k7qYJEaPxqJ6K3ZUCuSCmYzaWNo9D3JahUIjlAXaaVraejtgjNaSBPL5y7aVRMTJr4h5kyLFalTj61CmMSwZHbv0j4x5mI8mXmGsIZsH/pO8ONmaNjKtbzuyO6AJ0aYOBUyfSUwgtBTmZKfudOL+3l8djbGJwE8wfYqReTmR/UxZ7je3CqLX++mV5FXXSFXyrHbBtHmFk5noWu2KE/",
				HostKeyType.SSH_RSA).withOverwriteMode(OverwriteMode.NEVER);

		File localDir = new File(getClass().getClassLoader().getResource("testfiles").getFile());
		sftpClient.uploadFiles("/home/cumin/", localDir);

		sftpClient.deleteRemoteFolder("/home/cumin/testfiles");
		sftpClient.disconnect();
	}

	// Keep alive, on and off
	@Test
	@Ignore
	public void testKeepAlive() throws Exception, InterruptedException {
		SftpClient sftpClient = new SftpClient("hamenthotep", "cumin", "c8M1.n4-L3").withDisableHostkeyCheck(true);

		int waitTime = 300 * 1000;

		// Disabling keep alive
		sftpClient.withNoKeepAlive().connect();
		Thread.sleep(waitTime);
		assertFalse(sftpClient.sendKeepAlive());
		sftpClient.disconnect();

		// Enabling keep alive
		sftpClient.withActiveKeepAlive(10 * 1000);
		sftpClient.connect();
		Thread.sleep(waitTime);
		assertTrue(sftpClient.sendKeepAlive());
		sftpClient.disconnect();
	}

}
