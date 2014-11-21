package util;

import worker.Worker;
import worker.WorkerInfo;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * This class takes care of transferring a file from one worker to another. This
 * happens when a mapper does not have the file it should work on or a reducer
 * asks a mapper for intermediate file
 * 
 * @author yuruiz on 11/8/14.
 */
public class FileTransmission extends Thread {

	private String filename;
	private OutputStream outputStream;

	public FileTransmission(String filename, OutputStream outputStream) {
		this.filename = filename;
		this.outputStream = outputStream;
	}

	/* Convert int to binary */
	public static byte[] inttobyte(int myInteger) {
		return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
				.putInt(myInteger).array();
	}

	/* Convert binay to int */
	public static int bytetoint(byte[] byteBarray) {
		return ByteBuffer.wrap(byteBarray).order(ByteOrder.LITTLE_ENDIAN)
				.getInt();
	}

	/* Ask specific files from others nodes */
	public static void askforfile(String filename, List<WorkerInfo> infos,
			Worker worker) throws RuntimeException {

		for (int i = 0; i < infos.size(); i++) {
			try {
				WorkerInfo info = infos.get(i);
				Socket socket = new Socket(info.getIpAddress(), info.getPort());
				InputStream inputStream = socket.getInputStream();
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(
						socket.getOutputStream());
				Message mesg = new Message();

				mesg.setType(Message.MessageType.FILE_REQ);
				mesg.setFetcheFilename(filename);
				objectOutputStream.writeObject(mesg);

				byte[] len = new byte[4];
				inputStream.read(len);
				int filelen = bytetoint(len);

				/*
				 * If zero received, it means file not found on the nodes, try
				 * next node
				 */
				if (filelen == 0) {
					continue;
				}
				byte[] buffer = new byte[4096];
				int byteCount = 0;
				FileOutputStream output = new FileOutputStream(
						Config.DataDirectory + "/" + filename);

				while (byteCount < filelen) {
					int n = inputStream.read(buffer);
					output.write(buffer, 0, n);
					byteCount += n;
				}

				output.close();
				inputStream.close();
				objectOutputStream.close();
				socket.close();
				worker.addfiletolist(filename);
				return;
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		throw new RuntimeException("Ask for file failed");
	}

	/*
	 * Ask the Map result files from other node for the reduce task, the reduce
	 * task is identified by the node info id
	 */
	public static ArrayList<String> fetchfile(long JobID,
			WorkerInfo workerinfo, List<WorkerInfo> infos, Worker worker,
			int taskID) throws RuntimeException {

		ArrayList<String> retfilename = new ArrayList<String>();

		WorkerInfo local = Config.info.get(Config.workerID);

		for (WorkerInfo info : infos) {
			try {

				/* IF the node is the local node, process the file directly */
				if (info.equals(local)) {
					List<String> fileNames = copyMapperResult(worker, JobID,
							workerinfo, taskID);

					if (fileNames != null && fileNames.size() != 0) {
						retfilename.addAll(fileNames);
					}
					continue;
				}

				Socket socket = new Socket(info.getIpAddress(), info.getPort());

				InputStream inputStream = socket.getInputStream();

				ObjectOutputStream objectOutputStream = new ObjectOutputStream(
						socket.getOutputStream());

				Message mesg = new Message();

				mesg.setType(Message.MessageType.FILE_FETCH);
				mesg.setFetcheFilename(null);
				mesg.setFetchworkerInfo(workerinfo);
				mesg.setJobId(JobID);
				objectOutputStream.writeObject(mesg);

				byte[] len = new byte[4];
				inputStream.read(len);
				int filelen = bytetoint(len);

				if (filelen == 0) {
					socket.close();
					throw new RuntimeException("fetch file from "
							+ info.getId() + " failed");
				}
				byte[] buffer = new byte[4096];

				int byteCount = 0;

				System.out.println("Current fetching node is "
						+ workerinfo.getId());
				String filename = "JobID_" + JobID + "_FromMaper_"
						+ info.getId() + "_forReducerTask_" + taskID;

				FileOutputStream output = new FileOutputStream(
						Config.DataDirectory + "/" + filename);

				while (byteCount < filelen) {
					int n = inputStream.read(buffer);
					output.write(buffer, 0, n);
					byteCount += n;
				}

				output.close();
				socket.close();

				retfilename.add(filename);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return retfilename;
	}

	/* Rename the Mapper results for further reduce task processing */
	public static List<String> copyMapperResult(Worker worker, long jobID,
			WorkerInfo workerInfo, int taskID) throws IOException {
		List<String> fileList = worker.getfileList();
		List<String> localfile = new ArrayList<String>();
		List<String> destlist = new ArrayList<String>();
		int count = 0;

		String start = "Job_" + jobID;
		String end = "ForReducer_" + workerInfo.getId();
		synchronized (fileList) {
			for (String tempfilename : fileList) {
				if (tempfilename.startsWith(start)
						&& tempfilename.endsWith(end)) {
					localfile.add(tempfilename);
				}
			}
		}

		if (localfile.size() == 0) {
			return null;
		}

		for (String filename : localfile) {
			String destfile = "JobID_" + jobID + "_FromMaper_"
					+ workerInfo.getId() + "_forReducerTask_" + taskID + "dup_"
					+ count;

			FileOutputStream dest = new FileOutputStream(Config.DataDirectory
					+ "/" + destfile);

			File srcfile = new File(Config.DataDirectory + "/" + filename);

			Path srcpath = Paths.get(srcfile.getPath());

			Files.copy(srcpath, dest);

			dest.close();

			destlist.add(destfile);
			count++;
		}

		return destlist;

	}

	public void run() {
		try {
			File file = new File(Config.DataDirectory + "/" + filename);
			int length = (int) file.length();

			outputStream.write(inttobyte(length));

			FileInputStream inputStream = new FileInputStream(file);
			byte[] buffer = new byte[4096];

			int buffersize = 0;

			while ((buffersize = inputStream.read(buffer)) != -1) {
				outputStream.write(buffer, 0, buffersize);
			}

			inputStream.close();
			outputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
