package worker;

import task.MapperThread;
import task.ReducerThread;
import util.Config;
import util.FileTransmission;
import util.InputFile;
import util.Message;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Worker {

	private WorkerInfo info;
	private List<String> fileList;
	private List<InputFile> inputs;

	public Worker() {
		fileList = new ArrayList<String>();
		inputs = new ArrayList<InputFile>();
	}

	public WorkerInfo getInfo() {
		return info;
	}

	public void start() throws Exception {
		this.info = Config.getWorkerInfo();
		WorkerHeartbeat heartbeat = new WorkerHeartbeat(this);
		heartbeat.start();

		DFSbootstrap();
		System.out.println("Bootstrapping finished");

		register();
		System.out.println("Register to Master Success");

		ServerSocket listenSocket = null;
		try {
			listenSocket = new ServerSocket(info.getPort());
		} catch (IOException e) {
			e.printStackTrace();
		}

		assert listenSocket != null;

		Socket socket;
		try {
			FileTransmission transmission;
			System.out.println("Worker Starting");
			System.out.println("Address: " + info.getIpAddress() + ":"
					+ info.getPort());
			while ((socket = listenSocket.accept()) != null) {

				ObjectInputStream input = new ObjectInputStream(
						socket.getInputStream());
				Message mesg = (Message) input.readObject();

				switch (mesg.getType()) {
				case MAP_REQ:
					System.out.println("Mapper Req received");
					MapperThread mt = new MapperThread(mesg.getMapTask(), this);

					mt.start();
					break;
				case REDUCE_REQ:

					System.out.println("Reducer Req received");
					ReducerThread rt = new ReducerThread(mesg.getReduceTask(),
							this);

					rt.start();
					break;
				case FILE_FETCH:
					System.out.println("File Fetch received from " + mesg.getFetchworkerInfo().getId());
					long jobID = mesg.getJobId();
					int WorkerID = mesg.getFetchworkerInfo().getId();
					String filename = null;

					String start = "Job_" + jobID;
					String end = "ForReducer_" + WorkerID;
					for (String tempfilename : fileList) {
						if (tempfilename.startsWith(start)
								&& tempfilename.endsWith(end)) {
							filename = tempfilename;
							break;
						}
					}

					if (filename == null) {
                        OutputStream outputStream = socket.getOutputStream();
                        outputStream.write(FileTransmission.inttobyte(0));
                        break;
					}

					transmission = new FileTransmission(Config.DataDirectory
							+ "/" + filename, socket.getOutputStream());

					transmission.start();

					break;

				case FILE_REQ:
					System.out.println("File request received");
					String fetch_name = mesg.getFetcheFilename();

					if (!fileList.contains(fetch_name)) {
                        OutputStream outputStream = socket.getOutputStream();
                        outputStream.write(FileTransmission.inttobyte(0));
						break;
					}

					transmission = new FileTransmission(fetch_name,
							socket.getOutputStream());

					transmission.start();
					break;
				default:
					System.out.println("Error! Unknown Message Type received!");
					break;
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private void register() {
		try {
			Socket socket = new Socket(Config.MASTER_IP, Config.MASTER_PORT);
			ObjectOutputStream out = new ObjectOutputStream(
					socket.getOutputStream());
			Message mesg = new Message();
			mesg.setType(Message.MessageType.WORKER_REG);
			mesg.setReceiver(this.info);
			mesg.setInputs(inputs);
			out.writeObject(mesg);
			out.close();
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void DFSbootstrap() throws Exception {
		File dir = new File(Config.DataDirectory);

		if (!dir.exists()) {
			throw new IOException("The Working directory"
					+ Config.DataDirectory + "not exits");
		}

		File[] files = dir.listFiles();

		for (File file : files) {
			String filename = file.getName();
			System.out.println(" Get file " + filename);
			addfiletolist(filename);
			int length = countLines(Config.DataDirectory + "/" + filename);
			InputFile inputFile = new InputFile(filename, null, length);
			inputs.add(inputFile);
		}

	}

	public void addfiletolist(String filename) {
		synchronized (fileList) {
			if (!fileList.contains(filename)) {
				fileList.add(filename);
			}
		}

	}

	public void removefilefromlist(String filename) {
		synchronized (fileList) {
			if (fileList.contains(filename)) {
				fileList.remove(filename);
			}
		}
	}

    public List<String> getfileList() {
        return fileList;
    }

	public int countLines(String filename) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		int length = 0;
		try {
			while (reader.readLine() != null) {
				length++;
			}
			return length;
		} finally {
			reader.close();
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: Worker <Data Direcotry> <Node index>");
			return;
		}

		Config.setup(args);

		Worker worker = new Worker();
		worker.start();
	}

}
