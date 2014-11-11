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

public class Worker {

	private WorkerInfo info;
	private ArrayList<String> filelist;
    private ArrayList<InputFile> inputs;

	public Worker() {

	}

	public WorkerInfo getInfo() {
		return info;
	}

	public void start() throws Exception{
		this.info = Config.getWorkerInfo();
		WorkerHeartbeat heartbeat = new WorkerHeartbeat(this);
		heartbeat.start();
        DFSbootstrap();

		register();

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
			while ((socket = listenSocket.accept()) != null) {

				ObjectInputStream input = new ObjectInputStream(
						socket.getInputStream());
				Message mesg = (Message) input.readObject();

				switch (mesg.getType()) {
				case MAP_REQ:

					MapperThread mt = new MapperThread(mesg.getMapTask(), this);

					mt.start();
					break;
				case REDUCE_REQ:

					ReducerThread rt = new ReducerThread(mesg.getReduceTask(), this);

					rt.start();
					break;
				case FILE_FETCH:
					long jobID = mesg.getJobId();
					int WorkerID = mesg.getFetchworkerInfo().getId();
					String filename = null;

					String start = "Job_" + jobID;
					String end = "ForReducer_" + WorkerID;
					for (String tempfilename : filelist) {
						if (tempfilename.startsWith(start)
								&& tempfilename.endsWith(end)) {
							filename = tempfilename;
							break;
						}
					}

					if (filename == null) {
						// todo send file not found
					}

					transmission = new FileTransmission(filename,
							socket.getOutputStream());

					transmission.start();

					break;

				case FILE_REQ:
					String fetch_name = mesg.getFetcheFilename();

					if (!filelist.contains(fetch_name)) {
						// todo send file not found
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

    private void DFSbootstrap() throws Exception{
        File dir = new File(Config.DataDirectory);

        if (!dir.exists()) {
            throw new IOException("The Working directory" + Config.DataDirectory + "not exits");
        }

        File[] files = dir.listFiles();

        for (File file : files) {
            String filename = file.getName();
            addfiletolist(filename);
            int length = countLines(filename);
            InputFile inputFile = new InputFile(filename, null, length);
            inputs.add(inputFile);
        }

    }

	public void addfiletolist(String filename) {
		synchronized (filelist) {
			if (!filelist.contains(filename)) {
				filelist.add(filename);
			}
		}

	}

	public void removefilefromlist(String filename) {
		synchronized (filelist) {
			if (filelist.contains(filename)) {
				filelist.remove(filename);
			}
		}
	}

    public  int countLines(String filename) throws IOException {
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

	public static void main(String[] args) throws Exception{
        if (args.length != 2) {
            System.out.println("Usage: Worker <Data Direcotry> <Node index>");
            return;
        }

		Config.setup(args);

		Worker worker = new Worker();
		worker.start();
	}

}
