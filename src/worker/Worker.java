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

    public void start() {

        /*Get the info for the nodes*/
        this.info = Config.getWorkerInfo();

        /*Set up the heart beat*/
        WorkerHeartbeat heartbeat = new WorkerHeartbeat(this);
        heartbeat.start();

        /*Initialize the File system by scanning the local files and build the filelist*/
        try {
            DFSbootstrap();
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        System.out.println("Bootstrapping finished");

        /*Send register message to master node*/
        register();
        System.out.println("Register to Master Success");


        /*Make the server socket to listen request from other nodes*/
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
            System.out.println("Address: " + info.getIpAddress() + ":" + info.getPort());

            /*Processing requests received*/
            while ((socket = listenSocket.accept()) != null) {

                ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
                Message mesg = (Message) input.readObject();

                switch (mesg.getType()) {
                    /*Processing Map task from master*/
                    case MAP_REQ:
                        System.out.println("Mapper Req received");
                        MapperThread mt = new MapperThread(mesg.getMapTask(), this);

                        /*Start the Map task in a new thread*/
                        mt.start();
                        break;
                    /*Processing the Reduce task from master*/
                    case REDUCE_REQ:

                        System.out.println("Reducer Req received");
                        ReducerThread rt = new ReducerThread(mesg.getReduceTask(), this);

                        /*Start the Reduce task in a new thread*/
                        rt.start();
                        break;

                    /*Processing the File fetching request from reduce task on other nodes*/
                    case FILE_FETCH:
                        System.out.println("File Fetch received from " + mesg.getFetchworkerInfo().getId());

                        /*Get the corresponding jobID of the reduce task*/
                        long jobID = mesg.getJobId();

                        /*Get the corresponding workerID of the node*/
                        int WorkerID = mesg.getFetchworkerInfo().getId();
                        String filename = null;

                        String start = "Job_" + jobID;
                        String end = "ForReducer_" + WorkerID;

                        /*Check the file list to see if there is map result file for the Reduce task*/
                        for (String tempfilename : fileList) {
                            if (tempfilename.startsWith(start) && tempfilename.endsWith(end)) {
                                filename = tempfilename;
                                break;
                            }
                        }

                        /*If file not found, send a zero back*/
                        if (filename == null) {
                            OutputStream outputStream = socket.getOutputStream();
                            outputStream.write(FileTransmission.inttobyte(0));
                            break;
                        }

                        /*If file found, start a new thread and send the file*/
                        transmission = new FileTransmission(filename, socket.getOutputStream());

                        transmission.start();

                        break;

                    case FILE_REQ:
                        /*Processing file fetch request from map task on other nodes*/
                        System.out.println("File request received");
                        String fetch_name = mesg.getFetcheFilename();

                        /*if file not found, send zero back*/
                        if (!fileList.contains(fetch_name)) {
                            OutputStream outputStream = socket.getOutputStream();
                            outputStream.write(FileTransmission.inttobyte(0));
                            break;
                        }

                        /*if file found, start a new thread and send the file*/
                        transmission = new FileTransmission(fetch_name, socket.getOutputStream());

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

    /*Send register info to master node*/
    private void register() {
        try {
            Socket socket = new Socket(Config.MASTER_IP, Config.MASTER_PORT);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
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

    /*File system initialization, scan the local files and add them to the file list*/
    private void DFSbootstrap() throws Exception {
        File dir = new File(Config.DataDirectory);

        if (!dir.exists()) {
            throw new IOException("The Working directory" + Config.DataDirectory + "not exits");
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
