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
 * Created by yuruiz on 11/8/14.
 */
public class FileTransmission extends Thread{

    private String filename;
    private OutputStream outputStream;

    public FileTransmission(String filename, OutputStream outputStream) {
        this.filename = filename;
        this.outputStream = outputStream;
    }

    public static byte[] inttobyte(int myInteger){
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(myInteger).array();
    }


    public static int bytetoint(byte [] byteBarray){
        return ByteBuffer.wrap(byteBarray).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }


    public static void askforfile(String filename, List<WorkerInfo> infos, Worker worker) {

        for (int i = 0; i < infos.size(); i++) {
            try {
                WorkerInfo info = infos.get(i);

                Socket socket = new Socket(info.getIpAddress(), info.getPort());

                InputStream inputStream = socket.getInputStream();

                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());

                Message mesg = new Message();

                mesg.setType(Message.MessageType.FILE_REQ);
                mesg.setFetcheFilename(filename);
                objectOutputStream.writeObject(mesg);

                byte[] len = new byte[4];
                inputStream.read(len);
                int filelen = bytetoint(len);
                byte[] buffer = new byte[4096];

                int byteCount = 0;

                FileOutputStream output = new FileOutputStream(filename);

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
                break;
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public static ArrayList<String> fetchfile(long JobID, WorkerInfo workerinfo, List<WorkerInfo> infos, Worker worker) {

        ArrayList<String> retfilename = new ArrayList<String>();

        WorkerInfo local = Config.info.get(Config.workerID);

        for (WorkerInfo info: infos) {
            try {

                if (info.equals(local)) {
                    String filename = copyMapperResult(worker, JobID, workerinfo);

                    if (filename != null) {
                        retfilename.add(filename);
                    }
                    continue;
                }

                Socket socket = new Socket(info.getIpAddress(), info.getPort());

                InputStream inputStream = socket.getInputStream();

                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());

                Message mesg = new Message();

                mesg.setType(Message.MessageType.FILE_FETCH);
                mesg.setFetcheFilename(null);
                mesg.setFetchworkerInfo(workerinfo);
                mesg.setJobId(JobID);
                objectOutputStream.writeObject(mesg);

                byte[] len = new byte[4];
                inputStream.read(len);
                int filelen = bytetoint(len);
                byte[] buffer = new byte[4096];

                int byteCount = 0;

                String filename = "JobID_" + JobID + "_FromMaper_" + info.getId();

                FileOutputStream output = new FileOutputStream(Config.DataDirectory + "/" + filename);

                while (byteCount < filelen) {
                    int n = inputStream.read(buffer);
                    output.write(buffer, 0, n);
                    byteCount += n;
                }

                output.close();

                retfilename.add(filename);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return retfilename;
    }

    public static String copyMapperResult(Worker worker, long jobID, WorkerInfo workerInfo) throws IOException{
        List<String> fileList = worker.getfileList();

        String filename = null;

        String start = "Job_" + jobID;
        String end = "ForReducer_" + workerInfo.getId();
        for (String tempfilename : fileList) {
            if (tempfilename.startsWith(start)
                    && tempfilename.endsWith(end)) {
                filename = tempfilename;
                break;
            }
        }

        String destfile = "JobID_" + jobID + "_FromMaper_" + workerInfo.getId();

        FileOutputStream dest = new FileOutputStream(Config.DataDirectory + "/" + destfile);

        File srcfile = new File(Config.DataDirectory + "/" + filename);

        Path srcpath = Paths.get(srcfile.getPath());

        Files.copy(srcpath, dest);

        dest.close();

        return destfile;

    }

    public void run() {
        try{
            File file = new File(filename);
            int length = (int)file.length();

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
