package util;

import worker.WorkerInfo;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yuruiz on 11/8/14.
 */
public class FileTransmission {

    public static byte[] inttobyte(int myInteger){
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(myInteger).array();
    }


    public static int bytetoint(byte [] byteBarray){
        return ByteBuffer.wrap(byteBarray).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }


    public static void askforfile(String filename, List<WorkerInfo> infos) {

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
                break;
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public static ArrayList<String> fetchfile(long JobID, WorkerInfo workerinfo, List<WorkerInfo> infos) {

        ArrayList<String> retfilename = new ArrayList<String>();

        for (int i = 0; i < infos.size(); i++) {
            try {
                WorkerInfo info = infos.get(i);

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

                FileOutputStream output = new FileOutputStream(filename);

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
}
