package task;

import util.Config;
import util.FileTransmission;
import util.KeyValuePair;
import util.Message;
import worker.Worker;
import worker.WorkerInfo;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class ReducerThread extends Thread {

    private ReduceTask task;
    private Worker worker;
    private long jobID;
    private int taskID;
    private WorkerInfo info;
    private List<WorkerInfo> maperInfos;
    private ArrayList<String> inputs;

    public ReducerThread(ReduceTask task, Worker worker) {
        this.task = task;
        this.jobID = task.getJobId();
        this.taskID = task.getTaskId();
        this.info = task.getReducer();
        this.maperInfos = task.getMappers();
        this.worker = worker;
    }

    public void run() {
        try {

            System.out.println("Reducer task " + taskID + " is now running");

			/*Try to fetch Map result file from other node */
            inputs = FileTransmission.fetchfile(jobID, info, maperInfos, worker, taskID);

            if (inputs.size() != maperInfos.size()) {
                System.out.println("Fetch files from mapper failed!");
                return;
            }

			/*Add all the fetched files to file list*/
            for (int i = 0; i < inputs.size(); i++) {
                worker.addfiletolist(inputs.get(i));
            }

            /*Read the fetched files and build them into a hashmap*/
            HashMap<String, ArrayList<String>> map = buidmap();
            MapReduceMethod method = task.getMethod();

            Object[] keyArray = map.keySet().toArray();
            ArrayList<String> outputs = new ArrayList<String>();

			/*Sort the fetched data*/
            Arrays.sort(keyArray);

			/*Send the sorted Key value pair to reduce method*/
            for (Object key : keyArray) {
                ArrayList<String> valueList = map.get(key);
                KeyValuePair output = method.reduce((String) key, valueList);
                outputs.add(output.getKey() + "\t" + output.getValue() + "\n");
            }

            /*Write down the Reduce result*/
            String outputfilename = "Job_" + jobID + "_Task_" + taskID + "_ReducerResult_" + info.getId();
            FileOutputStream outputStream = new FileOutputStream(new File(Config.DataDirectory + "/" + outputfilename), false);

            for (int i = 0; i < outputs.size(); i++) {
                outputStream.write(outputs.get(i).getBytes());
            }

            outputStream.close();

            /*Send task done message to master*/
            Socket socket = new Socket(Config.MASTER_IP, Config.MASTER_PORT);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            Message mesg = new Message();
            mesg.setType(Message.MessageType.REDUCE_RES);
            mesg.setJobId(jobID);
            mesg.setReduceTask(task);
            objectOutputStream.writeObject(mesg);
            objectOutputStream.close();
            socket.close();

            System.out.println("Reducer task " + taskID + " now finished");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RuntimeException e) {
            e.printStackTrace();
            return;
        }
    }

    /*Read the fetched files and build them into a hashmap, all the key value pair with same key are merged*/
    private HashMap<String, ArrayList<String>> buidmap() {
        HashMap<String, ArrayList<String>> map = null;
        try {
            map = new HashMap<String, ArrayList<String>>();

            for (String filename : inputs) {
                String linebuf;
                System.out.println("Now Processing file " + filename);
                BufferedReader reader = new BufferedReader(new FileReader(Config.DataDirectory + "/" + filename));

                while ((linebuf = reader.readLine()) != null) {
                    String[] kvPair = linebuf.split("\t");

                    if (kvPair.length != 2) {
                        continue;
                    }

                    ArrayList<String> valueList;

                    if (map.containsKey(kvPair[0])) {
                        valueList = map.get(kvPair[0]);
                    } else {
                        valueList = new ArrayList<>();
                        map.put(kvPair[0], valueList);
                    }

                    valueList.add(kvPair[1]);
                }

                reader.close();
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return map;
    }
}
