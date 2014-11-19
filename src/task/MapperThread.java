package task;

import util.*;
import worker.Worker;
import worker.WorkerInfo;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class MapperThread extends Thread {

    private Worker worker;
    private MapTask task;
    private long jobID;
    private int taskID;
    private List<WorkerInfo> infos;

    public MapperThread(MapTask task, Worker worker) {
        this.task = task;
        this.jobID = task.getJobId();
        this.taskID = task.getTaskId();
        this.infos = task.getReducers();
        this.worker = worker;
    }

    public void run() {
        System.out.println("Map task " + taskID + " is now running");

        /*Get the customized Mapper method*/
        MapReduceMethod method = task.getMethod();

        /*Get the Mapper input file list*/
        List<Partition> filepartitions = task.getPartitions();

        System.out.println(filepartitions.size());
        List<KeyValuePair> outputs = new LinkedList<KeyValuePair>();

        /*Check if all the files are located in current nodes*/
        for (int i = 0; i < filepartitions.size(); i++) {
            String fileName = filepartitions.get(i).getFileName();
            Partition p = filepartitions.get(i);
            File file = new File(Config.DataDirectory + "/" + fileName);

            /*If file not located locally, try to fetch files from other node that might have it*/
            if (!file.exists()) {
                System.out.println("File " + fileName + " not exist");
                try {
                    FileTransmission.askforfile(fileName, p.getOwners(), worker);
                } catch (RuntimeException e) {
                    e.printStackTrace();
                    return;
                }


                file = new File(Config.DataDirectory + "/" + fileName);

                if (!file.exists()) {
                    System.out.println("Fetch file " + fileName + " failed at mapper task");
                    return;
                }
            }

            /*read the data in the file*/
            FileReader reader = new FileReader(Config.DataDirectory + "/" + fileName);
            String[][] input = reader.getKeyValuePairs(p.getStartIndex(), p.getLength());

            /*Pass the data to the Map task*/
            for (int j = 0; j < input.length; j++) {
                List<KeyValuePair> temp = method.map(input[j][0], input[j][1]);
                if (temp != null) {
                    outputs.addAll(temp);
                }
            }
        }

        System.out.println("Start shuffling");

        /*Shuffle the Map result*/
        shuffle(outputs);

        System.out.println("Finish shuffling");

        /*Send task done message back to master node*/
        try {
            Socket socket = new Socket(Config.MASTER_IP, Config.MASTER_PORT);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            Message mesg = new Message();
            mesg.setType(Message.MessageType.MAP_RES);
            mesg.setJobId(jobID);
            mesg.setMapTask(task);
            objectOutputStream.writeObject(mesg);
            objectOutputStream.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Map task " + taskID + " is now finished");

    }

    private void shuffle(List<KeyValuePair> outputs) {
        int reduceNum = task.getReducers().size();

        System.out.println("Reducer number is " + reduceNum);

        HashMap<Integer, List<KeyValuePair>> map = new HashMap<Integer, List<KeyValuePair>>(reduceNum);

        for (int i = 0; i < reduceNum; i++) {
            List<KeyValuePair> list = new LinkedList<KeyValuePair>();
            map.put(i, list);
        }

        System.out.println("Finish reducer mapper creating");

        System.out.println("Start distribution records, record number " + outputs.size());

        /*Suffle the Map result and dispatch them to corresponding reducer*/
        for (KeyValuePair record : outputs) {
            String key = record.getKey();
            int tempKey;
            if (key.length() > 0) tempKey = key.charAt(0) % reduceNum;
            else tempKey = 0;

            map.get(tempKey).add(record);

        }

        System.out.println("Start writing records to file");

        /*Write a Map result file for each reducer*/
        for (int i = 0; i < reduceNum; i++) {
            List<KeyValuePair> templist = map.get(i);

            String outputName = "Job_" + jobID + "_Task_" + taskID + "_ForReducer_" + infos.get(i).getId();

            FileWriter outputfile = new FileWriter(Config.DataDirectory + "/" + outputName);

            outputfile.write(templist);

            worker.addfiletolist(outputName);

            System.out.println("Writing to file" + outputName + "success");


        }
    }
}