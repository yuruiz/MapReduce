package task;

import util.FileTransmission;
import util.KeyValuePair;
import worker.Worker;
import worker.WorkerInfo;

import java.io.*;
import java.util.*;

public class ReducerThread extends Thread{

    private ReduceTask task;
    private Worker worker;
    private long jobID;
    private int taskID;
    private WorkerInfo info;
    private List<WorkerInfo> maperInfos;
    ArrayList<String> inputs;

    public ReducerThread(ReduceTask task, Worker worker) {
        this.task = task;
        this.jobID = task.getJobId();
        this.taskID = task.getTaskId();
        this.info = task.getReducer();
        this.maperInfos = task.getMappers();
        this.worker = worker;
    }

    public void run() {
        try{

            inputs = FileTransmission.fetchfile(jobID, info, maperInfos);

            if (inputs.size() != maperInfos.size()) {
                System.out.println("Fetch files from mapper failed!");
                return;
            }

            for (int i = 0; i < inputs.size(); i++) {
                worker.addfiletolist(inputs.get(i));
            }

            HashMap<String, ArrayList<String>> map = buidmap();
            MapReduceMethod method = task.getMethod();

            /*Start sort*/
            Object[] keyArray = map.keySet().toArray();
            ArrayList<String> outputs = new ArrayList<String>();
            Arrays.sort(keyArray);

            for (Object key : keyArray) {
                ArrayList<String> valueList = map.get(key);
                KeyValuePair output = method.reduce((String)key, valueList);
                outputs.add(output.getKey() + "\t" + output.getValue() + "\n");
            }

            String outputfilename = "Job_"+jobID+"_Task_"+taskID+"_ReducerResult_"+info.getId();
            FileOutputStream outputStream = new FileOutputStream(new File(outputfilename), false);

            for (int i = 0; i < outputs.size(); i++) {
                outputStream.write(outputs.get(i).getBytes());
            }

            outputStream.close();

            //todo notify the master the reduce task finished
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private HashMap<String, ArrayList<String>> buidmap() {
        HashMap<String, ArrayList<String>> map = null;
        try {
            int fileCount = inputs.size();
            map = new HashMap<String, ArrayList<String>>();

            for (int i = 0; i < fileCount; i++) {
                String linebuf;
                BufferedReader reader = new BufferedReader(new FileReader(inputs.get(i)));

                while ((linebuf = reader.readLine()) != null) {
                    String[] kvPair = linebuf.split("\t");
                    ArrayList<String> valueList;

                    if (map.containsKey(kvPair[0])) {
                        valueList = map.get(kvPair[0]);
                    } else {
                        valueList = new ArrayList<String>();
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