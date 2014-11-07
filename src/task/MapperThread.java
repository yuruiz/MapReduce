package task;

import util.FileReader;
import util.FileWriter;
import util.KeyValuePair;
import util.Partition;
import worker.WorkerInfo;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class MapperThread extends Thread{

    private MapTask task;
    private long jobID;
    private int taskID;
    private List<WorkerInfo> infos;

    public MapperThread(MapTask task) {
        this.task = task;
        this.jobID = task.getJobId();
        this.taskID = task.getTaskId();
        this.infos = task.getReducers();
    }

    public void run() {
        MapReduceMethod method = task.getMethod();
        List<Partition> filepartitions = task.getPartitions();
        List<KeyValuePair> outputs = new LinkedList<KeyValuePair>();

        for (int i = 0; i < filepartitions.size(); i++) {
            String fileName = filepartitions.get(i).getFileName();
            Partition p = filepartitions.get(i);
            File file = new File(fileName);

            if (!file.exists()) {
                System.out.println("File " + fileName + " not exist");
                //todo: fetch file
            }

            FileReader reader = new FileReader(fileName);
            String[][] input = reader.getKeyValuePairs(p.getStartIndex(), p.getLength());

            for (int j = 0; j < input.length; j++) {
                List<KeyValuePair> temp =method.map(input[i][0], input[i][1]);
                if (temp != null) {
                    outputs.addAll(temp);
                }
            }
        }

        shuffle(outputs);

        //todo send maptask done message to master
    }

    private void shuffle(List<KeyValuePair> outputs) {
        int reduceNum = task.getReducers().size();

        HashMap<Integer, List<KeyValuePair>> map = new HashMap<Integer, List<KeyValuePair>>(reduceNum);

        for (int i = 0; i < reduceNum; i++) {
            List<KeyValuePair> list = new LinkedList<KeyValuePair>();
            map.put(i, list);
        }

        for(int i = 0; i < outputs.size(); i++)
        {
            KeyValuePair record;
            String key;
            record = outputs.get(i);
            key = record.getKey();
            int tempKey;
            if(key.length() > 0)
                tempKey = key.charAt(0) % reduceNum;
            else
                tempKey = 0;

            map.get(tempKey).add(record);
        }


        for (int i = 0; i < reduceNum; i++) {
            List<KeyValuePair> templist = map.get(i);

            String outputName = "Job_"+jobID+"_Task_"+taskID+"_ForReducer_"+infos.get(i).getId();

            FileWriter outputfile = new FileWriter(outputName);

            outputfile.write(templist);

        }

        //todo send the files to reducers
    }
}