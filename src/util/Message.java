package util;

import task.MapReduceJob;
import task.MapTask;
import task.ReduceTask;

import java.io.Serializable;

public class Message implements Serializable {

    /**
     *
     */

    public enum MessageType {
        MAP_REQ, MAP_RES, REDUCE_REQ, REDUCE_RES, FILE_FETCH, FILE_PUSH, WORKER_REG
    }

    private static final long serialVersionUID = 7264137218310503076L;

    private MessageType type;

    private MapReduceJob job;
    private MapTask mapTask;
    private ReduceTask reduceTask;

    public void setType(MessageType type) {
        this.type = type;
    }

    public MessageType getType() {
        return type;
    }

    public MapReduceJob getJob() {
        return job;
    }

    public void setJob(MapReduceJob job) {
        this.job = job;
    }

    public MapTask getMapTask() {
        return mapTask;
    }

    public void setMapTask(MapTask mapTask) {
        this.mapTask = mapTask;
    }

    public ReduceTask getReduceTask() {
        return reduceTask;
    }

    public void setReduceTask(ReduceTask reduceTask) {
        this.reduceTask = reduceTask;
    }

}