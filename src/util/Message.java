package util;

import task.MapTask;
import task.ReduceTask;
import worker.WorkerInfo;

import java.io.Serializable;

public class Message implements Serializable {

	/**
     *
     */

	public enum MessageType {
		MAP_REQ, MAP_RES, REDUCE_REQ, REDUCE_RES, FILE_FETCH, WORKER_REG, RESEND, FILE_REQ, JOB_DONE
	}

	private static final long serialVersionUID = 7264137218310503076L;

	private MessageType type;
	private MapTask mapTask;
	private ReduceTask reduceTask;
    private String fetcheFilename;
    private WorkerInfo fetchworkerInfo;
	private long jobId;
	private WorkerInfo receiver;

	public void setType(MessageType type) {
		this.type = type;
	}

	public MessageType getType() {
		return type;
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

	public long getJobId() {
		return jobId;
	}

	public void setJobId(long jobId) {
		this.jobId = jobId;
	}

    public void setFetcheFilename(String filename) {
        this.fetcheFilename = filename;
    }

    public String getFetcheFilename() {
        return this.fetcheFilename;
    }

    public void setFetchworkerInfo(WorkerInfo info) {
        this.fetchworkerInfo = info;
    }

    public WorkerInfo getFetchworkerInfo() {
        return fetchworkerInfo;
    }

	public WorkerInfo getReceiver() {
		return receiver;
	}

	public void setReceiver(WorkerInfo receiver) {
		this.receiver = receiver;
	}

}
