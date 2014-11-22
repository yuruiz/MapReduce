package util;

import task.MapTask;
import task.ReduceTask;
import worker.WorkerInfo;

import java.io.Serializable;
import java.util.List;

/**
 * This class represents the message that is sent between master/worker or
 * worker/worker. There are different type of messages and the information
 * needed of a message type is encapsulated in the message.
 * 
 * @author siyuwei
 *
 */
public class Message implements Serializable {

	/**
	 * The message types, including map/reduce request, map/reduce response,
	 * file fetch request and register request.
	 * 
	 * @author siyuwei
	 *
	 */
	public enum MessageType {
		MAP_REQ, MAP_RES, REDUCE_REQ, REDUCE_RES, FILE_FETCH, WORKER_REG, RESEND, FILE_REQ
	}

	private static final long serialVersionUID = 7264137218310503076L;
	// message type
	private MessageType type;

	private MapTask mapTask;
	private ReduceTask reduceTask;
	private String fetcheFilename;

	// the information of the worker that is requiring a file fetch
	private WorkerInfo fetchworkerInfo;
	// the id of a map/reduce job
	private long jobId;
	// info of the worker that is requiring register
	private WorkerInfo worker;
	// input files for map
	private List<InputFile> inputs;
	// reduce result address
	private String result;

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

	public WorkerInfo getWorker() {
		return worker;
	}

	public void setReceiver(WorkerInfo receiver) {
		this.worker = receiver;
	}

	public void setInputs(List<InputFile> inputs) {
		this.inputs = inputs;
	}

	public List<InputFile> getInputs() {
		return this.inputs;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

}
