package task;

import java.io.Serializable;
import java.util.List;

import worker.WorkerInfo;

public class ReduceTask implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8451457993871474773L;
	
	//The reducer should know which workers to get data from
	private List<WorkerInfo> mappers;

	public List<WorkerInfo> getMappers() {
		return mappers;
	}

	public void setMappers(List<WorkerInfo> mappers) {
		this.mappers = mappers;
	}

}
