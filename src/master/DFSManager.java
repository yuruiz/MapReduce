package master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import util.InputFile;
import worker.WorkerInfo;

/**
 * This class acts as the implementation of a distributed file system manager.
 * At DFS boostrapping phase, every worker will report master with the data
 * files it has. The DFS manager will collect these information. Specifically,
 * manager knows that how many files there are and where do they reside.
 * 
 * @author siyuwei
 *
 */
public class DFSManager {
	private Map<String, InputFile> data;

	public DFSManager() {
		data = new HashMap<String, InputFile>();
	}

	/**
	 * Get all the info of a give file Name
	 * 
	 * @param fileName
	 *            the given file name
	 * @return
	 */
	public InputFile getFile(String fileName) {
		return data.get(fileName);
	}

	/**
	 * Add a file to the file system
	 * 
	 * @param fileName
	 *            the name of the file
	 * @param worker
	 *            the worker this data file exists on
	 * @param length
	 *            the length of the file
	 */
	public void addFile(String fileName, WorkerInfo worker, int length) {
		if (data.get(fileName) == null) {
			List<WorkerInfo> list = new ArrayList<WorkerInfo>();
			list.add(worker);
			data.put(fileName, new InputFile(fileName, list, length));
		} else {
			data.get(fileName).addLocation(worker);
		}
	}

}
