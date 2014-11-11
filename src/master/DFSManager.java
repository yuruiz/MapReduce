package master;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import util.InputFile;
import worker.WorkerInfo;

public class DFSManager {
	private Map<String, InputFile> data;

	public DFSManager() {
		data = new HashMap<String, InputFile>();
	}

	public InputFile getFile(String fileName) {
		return data.get(fileName);
	}

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
