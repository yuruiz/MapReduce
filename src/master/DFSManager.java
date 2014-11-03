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

	public void addFile(String fileName, WorkerInfo worker) {
		if (data.get(fileName) == null) {
			List<WorkerInfo> list = new ArrayList<WorkerInfo>();
			list.add(worker);
			data.put(fileName,
					new InputFile(fileName, list, this.getLength(fileName)));
		} else {
			data.get(fileName).addLocation(worker);
		}
	}

	private int getLength(String file) {

		int length = 0;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			while (reader.readLine() != null) {
				length++;
			}
			reader.close();
		} catch (IOException e) {
			return -1;
		}

		return length;
	}

}
