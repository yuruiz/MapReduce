package master;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;

import worker.*;
import util.Config;

public class Master {
	
	private Map<Integer, WorkerInfo> idToWorker;
	
	
	
	private final int port = Config.MASTER_PORT;

	public void run() {
		try {
			ServerSocket server = new ServerSocket(port);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}