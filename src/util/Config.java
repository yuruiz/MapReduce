package util;

import java.util.List;

import worker.WorkerInfo;

public class Config {

	public static String MASTER_IP;
	public static List<WorkerInfo> info;
	public static int workerID;
	public static int MASTER_PORT;
	public static int POLLING_PORT;
	public static int TIME_OUT;
	public static int SLEEP_TIME;

	public static void setup(String[] args) {
		if (args.length < 1) {
			System.err.println("In correct arguments");
			return;
		}

		ConfigParser p = new ConfigParser("Config.txt");

		workerID = Integer.parseInt(args[0]);
		WorkerInfo master = p.getMasterInfo();
		MASTER_IP = master.getIpAddress();
		MASTER_PORT = master.getPort();
		POLLING_PORT = master.getPollingPort();
		TIME_OUT = p.getNumValue("TIME_OUT");
		SLEEP_TIME = p.getNumValue("SLEEP_TIME");

	}

	public static WorkerInfo getWorkerInfo() {
		return info.get(workerID);
	}
}