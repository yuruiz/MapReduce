package util;

import worker.WorkerInfo;

import java.util.List;

/**
 * An encapsulation of all the configuration setting
 * 
 * @author siyuwei
 *
 */
public class Config {

	public static String MASTER_IP;
	// information of all the workers
	public static List<WorkerInfo> info;
	// the directory of the source data being stored
	public static String DataDirectory;
	// the id of the worker
	public static int workerID;
	public static int MASTER_PORT;
	public static int POLLING_PORT;
	// polling timeout
	public static int TIME_OUT;
	//
	public static int SLEEP_TIME;
	public static int MAX_REDUCE;

	public static void setup(String[] args) {

		ConfigParser p = new ConfigParser("Config.txt");

		if (args.length == 1) {
			DataDirectory = args[0];
		} else if (args.length == 2) {
			DataDirectory = args[0];
			workerID = Integer.parseInt(args[1]);
		}
		info = p.getWorkers();
		WorkerInfo master = p.getMasterInfo();
		MASTER_IP = master.getIpAddress();
		MASTER_PORT = master.getPort();
		POLLING_PORT = master.getPollingPort();
		TIME_OUT = p.getNumValue("TIME_OUT");
		SLEEP_TIME = p.getNumValue("SLEEP_TIME");
		MAX_REDUCE = p.getNumValue("MAX_REDUCE");

	}

	public static WorkerInfo getWorkerInfo() {
		return info.get(workerID);
	}
}
