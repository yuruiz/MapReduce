package util;

import worker.WorkerInfo;

import java.util.List;

public class Config {

	public static String MASTER_IP;
	public static List<WorkerInfo> info;
    public static String DataDirectory;
	public static int workerID;
	public static int MASTER_PORT;
	public static int POLLING_PORT;
	public static int TIME_OUT;
	public static int SLEEP_TIME;

	public static void setup(String[] args) {

		ConfigParser p = new ConfigParser("Config.txt");

		if (args.length == 1) {
            DataDirectory = args[0];
        }else if (args.length == 2) {
            DataDirectory = args[0];
            workerID = Integer.parseInt(args[1]);
        }else{
            System.out.println("Usage: Master <Working Directory>");
            System.out.println("Usage: Master <Working Directory> <Node ID>");
        }
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
