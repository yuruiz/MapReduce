package util;

import worker.WorkerInfo;

public class Config {
    public static String MASTER_IP;
    public static WorkerInfo info[];
    public static int workeriD;
    public static int MASTER_PORT;
    public static int POLLING_PORT;
    public static int TIME_OUT;
    public static int SLEEP_TIME;

    public static void setup(String[] args) {

    }

    public static WorkerInfo getWorkerInfo() {
        return info[workeriD];
    }
}