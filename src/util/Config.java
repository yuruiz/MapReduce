package util;

import worker.WorkerInfo;

public class Config {
    public static int MASTER_PORT;
    public static String MASTER_IP;
    public static int HEART_BEAT_INTVAL;
    public static WorkerInfo info[];
    public static int workeriD;

    public static void setup(String[] args) {

    }

    public static WorkerInfo getWorkerInfo() {
        return info[workeriD];
    }
}