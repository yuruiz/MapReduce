package util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import worker.WorkerInfo;

public class ConfigParser {

	private String fileName;

	public ConfigParser(String fileName) {
		this.fileName = fileName;
	}

	public int getMasterPort() {
		return Integer.parseInt(this.findValue("MASTER_PORT"));
	}

	public List<WorkerInfo> getWorkers() {
		List<WorkerInfo> workers = new ArrayList<WorkerInfo>();
		String line = this.findValue("WORKER");
		String[] ips = line.split("[,]");
		for (String s : ips) {
			String[] ipPort = s.split("[:]");
			if (ipPort.length < 3) {
				continue;
			}
			WorkerInfo w = new WorkerInfo(ipPort[0],
					Integer.parseInt(ipPort[1]), Integer.parseInt(ipPort[2]));
			workers.add(w);
		}

		return workers;

	}

	public int getTimeOut() {
		return Integer.parseInt((this.findValue("TIME_OUT")));
	}

	private String findValue(String key) {
		try {
			Scanner s = new Scanner(new FileInputStream(fileName));
			while (s.hasNextLine()) {
				String line = s.nextLine();
				if (line.contains(key.subSequence(0, key.length()))) {
					return line.substring(key.length() + 2, line.length() - 1);
				}
			}
			s.close();
		} catch (FileNotFoundException e) {
			System.err.println("Can't find configuration file");
		}

		return null;
	}

	// a test
	public static void main(String[] args) {
		ConfigParser p = new ConfigParser("Config.txt");
		System.out.println(p.findValue("MASTER_PORT"));
		System.out.println(p.findValue("WORKER"));

		for (WorkerInfo w : p.getWorkers()) {
			System.out.println(w.getIpAddress() + " " + w.getPort() + " "
					+ w.getPollingPort());
		}

	}

}
