package util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import worker.WorkerInfo;

/**
 * The config parser class parse the config settings from a config.txt file.
 * 
 * @author siyuwei
 *
 */
public class ConfigParser {

	private String fileName;

	public ConfigParser(String fileName) {
		this.fileName = fileName;
	}

	public int getNumValue(String key) {
		return Integer.parseInt(this.findValue(key));
	}

	public WorkerInfo getMasterInfo() {
		return getWorkers("MASTER").get(0);
	}

	public List<WorkerInfo> getWorkers() {
		return getWorkers("WORKER");
	}

	/**
	 * helper method that parses worker configuration information from a line
	 * 
	 * @param key
	 *            the string
	 * @return worker configuration parsed from the line
	 */
	private List<WorkerInfo> getWorkers(String key) {
		List<WorkerInfo> workers = new ArrayList<WorkerInfo>();
		String line = this.findValue(key);
		String[] ips = line.split("[,]");
		int i = 0;
		for (String s : ips) {
			String[] ipPort = s.split("[:]");
			if (ipPort.length < 3) {
				continue;
			}
			WorkerInfo w = new WorkerInfo(ipPort[0].trim(),
					Integer.parseInt(ipPort[1]), Integer.parseInt(ipPort[2]));
			w.setId(i);
			i++;
			workers.add(w);
		}

		return workers;

	}

	/**
	 * With a predefined format of "key=value", find the value with a given key
	 * 
	 * @param key
	 *            the given key
	 * @return the value found or null otherwise
	 */
	private String findValue(String key) {
		try {
			Scanner s = new Scanner(new FileInputStream(fileName));
			while (s.hasNextLine()) {
				String line = s.nextLine();
				if (line.contains(key.subSequence(0, key.length()))) {
					s.close();
					return line.substring(key.length() + 2, line.length() - 1);
				}
			}
			s.close();
		} catch (FileNotFoundException e) {
			System.err.println("Can't find configuration file");
		}

		return null;
	}

	// a simple test
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
