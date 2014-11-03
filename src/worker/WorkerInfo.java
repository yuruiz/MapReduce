package worker;

public class WorkerInfo {
	private final String ipAddress;
	private final int port;
	private final int pollingPort;
	private int id;

	public WorkerInfo(String ipAddress, int port, int pollingPort) {
		this.ipAddress = ipAddress;
		this.port = port;
		this.pollingPort = pollingPort;
	}

	public int getPort() {
		return port;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getPollingPort() {
		return pollingPort;
	}
}
