package worker;

public class WorkerInfo {
	private final String ipAddress;
	private final int port;
	private int id;

	public WorkerInfo(String ipAddress, int port) {
		this.ipAddress = ipAddress;
		this.port = port;
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
}
