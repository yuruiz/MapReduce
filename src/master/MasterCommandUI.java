package master;

import java.util.Scanner;
import task.ClientJob;

/**
 * The command line UI implementation of master which takes client request from
 * command line and run the given map/reduce task.
 * 
 * @author siyuwei
 *
 */
public class MasterCommandUI {

	private Master master;
	private static final String EXAMPLE_PACKAGE = "example.";

	/**
	 * Start running the command line interface
	 */
	public void run() {

		master = new Master();

		/*
		 * Usage hint
		 */
		System.out.println("*** Usage ***");
		System.out.println("Run a map reduce job: run <JobClassName>");
		System.out.println("Shut down server: shut down");
		System.out.println("Show all running jobs: print all");
		System.out.println("*** end usage ***");

		/*
		 * Start running master
		 */
		Thread t = new Thread(master);
		t.setDaemon(true);
		t.start();
		Scanner s = new Scanner(System.in);

		/*
		 * the main loop that keeps taking user input from standard input stream
		 */
		while (true) {

			String line = s.nextLine();
			String[] inputs = line.split(" ");
			if (inputs == null || inputs.length < 1) {
				System.out.println("Unrecognized Message");
				continue;
			}

			// case shut down, break the loop and end program
			if (inputs[0].equalsIgnoreCase("shut")) {
				break;
			}

			// case run job
			if (inputs[0].equalsIgnoreCase("run")) {
				if (inputs.length < 2) {
					System.out.println("In correct input");
					continue;
				}
				String jobName = EXAMPLE_PACKAGE + inputs[1];
				try {
					/*
					 * get the client job class and pass it to master for
					 * execution
					 */
					Class<?> jobClass = Class.forName(jobName);
					ClientJob job = (ClientJob) jobClass.newInstance();
					master.newJob(job);

				} catch (ClassNotFoundException | InstantiationException
						| IllegalAccessException | NoClassDefFoundError e) {
					System.out.println("Unrecognized job name");
					continue;
				}
			}

		}

		s.close();
		System.out.println("exited");

	}

	public static void main(String[] args) {
		MasterCommandUI m = new MasterCommandUI();
		m.run();
	}
}
