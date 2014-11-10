package master;

import java.util.Scanner;
import task.ClientJob;

public class MasterCommandUI {

	private static Master master;

	public static void main(String[] args) {

		master = new Master();

		System.out.println("*** Usage ***");
		System.out.println("Run a map reduce job: run <JobClassName>");
		System.out.println("Shut down server: shut down");
		System.out.println("Show all running jobs: print all");
		System.out.println("*** end usage ***");

		Thread t = new Thread(master);
		t.setDaemon(true);
		t.start();
		Scanner s = new Scanner(System.in);

		while (true) {

			String line = s.nextLine();
			System.out.println("enter");
			String[] inputs = line.split(" ");
			if (inputs == null || inputs.length < 1) {
				System.out.println("Unrecognized Message");
				continue;
			}

			// case shut down
			if (inputs[0].equalsIgnoreCase("shut")) {
				break;
			}

			// case run job
			if (inputs[0].equalsIgnoreCase("run")) {
				if (inputs.length < 2) {
					System.out.println("In correct input");
					continue;
				}
				String jobName = inputs[1];
				try {
					Class<?> jobClass = Class.forName(jobName);
					ClientJob job = (ClientJob) jobClass.newInstance();
					master.newJob(job);

				} catch (ClassNotFoundException | InstantiationException
						| IllegalAccessException e) {
					System.out.println("Unrecognized job name");
					continue;
				}
			}

			System.out.println("Unrecognized message");

		}

		s.close();
		System.out.println("exited");

	}
}
