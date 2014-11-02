package task;

import java.util.List;

import util.InputFile;

public interface ClientJob {

	public List<InputFile> getfiles();

	public MapReduceJob getJob();

	public int getMaxReduceFile();

}
