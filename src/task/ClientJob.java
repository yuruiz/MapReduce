package task;

import java.io.Serializable;
import java.util.List;

public interface ClientJob extends Serializable {

	public List<String> getfiles();

	public MapReduceJob getJob();

	public int getMaxReduceFile();

	public int getId();

}
