package task;

import java.io.Serializable;
import java.util.List;

public interface ClientJob extends Serializable, MapReduceMethod {

	public List<String> getfiles();

	public int getMaxReduceFile();

	public int getId();

}
