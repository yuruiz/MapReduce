package task;

import java.io.Serializable;
import java.util.List;

/**
 * Interface defined for a client job, a client job should define the input
 * files for the job, the max reduce file accepted
 * 
 * @author siyuwei
 *
 */
public interface ClientJob extends Serializable, MapReduceMethod {

	/**
	 * return the data files the map reduce task will run on
	 * 
	 * @return
	 */
	public List<String> getfiles();

	/**
	 * The number of reduce files the job accepts
	 * 
	 * @return
	 */
	public int getMaxReduceFile();

	/**
	 * The id of this job
	 * 
	 * @return
	 */
	public int getId();

}
