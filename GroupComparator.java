package hw5part3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator{

    public GroupComparator() {
        super(IPAndDate.class,true);
    }

    @SuppressWarnings("rawtypes")
	@Override
    public int compare(WritableComparable a, WritableComparable b) {

    	IPAndDate t1 = (IPAndDate) a;
    	IPAndDate t2 = (IPAndDate) b;
        return t1.getIp().compareTo(t2.getIp());
    }
}