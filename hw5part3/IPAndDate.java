package hw5part3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;

public class IPAndDate implements WritableComparable<IPAndDate> {
	private String ip = new String();
	private Date accessDate = new Date();
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");

	@Override
	public void readFields(DataInput arg0) throws IOException {
		ip = arg0.readUTF();
		accessDate = new Date(arg0.readLong());

	}

	public IPAndDate() {
		super();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(ip);
		arg0.writeLong(accessDate.getTime());
	}

	@Override
	public int compareTo(IPAndDate arg0) {
		if (this.getIp().compareTo(arg0.getIp()) == 0)
			return this.getAccessDate().compareTo(arg0.getAccessDate());
		else
			return this.getIp().compareTo(arg0.getIp());
	}

	@Override
	public String toString() {
		return ip + "     " + frmt.format(this.getAccessDate());
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public Date getAccessDate() {
		return accessDate;
	}

	public void setAccessDate(Date accessDate) {
		this.accessDate = accessDate;
	}
}
