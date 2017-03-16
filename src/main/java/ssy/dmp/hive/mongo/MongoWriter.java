package ssy.dmp.hive.mongo;

import java.io.IOException;
import java.util.Map;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.*;

import com.mongodb.BasicDBObject;
import org.bson.BSONObject;

public class MongoWriter implements RecordWriter {
	MongoTable table;

	public MongoWriter(String host, String port, String dbName, String dbUser, String dbPasswd, String colName) {
		this.table = new MongoTable(host, port, dbName, dbUser, dbPasswd, colName);
	}

	@Override
	public void close(boolean abort) throws IOException {
		if (table != null)
			table.close();
	}

	@Override
	public void write(Writable w) throws IOException {

		BasicDBObject dbo = new BasicDBObject();
		if (w instanceof BSONWritable ){
			dbo.putAll( ((BSONWritable)w).getDoc() );
		}
		else if ( w instanceof BSONObject){
			dbo.putAll( (BSONObject) w );
		}
		else{
			MapWritable map = (MapWritable) w;
			for (final Map.Entry<Writable, Writable> entry : map.entrySet()) {
				String key = entry.getKey().toString();
				dbo.put(key, MongoUtil.getObjectFromWritable(entry.getValue()));
			}
		}
		table.save(dbo);
	}



}
