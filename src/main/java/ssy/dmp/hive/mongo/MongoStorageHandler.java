package ssy.dmp.hive.mongo;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class MongoStorageHandler implements HiveStorageHandler {
	private Configuration mConf = null;

	public MongoStorageHandler() {
	}

	@Override
	public void configureTableJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		Properties properties = tableDesc.getProperties();
		ConfigurationUtil.copyMongoProperties(properties, jobProperties);
	}

	@Override
	public Class<? extends InputFormat> getInputFormatClass() {
		return MongoInputFormat.class;
	}

	@Override
	public HiveMetaHook getMetaHook() {
		return new DummyMetaHook();
	}

	@Override
	public Class<? extends OutputFormat> getOutputFormatClass() {
		return MongoOutputFormat.class;
	}

	@Override
	public Class<? extends SerDe> getSerDeClass() {
		return MongoSerDe.class;
	}

	@Override
	public Configuration getConf() {
		return this.mConf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.mConf = conf;
	}

	public void configureJobConf(TableDesc tableDesc, JobConf jobConf){
	}

	public void configureInputJobProperties(TableDesc desc, Map<String,String> props){
	}

	public void configureOutputJobProperties(TableDesc desc, Map<String,String> props){

	}

	public HiveAuthorizationProvider getAuthorizationProvider()
    throws HiveException{
		return null;
	}

	private class DummyMetaHook implements HiveMetaHook {

		@Override
		public void commitCreateTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void commitDropTable(Table tbl, boolean deleteData)
				throws MetaException {
			boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
			if (deleteData && isExternal) {
				// nothing to do...
			} else if(deleteData && !isExternal) {
				String dbHost = tbl.getParameters().get(ConfigurationUtil.DB_HOST);
				String dbPort = tbl.getParameters().get(ConfigurationUtil.DB_PORT);
				String dbName = tbl.getParameters().get(ConfigurationUtil.DB_NAME);
				String dbUser = tbl.getParameters().get(ConfigurationUtil.DB_USER);
				String dbPasswd = tbl.getParameters().get(ConfigurationUtil.DB_PASSWD);
				String dbCollection = tbl.getParameters().get(ConfigurationUtil.COLLECTION_NAME);
				MongoTable table = new MongoTable(dbHost, dbPort, dbName, dbUser, dbPasswd,
						dbCollection);
				table.drop();
				table.close();
			}
		}

		@Override
		public void preCreateTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void preDropTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void rollbackCreateTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void rollbackDropTable(Table tbl) throws MetaException {
			// nothing to do...
		}

	}
}
