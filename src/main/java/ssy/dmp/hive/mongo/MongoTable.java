package ssy.dmp.hive.mongo;

import java.util.Arrays;

import com.mongodb.*;

public class MongoTable {
    private MongoClient mongoClient;
    private DB db;
    private DBCollection collection;

    public MongoTable(String host, String port, String dbName, String dbUser, String dbPasswd,
                      String collectionName) {
        try {

            if (dbUser == null || "".equals(dbUser.trim())){
                throw new RuntimeException("user and password can not be null !!");
            }
            //创建个 credential对象
            MongoCredential credential = MongoCredential.createCredential(dbUser, dbName, dbPasswd.toCharArray());
            //创建一个链接地址
            ServerAddress serverAddress = new ServerAddress(host,Integer.valueOf(port));

            this.mongoClient = new MongoClient(serverAddress, Arrays.asList(credential));
            this.db = this.mongoClient.getDB(dbName);
            this.collection = this.db.getCollection(collectionName);

        } catch (MongoException e) {
            e.printStackTrace();
        }
    }

    public void save(BasicDBObject dbo) {
        this.collection.save(dbo);
    }

    public void close() {
        if (db != null) {
            db.getMongo().close();
        }
        if (mongoClient != null){
            mongoClient.close();
        }
    }

    public long count() {
        return (this.collection != null) ? this.collection.count() : 0;
    }

    public DBCursor findAll(String[] fields) {
        DBObject qFields = new BasicDBObject();
        for (String field : fields) {
            qFields.put(field, 1);
        }

        return this.collection.find(new BasicDBObject(), qFields);
    }

    public void drop() {
        this.collection.drop();
    }

}
