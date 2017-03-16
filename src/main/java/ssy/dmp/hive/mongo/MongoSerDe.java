package ssy.dmp.hive.mongo;

import java.sql.Timestamp;
import java.util.*;

import com.alibaba.fastjson.JSONArray;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;

public class MongoSerDe extends AbstractSerDe {

    static final String HIVE_TYPE_DOUBLE = "double";
    static final String HIVE_TYPE_FLOAT = "float";
    static final String HIVE_TYPE_BOOLEAN = "boolean";
    static final String HIVE_TYPE_BIGINT = "bigint";
    static final String HIVE_TYPE_TINYINT = "tinyint";
    static final String HIVE_TYPE_SMALLINT = "smallint";
    static final String HIVE_TYPE_INT = "int";
    static final String HIVE_TYPE_LIST = "array";

    private final MapWritable cachedWritable = new MapWritable();

    /**
     * # of columns in the Hive table
     */
    private int fieldCount;
    /**
     * An ObjectInspector which contains metadata
     * about rows
     */
    private StructObjectInspector objectInspector;
    /**
     * Column names in the Hive table
     */
    private List<String> columnNames;
    private List<TypeInfo> columnTypes;
    private List<Object> row;

    public Map<String, String> hiveToMongo;

    private static final int BSON_NUM = 8;
    private static final String OID = "oid";
    private static final String BSON_TYPE = "bsontype";

    Logger log = Logger.getLogger(MongoSerDe.class);

    @Override
    public void initialize(final Configuration conf, final Properties tbl) throws SerDeException {

        final String columnString = tbl.getProperty(ConfigurationUtil.COLUMN_MAPPING);
        if (StringUtils.isBlank(columnString)) {
            throw new SerDeException("No column mapping found, use "
                    + ConfigurationUtil.COLUMN_MAPPING);
        }
        final String[] columnNamesArray = ConfigurationUtil.getAllColumns(columnString);

        fieldCount = columnNamesArray.length;
        columnNames = new ArrayList<String>(columnNamesArray.length);
        columnNames.addAll(Arrays.asList(columnNamesArray));

        log.debug("column names in mongo collection: " + columnNames);

        String hiveColumnNameProperty = tbl.getProperty(Constants.LIST_COLUMNS);
        List<String> hiveColumnNameArray = new ArrayList<String>();

        if (hiveColumnNameProperty != null && hiveColumnNameProperty.length() > 0) {
            hiveColumnNameArray = Arrays.asList(hiveColumnNameProperty.split("[,:;]"));
        }
        log.debug("column names in hive table: " + hiveColumnNameArray);

        String columnTypeProperty = tbl
                .getProperty(Constants.LIST_COLUMN_TYPES);
        // System.err.println("column types:" + columnTypeProperty);
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        log.debug("column types in hive table: " + columnTypes);

        final List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(
                columnNamesArray.length);

        ObjectInspector inspector;
        for (int i = 0; i < columnNamesArray.length; i++) {
            inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo( columnTypes.get(i) );
            fieldOIs.add(inspector);
        }
        objectInspector = ObjectInspectorFactory
                .getStandardStructObjectInspector(hiveColumnNameArray, fieldOIs);
        row = new ArrayList<Object>(columnNamesArray.length);
    }

    /**
     * 从mongodb中读数据，并反序列化
     * TODO 待实现
     */
    @Override
    public Object deserialize(Writable wr) throws SerDeException {
        if (!(wr instanceof MapWritable)) {
            throw new SerDeException("Expected MapWritable, received "
                    + wr.getClass().getName());
        }

        final MapWritable input = (MapWritable) wr;
        final Text t = new Text();
        row.clear();

        for (int i = 0; i < fieldCount; i++) {
            t.set(columnNames.get(i));
            TypeInfo typeInfo = columnTypes.get(i);
            final Writable value = input.get(t);
            if (value != null && !NullWritable.get().equals(value)) {
                //parse as double to avoid NumberFormatException...
                //TODO:need more test,especially for type 'bigint'
                if (HIVE_TYPE_INT.equalsIgnoreCase(typeInfo.getTypeName())) {
                    row.add(Double.valueOf(value.toString()).intValue());
                } else if (MongoSerDe.HIVE_TYPE_SMALLINT.equalsIgnoreCase(typeInfo.getTypeName())) {
                    row.add(Double.valueOf(value.toString()).shortValue());
                } else if (MongoSerDe.HIVE_TYPE_TINYINT.equalsIgnoreCase(typeInfo.getTypeName())) {
                    row.add(Double.valueOf(value.toString()).byteValue());
                } else if (MongoSerDe.HIVE_TYPE_BIGINT.equalsIgnoreCase(typeInfo.getTypeName())) {
                    row.add(Long.valueOf(value.toString()));
                } else if (MongoSerDe.HIVE_TYPE_BOOLEAN.equalsIgnoreCase(typeInfo.getTypeName())) {
                    row.add(Boolean.valueOf(value.toString()));
                } else if (MongoSerDe.HIVE_TYPE_FLOAT.equalsIgnoreCase(typeInfo.getTypeName())) {
                    row.add(Double.valueOf(value.toString()).floatValue());
                } else if (MongoSerDe.HIVE_TYPE_DOUBLE.equalsIgnoreCase(typeInfo.getTypeName())) {
                    row.add(Double.valueOf(value.toString()));
                } else if (MongoSerDe.HIVE_TYPE_LIST.equalsIgnoreCase(typeInfo.getTypeName())){

                    JSONArray array = JSONArray.parseArray(value.toString());
                    log.info("array: " + array.toJSONString());
                    Object[] arr = new Object[array.size()];
                    for (int a = 0; a < arr.length; a++) {
                        arr[a] = array.get(a);
                    }
                    row.add(arr);
                } else{
                    row.add(value.toString());
                }
            } else {
                row.add(null);
            }
        }

        return row;

    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return BSONWritable.class;
    }

    /**
     * 从将Hive中的数据序列化到Mongodb中
     */
    @Override
    public Writable serialize(final Object obj, final ObjectInspector inspector)
            throws SerDeException {
        return new BSONWritable((BSONObject) serializeStruct(obj, (StructObjectInspector) inspector, ""));
    }

    /**
     * Turn struct obj into a BasicBSONObject
     */
    private Object serializeStruct(Object obj,
                                   StructObjectInspector structOI,
                                   String ext) {
        if (ext.length() > 0 && isObjectIdStruct(obj, structOI)) {

            String objectIdString = "";
            for (StructField s : structOI.getAllStructFieldRefs()) {
                if (s.getFieldName().equals(OID)) {
                    objectIdString = structOI.getStructFieldData(obj, s).toString();
                    break;
                }
            }
            return new ObjectId(objectIdString);
        } else {

            BSONObject bsonObject = new BasicBSONObject();
            // fields is the list of all variable names and information within the struct obj
            List<? extends StructField> fields = structOI.getAllStructFieldRefs();

            for (int i = 0 ; i < fields.size() ; i++) {
                StructField field = fields.get(i);

                String fieldName, hiveMapping;

                // get corresponding mongoDB field
                if (ext.length() == 0) {
                    fieldName = this.columnNames.get(i);
                    hiveMapping = fieldName;
                } else {
                    fieldName = field.getFieldName();
                    hiveMapping = (ext + "." + fieldName);
                }

                ObjectInspector fieldOI = field.getFieldObjectInspector();
                Object fieldObj = structOI.getStructFieldData(obj, field);

                if (this.hiveToMongo != null && this.hiveToMongo.containsKey(hiveMapping)) {
                    String mongoMapping = this.hiveToMongo.get(hiveMapping);
                    int lastDotPos = mongoMapping.lastIndexOf(".");
                    String lastMapping = lastDotPos == -1 ? mongoMapping :  mongoMapping.substring(lastDotPos+1);
                    bsonObject.put(lastMapping,
                            serializeObject(fieldObj, fieldOI, hiveMapping));
                } else {
                    bsonObject.put(fieldName,
                            serializeObject(fieldObj, fieldOI, hiveMapping));
                }
            }

            return bsonObject;
        }
    }

    /**
     *
     * Given a struct, look to se if it contains the fields that a ObjectId
     * struct should contain
     */
    private boolean isObjectIdStruct(Object obj, StructObjectInspector structOI) {
        List<? extends StructField> fields = structOI.getAllStructFieldRefs();

        // If the struct are of incorrect size, then there's no need to create
        // a list of names
        if (fields.size() != 2) {
            return false;
        }
        boolean hasOID = false;
        boolean isBSONType = false;
        for (StructField s : fields) {
            String fieldName = s.getFieldName();
            if (fieldName.equals(this.OID)) {
                hasOID = true;
            } else if (fieldName.equals(this.BSON_TYPE)) {
                String num = structOI.getStructFieldData(obj, s).toString();
                isBSONType = (Integer.parseInt(num) == this.BSON_NUM);
            }

        }
        return hasOID && isBSONType;
    }

    public Object serializeObject(Object obj, ObjectInspector oi, String ext) {
        switch (oi.getCategory()) {
            case LIST:
                return serializeList(obj, (ListObjectInspector) oi, ext);
            case MAP:
                return serializeMap(obj, (MapObjectInspector) oi, ext);
            case PRIMITIVE:
                return serializePrimitive(obj, (PrimitiveObjectInspector) oi);
            case STRUCT:
                return serializeStruct(obj, (StructObjectInspector) oi, ext);
            case UNION:
            default:
                log.error("Cannot serialize " + obj.toString() + " of type " + obj.toString());
                break;
        }
        return null;
    }

    /**
     * For a map of <String, Object> convert to an embedded document
     */
    private Object serializeMap(Object obj, MapObjectInspector mapOI, String ext) {
        BasicBSONObject bsonObject = new BasicBSONObject();
        ObjectInspector mapValOI = mapOI.getMapValueObjectInspector();

        // Each value is guaranteed to be of the same type
        for (Map.Entry<?, ?> entry : mapOI.getMap(obj).entrySet()) {
            String field = entry.getKey().toString();
            Object value = serializeObject(entry.getValue(), mapValOI, ext);
            bsonObject.put(field, value);
        }
        return bsonObject;
    }

    /**
     * For primitive types, depending on the primitive type,
     * cast it to types that Mongo supports
     */
    private Object serializePrimitive(Object obj, PrimitiveObjectInspector oi) {
        switch (oi.getPrimitiveCategory()) {
            case TIMESTAMP:
                Timestamp ts = (Timestamp) oi.getPrimitiveJavaObject(obj);
                return new Date(ts.getTime());
            default:
                return oi.getPrimitiveJavaObject(obj);
        }
    }

    private Object serializeList(Object obj, ListObjectInspector oi, String ext) {
        BasicBSONList list = new BasicBSONList();
        List<?> field = oi.getList(obj);
        ObjectInspector elemOI = oi.getListElementObjectInspector();

        for (Object elem : field) {
            list.add(serializeObject(elem, elemOI, ext));
        }

        return list;
    }

    public SerDeStats getSerDeStats() {
        // TODO Auto-generated method stub
        return null;
    }

}
