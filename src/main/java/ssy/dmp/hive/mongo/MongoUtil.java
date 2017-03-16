package ssy.dmp.hive.mongo;

import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.*;
import org.bson.BasicBSONObject;

import java.util.Map;

/**
 * Created by guoyanlei
 * date：2017/3/16
 * time：11:58
 */
public class MongoUtil {

    public static Object getObjectFromWritable(Writable w) {

        if (w instanceof Text) {
            return w.toString();
        }
        if (w instanceof BSONWritable) {
            return ((BSONWritable) w).getDoc();
        }

        if (w instanceof IntWritable) {
            // int
            return ((IntWritable) w).get();
        } else if (w instanceof ShortWritable) {
            // short
            return ((ShortWritable) w).get();
        } else if (w instanceof ByteWritable) {
            // byte
            return ((ByteWritable) w).get();
        } else if (w instanceof BooleanWritable) {
            // boolean
            return ((BooleanWritable) w).get();
        } else if (w instanceof LongWritable) {
            // long
            return ((LongWritable) w).get();
        } else if (w instanceof FloatWritable) {
            // float
            return ((FloatWritable) w).get();
        } else if (w instanceof org.apache.hadoop.hive.serde2.io.DoubleWritable) {
            // double
            return ((org.apache.hadoop.hive.serde2.io.DoubleWritable) w).get();
        } else if (w instanceof NullWritable) {
            //null
            return null;
        } else if (w instanceof ArrayWritable) {
            Writable[] o = ((ArrayWritable) w).get();
            Object[] a = new Object[o.length];
            for (int i = 0; i < o.length; i++) {
                a[i] = toBSON(o[i]);
            }
            return a;
        } else {
            // treat as string
            return w.toString();
        }

    }

    public static Object toBSON(final Object x) {
        if (x == null) {
            return null;
        }
        if (x instanceof Text) {
            return x.toString();
        }
        if (x instanceof BSONWritable) {
            return ((BSONWritable) x).getDoc();
        }
        if (x instanceof Writable) {
            if (x instanceof AbstractMapWritable) {
                if (!(x instanceof Map)) {
                    throw new IllegalArgumentException(
                            String.format("Cannot turn %s into BSON, since it does "
                                            + "not implement java.util.Map.",
                                    x.getClass().getName()));
                }
                Map<Writable, Writable> map = (Map<Writable, Writable>) x;
                BasicBSONObject bson = new BasicBSONObject();
                for (Map.Entry<Writable, Writable> entry : map.entrySet()) {
                    bson.put(entry.getKey().toString(),
                            toBSON(entry.getValue()));
                }
                return bson;
            }
            if (x instanceof ArrayWritable) {
                Writable[] o = ((ArrayWritable) x).get();
                Object[] a = new Object[o.length];
                for (int i = 0; i < o.length; i++) {
                    a[i] = toBSON(o[i]);
                }
                return a;
            }
            if (x instanceof NullWritable) {
                return null;
            }
            if (x instanceof BooleanWritable) {
                return ((BooleanWritable) x).get();
            }
            if (x instanceof BytesWritable) {
                return ((BytesWritable) x).getBytes();
            }
            if (x instanceof org.apache.hadoop.io.ByteWritable) {
                return ((org.apache.hadoop.io.ByteWritable) x).get();
            }
            if (x instanceof org.apache.hadoop.io.DoubleWritable) {
                return ((org.apache.hadoop.io.DoubleWritable) x).get();
            }
            if (x instanceof FloatWritable) {
                return ((FloatWritable) x).get();
            }
            if (x instanceof LongWritable) {
                return ((LongWritable) x).get();
            }
            if (x instanceof IntWritable) {
                return ((IntWritable) x).get();
            }

            // TODO - Support counters

        }
        return x;
    }
}
