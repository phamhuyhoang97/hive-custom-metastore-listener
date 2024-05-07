package com.medium.hive.metastore.listerner;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.DataType;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;


import com.linkedin.common.FabricType;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import org.apache.http.HttpResponse;
import org.json.JSONObject;
import org.json.JSONArray;
import com.google.gson.Gson;

import java.util.Map;
import java.util.HashMap;
import com.linkedin.data.template.StringMap;

import org.apache.hadoop.conf.Configuration;
import com.linkedin.schema.BooleanType;
import com.linkedin.schema.FixedType;
import com.linkedin.schema.StringType;
import com.linkedin.schema.BytesType;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.DateType;
import com.linkedin.schema.TimeType;
import com.linkedin.schema.EnumType;
import com.linkedin.schema.NullType;
import com.linkedin.schema.MapType;
import com.linkedin.schema.ArrayType;
import com.linkedin.schema.UnionType;
import com.linkedin.schema.MySqlDDL;

import com.linkedin.dataset.DatasetFieldProfile;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.DatasetFieldProfileArray;


public class Utils{
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomListener.class);
    private static final  ObjectMapper objMapper = new ObjectMapper();

   /**
    * The function `getDeltaTableSchema` retrieves the schema of a Delta table located at the specified
    * location and returns it as an array of `SchemaField` objects.
    * 
    * @param table_location The `table_location` parameter is the location of the Delta table. It is
    * the path where the Delta table is stored in the file system.
    * @return The method `getDeltaTableSchema` returns a `SchemaFieldArray` object.
    */
    public static SchemaFieldArray getDeltaTableSchema(String table_location){
        DeltaLog log = DeltaLog.forTable(new Configuration(), table_location);

        LOGGER.info("current snapshot: " + log.snapshot().getMetadata().getSchema().getFields()[0].getName()); 
        StructField [] cols = log.snapshot().getMetadata().getSchema().getFields();

        SchemaFieldArray fieldArray = new SchemaFieldArray();
        for (StructField col : cols) {
             
                String name = col.getName();
               
                DataType dataType = col.getDataType();
               
                boolean nullable = col.isNullable();

                LOGGER.info("name: " + name);
                LOGGER.info("data type: " + dataType.getSimpleString());
                LOGGER.info("nullable: " + nullable);


                SchemaField field = new SchemaField()
                    .setFieldPath(name)
                    .setType(mapType(dataType.getSimpleString()))
                    .setNullable(nullable)
                    .setNativeDataType(dataType.getSimpleString());

                fieldArray.add(field);
        }
        return fieldArray;
    }


    /**
     * The function `getDeltaTableProfile` retrieves profile information for a Delta table, including
     * total number of records, minimum and maximum values for each field, and null count and
     * proportion for each field.
     * 
     * @param table_location The `table_location` parameter is the location of the Delta table. It is a
     * string that specifies the path to the Delta table on the file system.
     * @return The method `getDeltaTableProfile` returns a `DatasetProfile` object.
     */
    public static DatasetProfile  getDeltaTableProfile(String table_location){
        
        DeltaLog log = DeltaLog.forTable(new Configuration(), table_location);

        int totalNumRecords = 0;
        JSONObject minValues = new JSONObject();
        JSONObject maxValues = new JSONObject();
        JSONObject nullCount = new JSONObject();

        CloseableIterator<AddFile>  files = log.snapshot().scan().getFiles();
        while (files.hasNext()) {
            AddFile addFile = files.next();
            JSONObject stats = new JSONObject(addFile.getStats());

            totalNumRecords += stats.getInt("numRecords");
            
            JSONObject fileMinValues = stats.getJSONObject("minValues");
            JSONObject fileMaxValues = stats.getJSONObject("maxValues");
            JSONObject fileNullCount = stats.getJSONObject("nullCount");

            LOGGER.info("stats: " + stats);

            for (String key : fileMinValues.keySet()) {
                if (!minValues.has(key)) {
                    minValues.put(key, fileMinValues.get(key));
                } else if (objToStr(fileMinValues.get(key)).compareTo(objToStr(minValues.get(key))) < 0) {
                   minValues.put(key, fileMinValues.get(key));
                }
            }

            
            for (String key : fileMaxValues.keySet()) {
                if (!maxValues.has(key)) {
                    maxValues.put(key, fileMaxValues.get(key));
                } else if (objToStr(fileMaxValues.get(key)).compareTo(objToStr(maxValues.get(key))) > 0) {
                   maxValues.put(key, fileMaxValues.get(key));
                }
            }
            for (String key : fileNullCount.keySet()) {
                if (!nullCount.has(key)) {
                    nullCount.put(key, fileNullCount.getInt(key));
                } else {
                   nullCount.put(key, fileNullCount.getInt(key)  + nullCount.getInt(key));
                }
            }
        }

        DatasetFieldProfileArray profileArray = new DatasetFieldProfileArray();                                                  

        for (String key: minValues.keySet()) {
            DatasetFieldProfile profile = new DatasetFieldProfile()
                                        .setFieldPath(key)
                                        .setMin(objToStr(minValues.get(key)))
                                        .setMax(objToStr(maxValues.get(key)))
                                        .setNullCount(nullCount.getInt(key))
                                        .setNullProportion(nullCount.getInt(key)/totalNumRecords);
            profileArray.add(profile);
        }
        return new DatasetProfile()
                    .setTimestampMillis(System.currentTimeMillis())
                    .setRowCount(totalNumRecords)
                    .setColumnCount(minValues.length())
                    .setFieldProfiles(profileArray);

    }

    /**
     * The function creates a SchemaFieldArray object by iterating through a JSONArray and extracting
     * the name, comment, and type values from each JSONObject in the array.
     * 
     * @param cols The parameter "cols" is a JSONArray that contains a list of columns. Each column is
     * represented as a JSONObject with the following properties:
     * @return The method is returning a SchemaFieldArray object.
     */
    public static SchemaFieldArray createSchemaFieldArray(JSONArray cols){
        SchemaFieldArray fieldArray = new SchemaFieldArray();
        for (int i = 0 ; i < cols.length(); i++){
                JSONObject col = cols.getJSONObject(i);
                String name = "", comment = "", type = "";
                try{
                    name = col.getString("name");
                } catch (Exception e){
                    LOGGER.error("Fail to get name");
                }
                try{
                    comment = col.getString("comment");
                } catch (Exception e){
                    LOGGER.error("Fail to get comment");
                }
                try{
                    type = col.getString("type");
                } catch (Exception e){
                    LOGGER.error("Fail to get type");
                }

                SchemaField field = new SchemaField()
                    .setFieldPath(name)
                    .setDescription(comment)
                    .setType(mapType(type))
                    .setNullable(true)
                    .setNativeDataType(type);
                fieldArray.add(field);
        }
        return fieldArray;
    }

    /**
     * The function `mapType` takes a string input representing a data type and returns the
     * corresponding SchemaFieldDataType object.
     * 
     * @param type The "type" parameter is a string that represents the data type of a field in a
     * schema.
     * @return The method is returning an instance of the `SchemaFieldDataType` class with a specific
     * type based on the input `type` parameter.
     */
    public static SchemaFieldDataType mapType(String type) {

            if (type.equals("voids")) {
                return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NullType()));
            } else if (type.equals("timestamp")) {
            
                return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new TimeType()));
            } else if (type.equals("boolean")){         
                return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BooleanType()));
            } else if (type.equals("tinyint")){ 
            
                return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
            } else if (type.equals("smallint")){ 
            
                return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
            } else if (type.equals("int")){ 
            
                return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
            } else if (type.equals("bigint")){ 
            
                return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
            } else if (type.equals("float")){ 
            
                return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
            } else if (type.equals("double")){ 
            
                return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
            } else if (type.equals("decimal")){ 
            
                return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
            } else if (type.equals("string")){ 
            
                return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType()));
            } else if (type.equals("varchar")){ 
            
                return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType()));
            } else if (type.equals("date")){ 
            
                return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new DateType()));
            } else if (type.startsWith("array")){ 
                return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new ArrayType()));
            } else if (type.startsWith("map")){ 
                return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new ArrayType()));
 
            }else if (type.startsWith("union")){ 
                return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new ArrayType()));
            } else {
                throw new RuntimeException(String.format("Unrecognized SchemaFieldDataType provided: %s", type));
            }

    }

    /**
     * The function converts an object to a JSON string using an object mapper.
     * 
     * @param obj The "obj" parameter is an object that you want to convert to a string representation.
     * @return The method is returning a String representation of the given object.
     */
    public static String objToStr(Object obj){
        try {
            return objMapper.writeValueAsString(obj);
        } catch (Exception e) {
            LOGGER.error("Error on conversion", e);
        }
        return null;
    }
}