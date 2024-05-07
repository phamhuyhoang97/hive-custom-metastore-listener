package com.medium.hive.metastore.listerner;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.events.AddForeignKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddNotNullConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddPrimaryKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.AddUniqueConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.AlterISchemaEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.CreateCatalogEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateISchemaEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropCatalogEvent;
import org.apache.hadoop.hive.metastore.events.DropConstraintEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropISchemaEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.apache.hadoop.hive.metastore.events.OpenTxnEvent;
import org.apache.hadoop.hive.metastore.events.CommitTxnEvent;
import org.apache.hadoop.hive.metastore.events.AbortTxnEvent;
import org.apache.hadoop.hive.metastore.events.AllocWriteIdEvent;


import org.codehaus.jackson.map.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.common.FabricType;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;


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


import com.linkedin.events.metadata.ChangeType;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.client.rest.RestEmitter;
import datahub.client.Callback;
import datahub.client.MetadataResponseFuture;
import datahub.client.MetadataWriteResponse;

import org.apache.hadoop.conf.Configuration;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.DataType;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;

import java.util.concurrent.Future;
import org.apache.http.HttpResponse;
import org.json.JSONObject;
import org.json.JSONArray;
import com.google.gson.Gson;

import java.util.Map;
import java.util.HashMap;
import com.linkedin.data.template.StringMap;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;

import com.linkedin.dataset.DatasetFieldProfile;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.DatasetFieldProfileArray;

import com.medium.hive.metastore.listerner.Utils;

public class CustomListener extends MetaStoreEventListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomListener.class);
    private static final  ObjectMapper objMapper = new ObjectMapper();

    public CustomListener(Configuration config) {
        super(config);
    }

    @Override
    public void onCreateTable(CreateTableEvent event) {
        LOGGER.info("On create table");
        handleTableEvent(event.getTable());
    }

    @Override
    public void onAlterTable(AlterTableEvent event) {
        LOGGER.info("On alter table");

        // handleTableEvent(event.getOldTable());
        handleTableEvent(event.getNewTable());
    }
    
    @Override
    public void onInsert(InsertEvent event) {
        LOGGER.info("On insert event");

        LOGGER.info("[CustomListener][Thread: " + Thread.currentThread().getName()+"] | " + Utils.objToStr(event.getTableObj()));
    }

    /**
     * The `handleTableEvent` function in Java retrieves various properties from a JSON object representing
     * a table, constructs a URN string, and emits metadata change proposals for the dataset and schema
     * using a REST emitter.
     * 
     * @param obj The `obj` parameter is an object that contains information about a table. It is expected
     * to be in JSON format.
     */
    private void handleTableEvent(Object obj){

        LOGGER.info("[CustomListener][Thread: " + Thread.currentThread().getName()+"] | " + Utils.objToStr(obj));

        RestEmitter emitter = RestEmitter.create(b -> b.server(System.getenv("DATAHUB_GMS")));

        String jsonInString = new Gson().toJson(obj);

        int createTime = 0; 
        String tableName = "", dbName = "", spark_version = "", spark_partition_provider = "", spark_source_provider = "",spark_schema = "", table_location = "", table_type = "", total_size = "", transient_lastDdlTime = "", numFiles = "";
        JSONArray cols = new JSONArray();
        
      
        JSONObject table = new JSONObject(jsonInString);

       // Retrieving various properties from a JSON object named "table". It is using
       // try-catch blocks to handle any exceptions that may occur during the retrieval process. The
       // properties being retrieved include tableName, dbName, createTime, spark_version,
       // spark_partition_provider, spark_source_provider, spark_schema, table_location, table_type,
       // total_size, transient_lastDdlTime, numFiles, and cols. The retrieved values are then used for
       // further processing or logging.
        try {
            tableName = table.getString("tableName");
            dbName = table.getString("dbName");
        }
        catch (Exception e){
            LOGGER.error("Failed to retrieve table from JSON: " + e);

        }
        try {
             createTime = table.getInt("createTime");
        } catch (Exception e) {
            LOGGER.error("Failed to retrieve 'createTime' from JSON: " + e);
        }

        try {
             spark_version = table.getJSONObject("parameters").getString("spark.sql.create.version");
        } catch (Exception e) {
            LOGGER.error("Failed to retrieve 'spark.sql.create.version' from JSON: " + e);
        }

        try {
             spark_partition_provider = table.getJSONObject("parameters").getString("spark.sql.partitionProvider");
        } catch (Exception e) {
            LOGGER.error("Failed to retrieve 'spark.sql.partitionProvider' from JSON: " + e);
        }

        try {
             spark_source_provider = table.getJSONObject("parameters").getString("spark.sql.sources.provider");
        } catch (Exception e) {
            LOGGER.error("Failed to retrieve 'spark.sql.sources.provider' from JSON: " + e);
        }

        try {
             spark_schema = table.getJSONObject("parameters").getString("spark.sql.sources.schema");
        } catch (Exception e) {
            LOGGER.error("Failed to retrieve 'spark.sql.sources.provider' from JSON: " + e);
        }

        try {
             table_location = table.getJSONObject("sd").getString("location") ;
        } catch (Exception e) {
            table_location = table.getJSONObject("sd").getJSONObject("serdeInfo").getJSONObject("parameters").getString("path") ;
            LOGGER.error(" 'location' from JSON: " + table_location);
        }

        try {
             table_type = table.getString("tableType");
        } catch (Exception e) {
            LOGGER.error("Failed to retrieve 'tableType' from JSON: " + e);
        }

        try {
             total_size = table.getJSONObject("parameters").getString("totalSize");
        } catch (Exception e) {
            LOGGER.error("Failed to retrieve 'totalSize' from JSON: " + e);
        }

        try {
             transient_lastDdlTime = table.getJSONObject("parameters").getString("transient_lastDdlTime");
        } catch (Exception e) {
            LOGGER.error("Failed to retrieve 'transient_lastDdlTime' from JSON: " + e);
        }

        try {
             numFiles = table.getJSONObject("parameters").getString("numFiles");
        } catch (Exception e) {
            LOGGER.error("Failed to retrieve 'numFiles' from JSON: " + e);
        }
        try {
            cols = table.getJSONObject("sd").getJSONArray("cols");
        } catch (Exception e){
            LOGGER.error("Failed to retrieve 'cols' from JSON: " + e);
        }
        table_location = table_location.replace("-__PLACEHOLDER__", "");


        // Creating a StringMap object called "input" and populating it with various
        // key-value pairs. These key-value pairs represent different properties related to a dataset. The code
        // then constructs a URN (Uniform Resource Name) string using the values from the "dbName" and
        // "tableName" variables, and assigns it to the "urn" variable.
        StringMap input = new StringMap();

        input.put("numFiles", numFiles);
        input.put("spark.sql.create.version", spark_version);
        input.put("spark.sql.partitionProvider", spark_partition_provider);
        input.put("spark.sql.sources.provider", spark_source_provider);
        input.put("spark.sql.sources.schema", spark_schema); // Replace with the appropriate value
        input.put("table_location", table_location);
        input.put("table_type", table_type);
        input.put("totalSize", total_size);
        input.put("transient_lastDdlTime", transient_lastDdlTime);

        String urn = "urn:li:dataset:(urn:li:dataPlatform:hive," + dbName + "." + tableName+",PROD)";

        // Checking the value of the variable `spark_source_provider`. If it is equal
        // to "delta", it tries to get the schema and profile of a delta table using the `Utils` class.
        // If successful, it creates a `MetadataChangeProposalWrapper` object with the entity type,
        // entity URN, and profile aspect, and emits it using the `emitter` object. If
        // `spark_source_provider` is not equal to "delta", it creates a schema field array using the
        // `Utils` class.
        SchemaFieldArray fieldArray = null;
        DatasetProfile profile = null;
        if(spark_source_provider.equalsIgnoreCase("delta")) {
            try{
                fieldArray =  Utils.getDeltaTableSchema(table_location);
            } catch (Exception e){
                LOGGER.error("Fail to get schema of delta table: "+ e);
            }

            try{
                profile = Utils.getDeltaTableProfile(table_location);
            } catch (Exception e){
                LOGGER.error("Fail to get schema of delta table: "+ e);
            }


            MetadataChangeProposalWrapper<?> mcpw_profile = MetadataChangeProposalWrapper.builder()
            .entityType("dataset")
            .entityUrn(urn)
            .upsert()
            .aspect(profile)
            .build();
            try{
                emitter.emit(mcpw_profile, null).get();
            }
                catch (Exception e){
                LOGGER.error("Fail to emit profilling metadata:", e);
            }

        }

        else{
            fieldArray = Utils.createSchemaFieldArray(cols);
        }



       
        // Creating a metadata change proposal for a dataset. It sets the entity type to
        // "dataset" and the entity URN to the provided URN. It specifies that it wants to upsert (update or
        // insert) the metadata. It then sets the dataset properties, including the name, description, and
        // custom properties. Finally, it builds the metadata change proposal.
        MetadataChangeProposalWrapper<?> mcpw_prop = MetadataChangeProposalWrapper.builder()
        .entityType("dataset")
        .entityUrn(urn)
        .upsert()
        .aspect(new DatasetProperties()
            .setName(tableName)
            .setDescription("Sent from REST emitter")
            .setCustomProperties(input)
        )
        .build();


        // Creating a MetadataChangeProposalWrapper object in Java. This object is used to
        // propose changes to the metadata of a dataset.
        MetadataChangeProposalWrapper<?> mcpw_schema = MetadataChangeProposalWrapper.builder()
        .entityType("dataset")
        .entityUrn(urn)
        .upsert()
        .aspect(new SchemaMetadata()
            // .setAspectVersion(0L)
            .setPlatform(new DataPlatformUrn("hive"))
            .setDataset(new DatasetUrn(new DataPlatformUrn("hive"), dbName + "." + tableName, FabricType.PROD))
            .setVersion(10L)
            .setHash("")
            .setPlatformSchema(SchemaMetadata.PlatformSchema.create(new MySqlDDL().setTableSchema("foo")))
            .setSchemaName("Schema")
            .setFields(fieldArray)
        )
        .build();

        try{
            emitter.emit(mcpw_prop, null).get();
        }
        catch (Exception e){
            LOGGER.error("Fail to emit prop metadata:", e);
        }

        try{
            emitter.emit(mcpw_schema, null).get();
            LOGGER.info("Emitted schema metadata");
        }
        catch (Exception e){
            LOGGER.error("Fail to emit schema metadata:", e);
        }
    }
      
    @Override
    public void onConfigChange(ConfigChangeEvent tableEvent) {
        LOGGER.info("On config change");
    }


    public void onDropTable(DropTableEvent tableEvent) {
        LOGGER.info("On drop table");
    }


    public void onAddPartition(AddPartitionEvent partitionEvent) {
        LOGGER.info("On add partition");
    }


    public void onDropPartition(DropPartitionEvent partitionEvent) {
        LOGGER.info("On drop partition");
    }


    public void onAlterPartition(AlterPartitionEvent partitionEvent) {
        LOGGER.info("On alter partition");
    }


    public void onCreateDatabase(CreateDatabaseEvent dbEvent) {
        LOGGER.info("On create database");
    }


    public void onDropDatabase(DropDatabaseEvent dbEvent) {
        LOGGER.info("On drop database");
    }

    public void onAlterDatabase(AlterDatabaseEvent dbEvent) {
        LOGGER.info("On alter database");
    }


    public void onLoadPartitionDone(LoadPartitionDoneEvent partSetDoneEvent) {
        LOGGER.info("On load partition done");
    }


    public void onCreateFunction(CreateFunctionEvent fnEvent) {
        LOGGER.info("On create function");
    }


    public void onDropFunction(DropFunctionEvent fnEvent) {
        LOGGER.info("On drop function");
    }


    public void onAddPrimaryKey(AddPrimaryKeyEvent addPrimaryKeyEvent) {
        LOGGER.info("On add primary key");
    }


    public void onAddForeignKey(AddForeignKeyEvent addForeignKeyEvent) {
        LOGGER.info("On add foreign key");
    }


    public void onAddUniqueConstraint(AddUniqueConstraintEvent addUniqueConstraintEvent) {
        LOGGER.info("On add unique constraint");
    }


    public void onAddNotNullConstraint(AddNotNullConstraintEvent addNotNullConstraintEvent) {
        LOGGER.info("On add not null constraint");
    }


    public void onDropConstraint(DropConstraintEvent dropConstraintEvent) {
        LOGGER.info("On drop constraint");
    }


    public void onCreateISchema(CreateISchemaEvent createISchemaEvent) {
        LOGGER.info("On create iSchema");
    }


    public void onAlterISchema(AlterISchemaEvent alterISchemaEvent) {
        LOGGER.info("On alter iSchema");
    }


    public void onDropISchema(DropISchemaEvent dropISchemaEvent) {
        LOGGER.info("On drop iSchema");
    }


    public void onAddSchemaVersion(AddSchemaVersionEvent addSchemaVersionEvent) {
        LOGGER.info("On add schema version");
    }


    public void onAlterSchemaVersion(AlterSchemaVersionEvent alterSchemaVersionEvent) {
        LOGGER.info("On alter schema version");
    }


    public void onDropSchemaVersion(DropSchemaVersionEvent dropSchemaVersionEvent) {
        LOGGER.info("On drop schema version");
    }


    public void onCreateCatalog(CreateCatalogEvent createCatalogEvent) {
        LOGGER.info("On create catalog");
    }


    public void onDropCatalog(DropCatalogEvent dropCatalogEvent) {
        LOGGER.info("On drop catalog");
    }


  
}

