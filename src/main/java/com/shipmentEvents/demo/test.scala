package com.telstraicm

/**
 * @author Yong Yuan, yong.yuan@team.telstra.com
 */

//import scala.math.random

import org.apache.directory.api.util.ByteBuffer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}
//import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.SparkFiles
import java.util.Properties
import org.postgresql.Driver
import org.apache.spark.sql.streaming.Trigger
import java.time.Instant
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement


import scala.io.Source

import java.nio.charset.StandardCharsets

import com.amazonaws.services.kms.{AWSKMS, AWSKMSClientBuilder}
import com.amazonaws.services.kms.model.DecryptRequest
import java.nio.ByteBuffer
import com.google.common.io.BaseEncoding


object App {

  def decrypt(base64EncodedValue: String): String = {

    val kmsClient: AWSKMS = AWSKMSClientBuilder.standard.build

    val textDecoded: ByteBuffer = ByteBuffer.wrap(BaseEncoding.base64().decode(base64EncodedValue))

    val req : DecryptRequest = new DecryptRequest().withCiphertextBlob(textDecoded)
    val plainText : ByteBuffer = kmsClient.decrypt(req).getPlaintext

    val printable = StandardCharsets.UTF_8.decode(plainText).toString

    return printable
  }



  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("IN-SESSION-ROE-ICM-NEWSCHEMA")
      .getOrCreate()

    import spark.sqlContext.implicits._

    spark.catalog.clearCache()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setCheckpointDir("/home/hadoop/checkpoint")

    System.gc()

    var trust_store_password = decrypt(System.getenv("tspass"))
    val opmode = System.getenv("opmode")

    //It is to make the checkpoint code environment independent.
    val s3bucket = System.getenv("s3bucket")
    val s3folder = System.getenv("s3folder")
    val s3bakfile_ins = s3bucket + s3folder + "/in_session_table"

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "pudc-ngkafka-001.networks.in.telstra.com.au:6667")
      .option("subscribe", "cmnp-api-iot-radius-filtered")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.ssl.truststore.location", "/home/hadoop/cacerts")
      .option("kafka.ssl.truststore.password", trust_store_password)
      .option("kafka.ssl.truststore.type", "JKS")
      .option("kafka.sasl.kerberos.service.name", "kafka")
      .option("kafka.sasl.mechanism", "GSSAPI")
      .option("groupIdPrefix","IN_SESSION")
      .option("failOnDataLoss", false)
      .load()

    df.printSchema()


    val schema = new StructType()
      .add("capture_ts", StringType)
      .add("message_type", StringType)
      .add("client_ipv4", StringType)
      .add("apn", StringType)
      .add("imsi", StringType)

    val simservice = df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("data"))
      .select("data.*")

    val dbname = decrypt(System.getenv("dbname"))
    val dbport = decrypt(System.getenv("dbport"))

    val dbuser_write = decrypt(System.getenv("dbuser_write"))
    val dbpass_write = decrypt(System.getenv("dbpass_write"))
    val dbhost_write = decrypt(System.getenv("dbhost_write"))

    var url = "jdbc:postgresql://"+dbhost_write+":"+dbport+"/"+dbname
    var connectionProperties = new Properties()
    connectionProperties.put("driver","org.postgresql.Driver")
    connectionProperties.put("user",dbuser_write)
    connectionProperties.put("password",dbpass_write)

    var con = DriverManager.getConnection(url, dbuser_write, dbpass_write)
    try{
      val st = con.createStatement()

      var in_session_df = simservice
        .selectExpr("cast(imsi as long) imsi", "cast(client_ipv4 as string) client_ip_addr", "cast(apn as string) apn", "cast(message_type as string) message_type", "cast(capture_ts as string) eventTime")

      //The conversion of timestamp must be very careful here.
      //The timestamp in radius data has the following format. "30/Jul/20 09:02:07.251960961 AM"
      //It is UTC time. It has the accuracy to nanoseconds. It has AM/PM.
      //Scala Spark to_timestamp convertion only supports timestamp to second, so as to Postgresql. It is the reason the timestamp is converted as below.
      in_session_df = in_session_df.withColumn("eventTime", to_timestamp(concat(substring(in_session_df("eventTime"), 1, 18), substring(in_session_df("eventTime"), 29, 31)) , "dd/MMM/yy hh:mm:ss aa"))


      val in_session_df_filtered = in_session_df.filter(in_session_df("message_type")==="InterimUpdate" || in_session_df("message_type")==="Stop" || in_session_df("message_type")==="Start")

      val in_session_df_filtered_withwm = in_session_df_filtered.withWatermark("eventTime", "4 minutes")

      var batchDF_group = SparkSession.builder().getOrCreate().emptyDataFrame

      val in_session_stream = in_session_df_filtered_withwm.writeStream
        .trigger(Trigger.ProcessingTime("120 seconds"))
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          if(!batchDF.isEmpty)
          {
            batchDF.persist()
            batchDF.write.mode(SaveMode.Overwrite).saveAsTable("insessiontable")
            spark.catalog.refreshTable("insessiontable")

            //For each microbatch, the first step is to populate the staging.in_session table based on the obtained radius data packets.
            spark.sql("select imsi, first(client_ip_addr) AS ip_address, first(apn) AS apn, MAX(CASE WHEN message_type='Start' THEN eventTime ELSE NULL END) AS start_time, MAX(CASE WHEN message_type='Stop' THEN eventTime ELSE NULL END) AS stop_time, MAX(CASE WHEN message_type='InterimUpdate' THEN eventTime ELSE NULL END) AS update_time from insessiontable group by imsi")
              .write
              .mode(SaveMode.Append)
              .jdbc(url, "staging.in_session", connectionProperties)


            val query_in_session = Sql_Statements.sql_in_session
            val query_truncate_staging_table = "truncate table staging.in_session"

            //As the second step, the sims.in_session table will be populated according to the ingestion sql.
            st.executeUpdate(query_in_session)

            //As the third step, the  staging table will be truncated to prepare for the next microbatch.
            st.executeUpdate(query_truncate_staging_table)

            //S3 checkpointing for spark job, which is also used as the heartbeat signal for monitoring and alarming, since EMR does not have healthcheck probe as ECS. The design is documented in the Continous Integration of Spark Jobs as the confluence page. https://confluence.tools.telstra.com/display/TISM/Spark+Design+Overview
            batchDF.coalesce(1)
              .write
              .format("csv")
              .option("header","true")
              .mode("Overwrite")
              .save(s3bakfile_ins)

            //In Dev and Green V2 cluster, opmode will be DEBUG to generate some output data for the debugging purpose. In prod emr cluster, it will be marked as Production to avoid too much logs as we are running stream jobs and the checkpoint can be used to investigate the running status
            if(opmode == "DEBUG")
            {
              batchDF.show()
            }

            batchDF.unpersist()
            spark.catalog.clearCache()
          }
        }
        .start()
        .awaitTermination()
    }
    catch {
      case ex: SQLException => {
        printf("SQLException: " + ex)
        con.close()
      }
      case ex: Exception => {
        printf("General Exception" + ex)
        con.close()
      }
      case ex: RuntimeException => {
        printf("RuntimeException" + ex)
        con.close()
      }
      case ex: Throwable => {
        printf("Throwable Exception" + ex)
        con.close()
      }
    }
    finally
    {
      con.close()
    }

  }

}
