import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

// TableConfig without columnsToCompare because we infer them from the target schema
case class TableConfig(
  tableName: String,
  primaryKey: Seq[String],
  structColumns: Seq[String],
  beginTimestampCol: String,
  endTimestampCol: String,
  softDeleteCol: String,
  isSoftDeleteEnabled: Boolean
)

def getTargetSchema(targetDF: DataFrame): Map[String, String] = {
  targetDF.schema.fields.map { field =>
    field.name -> field.dataType.typeName
  }.toMap
}

def castColumns(df: DataFrame, columnsToCompare: Map[String, String]): DataFrame = {
  columnsToCompare.foldLeft(df) { case (tempDF, (colName, colType)) =>
    if (tempDF.columns.contains(colName)) {
      tempDF.withColumn(colName, col(colName).cast(colType)) // cast only if the column exists
    } else {
      tempDF // return the DataFrame as is if the column doesn't exist
    }
  }
}

def mergeInto(
  sourceDF: DataFrame,
  targetTableConfig: TableConfig
)(implicit spark: SparkSession): (Long, Long, Long, Long) = {

  // Load the target table
  val targetDF = spark.read.format("iceberg").load(targetTableConfig.tableName)

  // Get target schema to dynamically cast columns
  val targetSchema = getTargetSchema(targetDF)

  // Cast source and target DataFrame columns to the expected data types based on target schema
  val sourceCastedDF = castColumns(sourceDF, targetSchema)
  val targetCastedDF = castColumns(targetDF, targetSchema)

  // Compute hash for struct columns and non-struct columns
  val sourceWithHash = sourceCastedDF.withColumn("combined_hash",
    hash(
      concat_ws("_", 
        targetSchema.keys.toSeq.map(col) ++ 
        targetTableConfig.structColumns.map(c => hash(col(c))): _*
      )
    )
  )
  val targetWithHash = targetCastedDF.withColumn("combined_hash",
    hash(
      concat_ws("_", 
        targetSchema.keys.toSeq.map(col) ++ 
        targetTableConfig.structColumns.map(c => hash(col(c))): _*
      )
    )
  )

  // Join source and target based on the primary key
  val joinedDF = sourceWithHash.alias("source")
    .join(targetWithHash.alias("target"), targetTableConfig.primaryKey, "full_outer")
    .select(
      col("source.*"),
      col("source.combined_hash").as("source_hash"),
      col("target.combined_hash").as("target_hash"),
      col(s"target.${targetTableConfig.softDeleteCol}").as("is_deleted")
    )

  // Identify inserts
  val insertsDF = joinedDF.filter($"target_hash".isNull)
    .withColumn(targetTableConfig.beginTimestampCol, current_timestamp())
    .withColumn(targetTableConfig.endTimestampCol, lit(null))
    .withColumn(targetTableConfig.softDeleteCol, lit(false))

  // Identify updates
  val updatesDF = joinedDF.filter($"target_hash".isNotNull && $"source_hash" =!= $"target_hash")
    .withColumn(targetTableConfig.endTimestampCol, current_timestamp())

  // Identify unchanged rows
  val unchangedDF = joinedDF.filter($"source_hash" === $"target_hash")

  // Handle soft deletes
  val deletesDF = if (targetTableConfig.isSoftDeleteEnabled) {
    joinedDF.filter($"source_hash".isNull && $"target_hash".isNotNull)
      .withColumn(targetTableConfig.softDeleteCol, lit(1))
      .withColumn(targetTableConfig.endTimestampCol, current_timestamp())
  } else {
    spark.emptyDataFrame
  }

  // Count reconciliation for inserts, updates, deletes, and unchanged
  val insertsCount = insertsDF.count()
  val updatesCount = updatesDF.count()
  val deletesCount = if (targetTableConfig.isSoftDeleteEnabled) deletesDF.count() else 0L
  val unchangedCount = unchangedDF.count()

  // Log reconciliation counts
  println(s"Reconciliation Summary for Table: ${targetTableConfig.tableName}")
  println(s"Inserts: $insertsCount")
  println(s"Updates: $updatesCount")
  println(s"Deletes: $deletesCount")
  println(s"Unchanged: $unchangedCount")

  // Write inserts to Iceberg (new rows)
  if (insertsCount > 0) {
    insertsDF.write.format("iceberg").mode("append").save(targetTableConfig.tableName)
  }

  // Use MERGE INTO to update existing rows in the Iceberg table
  if (updatesCount > 0) {
    updatesDF.createOrReplaceTempView("source_updates")

    spark.sql(s"""
      MERGE INTO ${targetTableConfig.tableName} AS target
      USING source_updates AS source
      ON ${targetTableConfig.primaryKey.map(key => s"target.$key = source.$key").mkString(" AND ")}
      WHEN MATCHED THEN
        UPDATE SET *
    """)
  }

  // Use MERGE INTO to mark soft deletes
  if (deletesCount > 0 && !deletesDF.isEmpty) {
    deletesDF.createOrReplaceTempView("source_deletes")

    spark.sql(s"""
      MERGE INTO ${targetTableConfig.tableName} AS target
      USING source_deletes AS source
      ON ${targetTableConfig.primaryKey.map(key => s"target.$key = source.$key").mkString(" AND ")}
      WHEN MATCHED THEN
        UPDATE SET target.${targetTableConfig.softDeleteCol} = true, target.${targetTableConfig.endTimestampCol} = current_timestamp()
    """)
  }

  // Return reconciliation counts
  (insertsCount, updatesCount, deletesCount, unchangedCount)
}

// Example of running for multiple tables
val tableConfigs = Seq(
  TableConfig(
    tableName = "iceberg_catalog.db.table1",
    primaryKey = Seq("id"),
    structColumns = Seq("address"),
    beginTimestampCol = "begin_timestamp",
    endTimestampCol = "end_timestamp",
    softDeleteCol = "is_deleted",
    isSoftDeleteEnabled = true
  ),
  TableConfig(
    tableName = "iceberg_catalog.db.table2",
    primaryKey = Seq("id"),
    structColumns = Seq("details"),
    beginTimestampCol = "start_time",
    endTimestampCol = "finish_time",
    softDeleteCol = "deleted",
    isSoftDeleteEnabled = true
  )
)

// Loop through each table
tableConfigs.foreach { config =>
  val sourceDF = spark.read.parquet(s"path_to_source_${config.tableName}")
  val (inserts, updates, deletes, unchanged) = mergeInto(sourceDF, config)
  println(s"Table ${config.tableName}: Inserts: $inserts, Updates: $updates, Deletes: $deletes, Unchanged: $unchanged")
}
