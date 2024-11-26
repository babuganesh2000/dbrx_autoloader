import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable


class MetadataDrivenPipeline:
    def __init__(self, metadata_path: str):
        """
        Initialize the class with the path to the metadata JSON file.
        """
        self.spark = SparkSession.builder.appName("MetadataDrivenPipeline").getOrCreate()
        self.metadata_path = metadata_path
        self.metadata = self._load_metadata()

    def _load_metadata(self):
        """
        Load metadata from the JSON file.
        """
        with open(self.metadata_path, "r") as file:
            return json.load(file)

    def get_entity_metadata(self, entity_name: str):
        """
        Fetch metadata for the specified entity name.
        """
        for entity in self.metadata:
            if entity["table_name"] == entity_name:
                return entity
        raise ValueError(f"Entity '{entity_name}' not found in metadata.")

    def _read_source_data(self, source_container, table_name, increment_column, checkpoint_path):
        """
        Read incremental data from ADLS using Auto Loader.
        """
        df = (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.schemaLocation", checkpoint_path)
            .load(f"abfss://{source_container}.dfs.core.windows.net/{table_name}")
        )
        return df

    def _write_to_delta(self, df, target_table, key_column):
        """
        Perform upsert into Delta Lake using DeltaTable.merge.
        """
        delta_table_path = f"/mnt/delta/{target_table}"
        if DeltaTable.isDeltaTable(self.spark, delta_table_path):
            delta_table = DeltaTable.forPath(self.spark, delta_table_path)
            delta_table.alias("tgt").merge(
                df.alias("src"),
                f"tgt.{key_column} = src.{key_column}"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            # Write as a new Delta table if it doesn't exist
            df.write.format("delta").mode("overwrite").save(delta_table_path)
            
    def _archive_files(self, source_container, table_name, archive_folder):
        """
        Archive processed files.
        """
        # Copy files to archive (pseudo-code; implement using ADLS SDK or Databricks utilities)
        self.spark.conf.set("fs.azure.account.key.<storage_account>.dfs.core.windows.net", "<account_key>")
        archive_path = f"abfss://{source_container}.dfs.core.windows.net/{archive_folder}"
        processed_path = f"abfss://{source_container}.dfs.core.windows.net/{table_name}/processed"
        self.spark.fs.mv(processed_path, archive_path)

    def run_workflow_for_entity(self, entity_name: str):
        """
        Run the workflow for a specific entity by name.
        """
        try:
            # Fetch metadata for the entity
            entity_metadata = self.get_entity_metadata(entity_name)

            source_group = entity_metadata["source_group"]
            table_name = entity_metadata["table_name"]
            key_column = entity_metadata["key_column"]
            increment_column = entity_metadata["increment_column"]
            source_container = entity_metadata["source_container"]
            archive_folder = entity_metadata["archive_folder"]
            target_table = entity_metadata["target_table"]

            print(f"Processing entity: {table_name}")

            # Define checkpoint path for the entity
            checkpoint_path = f"/mnt/checkpoints/{source_group}/{table_name}"

            # Read incremental data
            df = self._read_source_data(source_container, table_name, increment_column, checkpoint_path)

            # Add a timestamp column for data tracking
            df = df.withColumn("LoadTimestamp", current_timestamp())

            # Write to Delta Lake
            self._write_to_delta(df, target_table, key_column)

            print(f"Workflow completed for entity: {table_name}")

        except ValueError as e:
            print(e)
