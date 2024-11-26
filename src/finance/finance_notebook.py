from metadata_pipeline import MetadataDrivenPipeline

# Path to the metadata JSON file
metadata_file_path = "/dbfs/mnt/config/metadata.json"

# Instantiate the class
pipeline = MetadataDrivenPipeline(metadata_file_path)

# Run the workflow for a specific entity
entity_name = "Orders"
pipeline.run_workflow_for_entity(entity_name)
