import os
import sys
import yaml
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(parent_dir)
import schedule
import time
import logging
from src.data_ingestion.utils.globals import init_global_spark_session, init_global_configuration
from src.data_ingestion.utils.spark_manager import SparkManager
from src.data_ingestion.bronze.ingest_raw_data import BronzeLayerIngestion
from src.data_ingestion.silver.transform_data import SilverLayerTransformation
from src.data_ingestion.gold.create_features import GoldLayerFeatures

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)


class MainWorkflow:
    
    def __init__(self, config_path):
        logger.info("Initializing Spark session and other components.")
        try:
            with open(config_path, 'r') as config_file:
                config = yaml.safe_load(config_file)
            init_global_configuration(configuration=config)        
            self.spark = None
            self.spark = SparkManager.get_spark_session()
            init_global_spark_session(spark_session=self.spark)
            logger.info("Initialization completed successfully.")
        except FileNotFoundError as e:
            logger.error(f"Configuration file not found: {config_path}", exc_info=True)
        except yaml.YAMLError as e:
            logger.error(f"Error loading YAML file: {str(e)}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error during initialization: {str(e)}", exc_info=True)

def run_workflow():
    logger.info("Starting workflow.")
    try:
        bronze_ingestion = BronzeLayerIngestion()
        bronze_ingestion.start()

        silver_processing = SilverLayerTransformation()
        silver_processing.start()

        gold_transformation = GoldLayerFeatures()
        gold_transformation.start()
           
        logger.info("Workflow completed successfully.")
    except Exception as e:
        logger.error(f"Error during workflow execution: {str(e)}", exc_info=True)

if __name__ == "__main__":

    logger.info("Starting main workflow...")
    config_path = os.path.join(current_dir, "..", "..", "config", "config.yaml")
    try:
        MainWorkflow(config_path)
    except Exception as e:
        logger.error(f"Critical error during workflow initialization: {str(e)}", exc_info=True)
        sys.exit(1)
    logger.info("Scheduling workflow to run every 1 minute")
    schedule.every().day.at("00:00:00").do(run_workflow)
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Workflow manually stopped.")
    except Exception as e:
        logger.error(f"Error during workflow scheduling: {str(e)}", exc_info=True)
    finally:
        try:
            SparkManager.stop_spark_session()
            logger.info("Spark session stopped successfully.")
        except Exception as e:
            logger.error(f"Error stopping Spark session: {str(e)}", exc_info=True)