import unittest
from unittest.mock import patch, MagicMock, mock_open
import yaml
import logging
from src.data_ingestion.utils.spark_manager import SparkManager
from src.data_ingestion.workflow import MainWorkflow, run_workflow
from src.data_ingestion.gold.create_features import GoldLayerFeatures

class TestMainWorkflow(unittest.TestCase):
    @patch('src.data_ingestion.workflow.yaml.safe_load')
    @patch('src.data_ingestion.workflow.SparkManager.get_spark_session')
    @patch('src.data_ingestion.workflow.init_global_configuration')
    @patch('src.data_ingestion.workflow.init_global_spark_session')
    @patch('builtins.open', new_callable=mock_open, read_data="config_data")
    @patch('src.data_ingestion.workflow.GoldLayerFeatures')
    def test_workflow_execution(self, mock_gold, mock_file, mock_init_spark_session, mock_init_config, mock_get_spark, mock_yaml_load):
        
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_yaml_load.return_value = {"some": "config"}
        mock_gold_instance = MagicMock()
        mock_gold.return_value = mock_gold_instance

        try:
            
            workflow = MainWorkflow("fake_config_path")
            
            mock_file.assert_called_once_with("fake_config_path", "r")
            mock_yaml_load.assert_called_once_with(mock_file())
            mock_init_config.assert_called_once_with(configuration={"some": "config"})
            mock_get_spark.assert_called_once()
            mock_init_spark_session.assert_called_once_with(spark_session=mock_spark)
            
            run_workflow()
            
            mock_gold.assert_called_once()
            mock_gold_instance.start.assert_called_once()
        except Exception as e:
            self.fail(f"Test Failed: {str(e)}")
