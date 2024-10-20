import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from src.data_ingestion.silver.transform_data import SilverLayerTransformation

class TestSilverLayerTransformation(unittest.TestCase):
    @patch('src.data_ingestion.silver.transform_data.global_spark_session')
    def test_silver_layer_transformation(self, mock_spark_session):
        # Configuration des mocks
        mock_spark = MagicMock()
        mock_spark_session.return_value = mock_spark
        
        # # Création de mock DataFrames
        mock_empty_df = MagicMock()
        mock_empty_df.isEmpty.return_value = True
        mock_empty_df.schema = StructType([
            StructField("id", LongType(), True),
            StructField("last_update_date", TimestampType(), True)
        ])

        # # Mock pour les données des tracks
        mock_track_df = MagicMock()
        mock_track_df.isEmpty.return_value = False
        mock_track_df.columns = ["name", "artist", "genres", "duration", "last_update_date"]
        
        # # Configuration des retours pour les différentes opérations
        mock_df_operations = MagicMock()
        mock_df_operations.withColumn.return_value = mock_df_operations
        mock_df_operations.select.return_value = mock_df_operations
        mock_df_operations.filter.return_value = mock_df_operations
        mock_df_operations.join.return_value = mock_df_operations
        mock_df_operations.unionByName.return_value = mock_df_operations
        mock_df_operations.write.partitionBy.return_value.mode.return_value.parquet = MagicMock()
        mock_df_operations.first.return_value = [datetime(2023, 1, 1)]
        
        mock_data_df = MagicMock()
        mock_data_df.isEmpty.return_value = False
        mock_data_df.columns = ["id", "last_update_date"]
        
        # # Configuration du mock spark.read
        mock_reader = MagicMock()
        mock_reader.schema.return_value.parquet.return_value = mock_df_operations
        mock_spark.read = mock_reader
        
        # # Configuration du mock createDataFrame
        mock_spark.createDataFrame.return_value = mock_empty_df

        try:
        #     # Instanciation de la classe à tester
            silver_transformer = SilverLayerTransformation()
            
        #     # Test du processus complet
            silver_transformer.start()
            
        #     # Vérifications des appels
            self.assertTrue(mock_spark_session.called)
            self.assertTrue(mock_reader.schema.called)
            
        #     # Test du traitement d'une table spécifique
            with patch('os.path.dirname') as mock_dirname, \
                 patch('os.path.abspath') as mock_abspath, \
                 patch('os.path.join') as mock_join, \
                 patch('os.makedirs') as mock_makedirs:
                
                mock_dirname.return_value = "/fake/path"
                mock_abspath.return_value = "/fake/path"
                mock_join.return_value = "/fake/path/data"
                
                silver_transformer._process_table("track")
                
        #         # Vérification de la création des répertoires
                self.assertTrue(mock_makedirs.called)
                
        #         # Test du traitement des tables dérivées
                silver_transformer._process_table("album")
                silver_transformer._process_table("artist")
                silver_transformer._process_table("genre")

                self.assertTrue(mock_spark_session.called)
                self.assertTrue(mock_reader.schema.called)
                self.assertTrue(mock_makedirs.called)
                
        except Exception as e:
            self.fail(f"Le test a échoué avec l'erreur: {str(e)}")