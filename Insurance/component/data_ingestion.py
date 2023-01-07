from Insurance.entity.config_entity import DataIngestionConfig
import sys,os,stat
import shutil
from Insurance.exception import InsuranceException
from Insurance.logger import logging
from Insurance.entity.artifact_entity import DataIngestionArtifact
import tarfile
import numpy as np
from six.moves import urllib
import pandas as pd
from urllib import request
from urllib.request import urlopen
from sklearn.model_selection import StratifiedShuffleSplit

class DataIngestion:

    def __init__(self,data_ingestion_config:DataIngestionConfig ):
        try:
            logging.info(f"{'>>'*20}Data Ingestion log started.{'<<'*20} ")
            self.data_ingestion_config = data_ingestion_config

        except Exception as e:
            raise InsuranceException(e,sys)

    def download_Insurance_data(self,) -> str:
        try:
            #extracting remote url
            download_url = self.data_ingestion_config.dataset_download_url
            #folder location to download file
            tgz_download_dir = self.data_ingestion_config.tgz_download_dir

            os.makedirs(tgz_download_dir,exist_ok=True)
            
            Insurance_tgz_file = os.path.basename(download_url)
            Insurance_tgz_file_name = Insurance_tgz_file[:-9]

            tgz_file_path = os.path.join(tgz_download_dir,Insurance_tgz_file_name)

            logging.info(f"Downloading file from :[{download_url}] into :[{tgz_file_path}]")
            urllib.request.urlretrieve(download_url, tgz_file_path)
            logging.info(f"File :[{tgz_file_path}] has been downloaded successfully.")
            return tgz_file_path
        
        except Exception as e:
            raise InsuranceException(e,sys) from e




    def extract_file(self,tgz_file_path:str):
        try:
            raw_data_dir = self.data_ingestion_config.raw_data_dir

            if os.path.exists(raw_data_dir):
                os.remove(raw_data_dir)
            
            os.makedirs(raw_data_dir,exist_ok=True)

            logging.info(f"Extracting tgz file: [{tgz_file_path}] into dir: [{raw_data_dir}]")
            with tarfile.open(tgz_file_path) as Insurance_file_obj:
                Insurance_file_obj.extractall(path=raw_data_dir)
            logging.info(f"Extraction completed")

        except Exception as e:
            raise InsuranceException(e, sys) from e


    def split_data_as_train_test(self)-> DataIngestionArtifact:
        try:
            raw_data_dir = self.data_ingestion_config.raw_data_dir
            file_name = os.listdir(raw_data_dir)[0]
            Insurance_file_path = os.path.join(raw_data_dir,file_name)

            logging.info(f"Reading csv file: [{Insurance_file_path}]")

            Insurance_dataframe = pd.read_csv(Insurance_file_path)

            Insurance_dataframe["expense_cat"] = pd.cut(
                Insurance_dataframe["expenses"],
                bins=[0.0, 1.5, 3.0, 4.5, 6.0, np.inf],
                labels=[1,2,3,4,5]
            )
            
        
            logging.info(f"Splitting data into train and test")
            start_train_set = None
            start_test_set = None

            split = StratifiedShuffleSplit(n_splits= 1, test_size=0.2, random_state=0)
          

            for train_index,test_index in split.split(Insurance_dataframe,Insurance_dataframe["expense_cat"]):
                start_train_set = Insurance_dataframe.loc[train_index].drop(["expense_cat"],axis=1)
                start_test_set = Insurance_dataframe.loc[test_index].drop(["expense_cat"],axis=1)

            train_file_path = os.path.join(self.data_ingestion_config.ingested_train_dir,
                                            file_name)
            print(988,train_file_path)

            test_file_path = os.path.join(self.data_ingestion_config.ingested_test_dir,
                                        file_name)
            
            if start_train_set is not None:
                os.makedirs(self.data_ingestion_config.ingested_train_dir,exist_ok=True)
                logging.info(f"Exporting training datset to file: [{train_file_path}]")
                start_train_set.to_csv(train_file_path,index=False)

            if start_test_set is not None:
                os.makedirs(self.data_ingestion_config.ingested_test_dir, exist_ok= True)
                logging.info(f"Exporting test dataset to file: [{test_file_path}]")
                start_test_set.to_csv(test_file_path,index=False)
            

            data_ingestion_artifact = DataIngestionArtifact(train_file_path=train_file_path,
                                test_file_path=test_file_path,
                                is_ingested=True,
                                message=f"Data ingestion completed successfully."
                                
                                )
            logging.info(f"Data Ingestion artifact:[{data_ingestion_artifact}]")
            return data_ingestion_artifact
        
        except Exception as e:
            raise InsuranceException(e, sys) from e

            

    def initiate_data_ingestion(self)-> DataIngestionArtifact:
        try:
            tgz_file_path = self.download_Insurance_data()
            self.extract_file(tgz_file_path=tgz_file_path)
            return self.split_data_as_train_test()
            
        except Exception as e:
            raise InsuranceException(e,sys) from e
    
    def __del__(self):
        logging.info(f"{'>>'*20}Data Ingestion log completed.{'<<'*20} \n\n")