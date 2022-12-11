from Insurance.pipeline.pipeline import Pipeline
from Insurance.exception import InsuranceException
from Insurance.logger import logging

def main():
    try:
        pipeline = Pipeline()
        pipeline.run_pipeline()
        # data_validation_cofig = Configuration().get_data_transformation_config()
        # print(data_validation_cofig)
    except Exception as e:
        logging.error(f"{e}")
        print(e)

if __name__ =="__main__":
    main()
