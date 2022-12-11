from flask import Flask
from Insurance.logger import logging
from Insurance.exception import InsuranceException
import sys 

app=Flask(__name__)

@app.route("/",methods =['GET','POST'])
def index():
    try:
        raise Exception("testing")
    except Exception as e :
        Insurance = InsuranceException(e,sys)
        logging.info (Insurance.error_message)
        logging.info('AAAAAAAAAAAAAAAAAAAAAA')
    return "Hello Swarupa"

if __name__ == "__main__":
    app.run(debug= True)
