# Score Prediction Project

This module built for predicting the score and it is written on Python.


## How to run the Score Prediction Module
### Create the virtual environment
1. pip install virtualenv
2. virtualenv venv

### Activate the virtual environment
1. venv\Script\activate

### Install the prerequistics
1. pip install -r requirements.txt
Note: You install all the prerequistics inside the venv

### Run the Score Module via Fast API
Run the below command in the current directory (With uvicorn)
1. uvicorn score.app:app --host="0.0.0.0" --port=5000