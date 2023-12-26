from datetime import datetime, timedelta, date
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine

from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.layers import LSTM
from vnstock import *
def get_data():
    engine = create_engine('postgresql://airflow:airflow@172.18.0.3:5432/stock_db')
    table_name = 'd1'
    query = f'SELECT * FROM {table_name};'
    df = pd.read_sql(query, engine)
    return df
def create_dataset(dataset, time_step=1):
	dataX, dataY = [], []
	for i in range(len(dataset)-time_step-1):
		a = dataset[i:(i+time_step), 0]   ###i=0, 0,1,2,3-----99   100 
		dataX.append(a)
		dataY.append(dataset[i + time_step, 0])
	return np.array(dataX), np.array(dataY)
def train_model():
    df = get_data()
    df1=df.reset_index()['close']
    scaler=MinMaxScaler(feature_range=(0,1))
    df1=scaler.fit_transform(np.array(df1).reshape(-1,1))
    training_size=int(len(df1)*0.65)
    test_size=len(df1)-training_size
    train_data,test_data=df1[0:training_size,:],df1[training_size:len(df1),:1]

    # reshape into X=t,t+1,t+2,t+3 and Y=t+4
    time_step = 100
    X_train, y_train = create_dataset(train_data, time_step)
    X_test, y_test = create_dataset(test_data, time_step)
    
    # reshape input to be [samples, time steps, features] which is required for LSTM
    X_train =X_train.reshape(X_train.shape[0],X_train.shape[1] , 1)
    X_test = X_test.reshape(X_test.shape[0],X_test.shape[1] , 1)
    
    model=Sequential()
    model.add(LSTM(50,return_sequences=True,input_shape=(100,1)))
    model.add(LSTM(50,return_sequences=True))
    model.add(LSTM(50))
    model.add(Dense(1))
    model.compile(loss='mean_squared_error',optimizer='adam')
    callback_model = tf.keras.callbacks.ModelCheckpoint('/opt/airflow/plugins/stockmodel.h5', monitor='val_loss')
    history = model.fit(x = X_train, y = y_train, validation_data=(X_test, y_test), epochs = 10, batch_size = 64, callbacks = [callback_model])
    return True

default_args = {
    'owner': 'DuongNam',
    'start_date': days_ago(0),
    'email': ['namduong1702@gmail.com'],
    'email_on_failure': False,
    'email_on-retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='train_model_stock_dag',
        default_args=default_args,
        description='Train model stock',
        start_date=datetime(2023, 12, 23),
        schedule_interval='@daily',
        tags=['stock_data']
) as dag:

    train_model = PythonOperator(
        task_id='train_stock_model',
        python_callable=train_model,
        dag=dag
    )
    train_model
        