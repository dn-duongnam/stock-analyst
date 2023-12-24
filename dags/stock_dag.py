from datetime import datetime, timedelta, date
import time
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import numpy as np
import ssl

from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.layers import LSTM
from vnstock import *
def crawl_data():
    current_date = datetime.now().date().strftime('%Y-%m-%d')
    stock_data = stock_historical_data(symbol="TCH", start_date="2020-01-01", end_date=current_date, resolution="1D", type="stock", beautify=True, decor=False, source='DNSE')
    if not stock_data.empty:
        csv_file_name = "/opt/airflow/plugins/stock_data.csv"
        stock_data.to_csv(csv_file_name, index=False)
        print(f"Dữ liệu đã được lưu vào tệp {csv_file_name}")
    else:
        print("Không có dữ liệu để lưu.")
    return True
def create_dataset(dataset, time_step=1):
	dataX, dataY = [], []
	for i in range(len(dataset)-time_step-1):
		a = dataset[i:(i+time_step), 0]   ###i=0, 0,1,2,3-----99   100 
		dataX.append(a)
		dataY.append(dataset[i + time_step, 0])
	return np.array(dataX), np.array(dataY)
def train_model():
    df = pd.read_csv("/opt/airflow/plugins/stock_data.csv")
    df1=df.reset_index()['close']
    scaler=MinMaxScaler(feature_range=(0,1))
    df1=scaler.fit_transform(np.array(df1).reshape(-1,1))
    training_size=int(len(df1)*0.65)
    test_size=len(df1)-training_size
    train_data,test_data=df1[0:training_size,:],df1[training_size:len(df1),:1]

    # reshape into X=t,t+1,t+2,t+3 and Y=t+4
    time_step = 100
    X_train, y_train = create_dataset(train_data, time_step)
    X_test, ytest = create_dataset(test_data, time_step)
    
    # reshape input to be [samples, time steps, features] which is required for LSTM
    X_train =X_train.reshape(X_train.shape[0],X_train.shape[1] , 1)
    X_test = X_test.reshape(X_test.shape[0],X_test.shape[1] , 1)
    
    model=Sequential()
    model.add(LSTM(50,return_sequences=True,input_shape=(100,1)))
    model.add(LSTM(50,return_sequences=True))
    model.add(LSTM(50))
    model.add(Dense(1))
    model.compile(loss='mean_squared_error',optimizer='adam')
    model.fit(X_train,y_train,validation_data=(X_test,ytest),epochs=10,batch_size=64,verbose=1)
    model.save("/opt/airflow/plugins/stockmodel.h5")
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
        dag_id='stock_dag',
        default_args=default_args,
        description='Train model stock',
        start_date=datetime(2023, 12, 23),
        schedule_interval='@daily',
        tags=['stock_data']
) as dag:
    
    crawl_data = PythonOperator(
        task_id='crawl_data',
        python_callable=crawl_data,
        dag=dag
    )
    train_model = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
        dag=dag
    )
    crawl_data >> train_model
        