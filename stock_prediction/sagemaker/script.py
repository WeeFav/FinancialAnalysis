import argparse
import os
import joblib
import numpy as np
import pandas as pd
from keras.models import Sequential
from keras.layers import LSTM, Input, Dense, Dropout
from sklearn.metrics import mean_squared_error
import boto3

if __name__ == "__main__":
    print("extracting arguments")
    parser = argparse.ArgumentParser()

    # Hyperparameters sent by the client are passed as command-line arguments to the script.
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--batch_size", type=int, default=128)
    
    # Data, model, and output directories
    parser.add_argument("--model-dir", type=str, default=os.environ.get("SM_MODEL_DIR"))
    parser.add_argument('--xtrain', type=str, default=os.environ.get('SM_CHANNEL_XTRAIN'))
    parser.add_argument('--ytrain', type=str, default=os.environ.get('SM_CHANNEL_YTRAIN'))
    parser.add_argument('--xtest', type=str, default=os.environ.get('SM_CHANNEL_XTEST'))
    parser.add_argument('--ytest', type=str, default=os.environ.get('SM_CHANNEL_YTEST'))
    parser.add_argument('--scaler', type=str, default=os.environ.get('SM_CHANNEL_SCALER'))
    args, _ = parser.parse_known_args()
   
    print("reading data")

    # SageMaker does the following:
    # Downloads each S3 object to the training instance.
    # Mounts them to /opt/ml/input/data/{channel_name}.
    # Sets environment variables like SM_CHANNEL_XTRAIN=/opt/ml/input/data/xtrain
    xtrain_dir = args.xtrain  # will be /opt/ml/input/data/xtrain
    xtrain_path = os.path.join(xtrain_dir, 'X_train_dict.joblib')
    X_train_dict = joblib.load(xtrain_path)
    
    ytrain_dir = args.ytrain
    ytrain_path = os.path.join(ytrain_dir, 'y_train_dict.joblib')
    y_train_dict = joblib.load(ytrain_path)
    
    xtest_dir = args.xtest
    xtest_path = os.path.join(xtest_dir, 'X_test_dict.joblib')
    X_test_dict = joblib.load(xtest_path)
    
    ytest_dir = args.ytest  # will be /opt/ml/input/data/xtrain
    ytest_path = os.path.join(ytest_dir, 'y_test_dict.joblib')
    y_test_dict = joblib.load(ytest_path)
    
    scaler_dir = args.scaler  # will be /opt/ml/input/data/xtrain
    scaler_path = os.path.join(scaler_dir, 'scaler_dict.joblib')
    scaler_dict = joblib.load(scaler_path)

    print(X_train_dict)
    print(scaler_dict)
    
    print("building training datasets")
    # combine all training samples
    X_train = np.concatenate([X_train_dict[company] for company in X_train_dict], axis=0)
    y_train = np.concatenate([y_train_dict[company] for company in y_train_dict], axis=0)
    print(X_train.shape)
    print(y_train.shape)
    
    # Train model
    print("building model")
    model = Sequential()
    model.add(LSTM(128, return_sequences=True, input_shape=(X_train.shape[1], X_train.shape[2])))
    model.add(LSTM(128, return_sequences=True))
    model.add(LSTM(128))
    model.add(Dense(1))
    model.compile(loss='mean_squared_error', optimizer='adam')
    model.summary()

    print("training model")
    # train model
    history = model.fit(
        X_train,
        y_train,
        validation_split=0.2,
        epochs=args.epochs,
        batch_size=args.batch_size,
        verbose=1
    )

    print("evaluate model")
    # unscale the data
    for company in scaler_dict:
        scaler = scaler_dict[company]
        y_pred = model.predict(X_test_dict[company]).flatten()
        y_true = y_test_dict[company]
        # create dummy input matrix since we originally scaled on a matrix
        dummy_pred = np.zeros(shape=(len(y_pred), 4))
        dummy_pred[:, 0] = y_pred
        dummy_true = np.zeros(shape=(len(y_true), 4))
        dummy_true[:, 0] = y_true
        y_pred_scaled = scaler.inverse_transform(dummy_pred)[:, 0]
        y_true_scaled = scaler.inverse_transform(dummy_true)[:, 0]
        MSE = mean_squared_error(y_pred_scaled, y_true_scaled)
        print("MSE", MSE)

    # Persist model
    path = os.path.join(args.model_dir, "model.joblib")
    joblib.dump(model, path)
    print("model persisted at " + path)