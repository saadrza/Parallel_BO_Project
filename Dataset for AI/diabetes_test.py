import numpy as np
import tensorflow as tf
from tensorflow import keras
import keras
import random 

from sklearn.model_selection import GridSearchCV, RandomizedSearchCV, StratifiedKFold 
from keras.models import Sequential
from keras.layers import Dense, Dropout
from keras.wrappers.scikit_learn import KerasClassifier
from numpy import sqrt
from numpy import argmax
from tensorflow.keras.optimizers import Adam

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import time
import os
from sklearn.model_selection import train_test_split

from sklearn.utils import resample
from sklearn.metrics import confusion_matrix,roc_curve, classification_report, f1_score, roc_auc_score,accuracy_score, recall_score, precision_score,log_loss, make_scorer, cohen_kappa_score,fbeta_score,matthews_corrcoef,recall_score
from imblearn.metrics import geometric_mean_score
from sklearn.model_selection import cross_val_score
import seaborn as sns
import matplotlib.pyplot as plt

import math
import ast

from sklearn.metrics import auc

#for RadomizedSearchCV
from scipy.stats import uniform as sp_randFloat
from scipy.stats import randint as sp_randInt

# random seed
from random_seeds import reset_seeds

# Percentile bootstrap
from bootstrapmetric_diabetes import bootstrap_metric
# from bca import bootstrap_metric

# for HyperOpt TPE
from hyperopt import hp, fmin, tpe, STATUS_OK, Trials, space_eval
from hyperopt.early_stop import no_progress_loss
import sys
import yaml
import argparse
from datetime import datetime
import keras.backend as K

def save_results(method, folder_name, proposed=False):
    '''
    Save results of methods
    '''
    
    # Get the current working directory
    current_directory = os.getcwd()
    # Get the parent directory
    parent_directory = os.path.dirname(current_directory)

    # Define the path to the result folder
    folder_path = os.path.join(parent_directory, 'results',  name_dataset, method, folder_name)
    # Check if the directory exists
    if not os.path.exists(folder_path):
        # If it doesn't exist, create it
        os.makedirs(folder_path)

    # Save the results to a CSV file inside the folder
    if proposed == True:
        return folder_path
    else:
        file_path = os.path.join(folder_path, f"{method}.csv")      
        return file_path

def recall_m(y_true, y_pred):
    true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
    possible_positives = K.sum(K.round(K.clip(y_true, 0, 1)))
    recall = true_positives / (possible_positives + K.epsilon())
    return recall

def precision_m(y_true, y_pred):
    true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
    predicted_positives = K.sum(K.round(K.clip(y_pred, 0, 1)))
    precision = true_positives / (predicted_positives + K.epsilon())
    return precision

def f1_m(y_true, y_pred):
    precision = precision_m(y_true, y_pred)
    recall = recall_m(y_true, y_pred)
    return 2*((precision*recall)/(precision+recall+K.epsilon()))

# Model Initialization
def create_model(dropout_rate, learning_rate, neurons):
    '''
    Weight Initialization for all methods
    '''
    reset_seeds()
    model = Sequential()
    model.add(Dense(neurons,input_dim = n_input,kernel_initializer = kernel_initializer,activation =activation_function))
    model.add(Dropout(dropout_rate))
    model.add(Dense(1,activation = 'sigmoid'))
    
    adam = Adam(learning_rate = learning_rate)
    # model.compile(loss = 'binary_crossentropy',optimizer = adam,metrics = ['accuracy'])
    if metric_name == 'accuracy':
        model.compile(loss = 'binary_crossentropy',optimizer = adam,metrics = ['accuracy'])
    if metric_name == 'auc':
        model.compile(loss = 'binary_crossentropy',optimizer = adam,metrics = [tf.keras.metrics.AUC()])
    if metric_name == 'f1' :
        model.compile(loss = 'binary_crossentropy',optimizer = adam,metrics = [f1_m])
    
    return model

def scorer():
    
    if metric_name == 'accuracy':
        metric_scorer = make_scorer(accuracy_score)
    if metric_name == 'auc':
        metric_scorer = make_scorer(roc_auc_score)
    if metric_name == 'f1' :
        metric_scorer = make_scorer(f1_score)
    return metric_scorer

# Grid Search
def grid_method(grid_search_params):
    '''
    Grid Search Method
    '''
    print("Grid Search ####################################################################################\n")
    start_time = time.time()
    # Create the model
    model = KerasClassifier(build_fn = create_model,verbose = 0,batch_size = 32,epochs = 100)

    # Build and fit the GridSearchCV
    metric_scorer = scorer()
    grid = GridSearchCV(estimator = model,param_grid = grid_search_params, cv = StratifiedKFold(shuffle=True,random_state=42),verbose = 10, scoring = metric_scorer)
    # grid = GridSearchCV(estimator = model,param_grid = grid_search_params, cv = StratifiedKFold(shuffle=True,random_state=42),verbose = 10, scoring='accuracy')
    grid_result = grid.fit(X_train, y_train)
    print('Best hyperparameters GS: {}, using {}'.format(grid_result.best_score_,grid_result.best_params_))
    
    #Save Gridsearch object
    df_GS = pd.DataFrame(grid_result.cv_results_)
    
    # Create a unique folder name based on current datetime
    now = datetime.now()
    folder_name = now.strftime("%Y-%m-%d-%H-%M-%S")
    
    filename = save_results('GS', folder_name)
    df_GS.to_csv(filename, index=False)
    best_hyperparameters.append(grid_result.best_params_)
    search_space.append(space_grid)
    time_list.append(int(time.time() - start_time))
    print("--- %s seconds ---" % (time.time() - start_time))
    
    
def random_method(random_search_params):
    '''
    Random Search Method
    '''
    print("Random Search ####################################################################################\n")
    start_time = time.time()
    # Create the model
    model = KerasClassifier(build_fn = create_model,verbose = 0,batch_size = 32,epochs = 100)
    
    metric_scorer = scorer()
    # Build and fit the GridSearchCV
    grid = RandomizedSearchCV(estimator = model,param_distributions = random_search_params, cv = StratifiedKFold(shuffle=True,random_state=42), verbose = 10,  n_iter=nbr_evals, scoring = metric_scorer)
    # grid = RandomizedSearchCV(estimator = model,param_distributions = random_search_params, cv = StratifiedKFold(shuffle=True,random_state=42), verbose = 10,  n_iter=nbr_evals, scoring='accuracy')
    grid_result = grid.fit(X_train, y_train)
    print('Best hyperparameters RS: {}, using {}'.format(grid_result.best_score_,grid_result.best_params_))
    #Save Randomsearch object
    df_RS = pd.DataFrame(grid_result.cv_results_)
    
    # Create a unique folder name based on current datetime
    now = datetime.now()
    folder_name = now.strftime("%Y-%m-%d-%H-%M-%S")
    
    filename = save_results('RS', folder_name)
    df_RS.to_csv(filename, index=False)

    best_hyperparameters.append(grid_result.best_params_)
    time_list.append(int(time.time() - start_time))
    print("--- %s seconds ---" % (time.time() - start_time))
    
# Define the objective function for Hyperopt
def objective_bs(params,X_bs, y_bs):
    '''
    Hyperopt calls this function with values generated from the hyperparameter space provided in the space argument. This function can return the loss as a scalar value or in a dictionary
    '''
    model = KerasClassifier(build_fn=create_model,**params)

    kfold = StratifiedKFold(shuffle=True, random_state=42)
    if metric_name == "accuracy":
        score = cross_val_score(model,X_bs, y_bs, cv=kfold, scoring="accuracy",n_jobs=-1).mean()
    if metric_name == "auc":
        score = cross_val_score(model,X_bs, y_bs, cv=kfold, scoring="roc_auc",n_jobs=-1).mean()
    if metric_name == 'f1' :
        score = cross_val_score(model,X_bs, y_bs, cv=kfold, scoring="f1",n_jobs=-1).mean()
        
    results_bayesian_bootstrap.append({'params': params, 'score': score})
    return {'loss': -score, 'status': STATUS_OK}


def objective_bayesian(params,X_bs, y_bs):
    '''
    Hyperopt calls this function with values generated from the hyperparameter space provided in the space argument. This function can return the loss as a scalar value or in a dictionary
    '''
    model = KerasClassifier(build_fn=create_model,**params)

    kfold = StratifiedKFold(shuffle=True, random_state=42)
    if metric_name == "accuracy":
        score = cross_val_score(model,X_bs, y_bs, cv=kfold, scoring="accuracy",n_jobs=-1).mean()
    if metric_name == "auc":
        score = cross_val_score(model,X_bs, y_bs, cv=kfold, scoring="roc_auc",n_jobs=-1).mean()
    if metric_name == 'f1' :
        score = cross_val_score(model,X_bs, y_bs, cv=kfold, scoring="f1",n_jobs=-1).mean()
        
    results_bayesian.append({'params': params, 'score': score})
    return {'loss': -score, 'status': STATUS_OK}
        


def confidence_interval(list_score_sample,list_combination_sample):
    '''
    Calculate confidence interval from scores list
    '''
    # average and standard deviation of scores list
    mean_score = np.mean(list_score_sample)
    std_score = np.std(list_score_sample)

    # confidence interval of scores
    confidence_interval = (mean_score - std_score, mean_score + std_score)  
    indice_score = np.where((list_score_sample >= confidence_interval[0]) & (list_score_sample <= confidence_interval[1]))
    combination_list = [list_combination_sample[i] for i in indice_score[0]]

    # Initialize empty lists to store dropout_rate, learning_rate, and neurons
    dropout_rates = []
    learning_rates = []
    neurons_list = []

    # Loop through each combination and extract the hyperparameter values
    for combination in combination_list:
        dropout_rates.append(combination['dropout_rate'])
        learning_rates.append(combination['learning_rate'])
        neurons_list.append(combination['neurons'])

    #remove duplicate values and sort list
    dropout_final = sorted(set(dropout_rates))
    learning_final = sorted(set(learning_rates))
    neurons_final = sorted(set(neurons_list))
   
    print("search space",int(len(dropout_final)*len(learning_final)*len(neurons_final)))
    return dropout_final, learning_final, neurons_final


def bayesian_method(hyperopt_params):
    '''
    Bayesian (HyperOpt TPE) Method
    '''
    print("Bayesian (HyperOpt TPE) ####################################################################################\n")
    
    start_time = time.time()

    # Run Hyperopt
    trials = Trials()
    best = fmin(fn=lambda p: objective_bayesian(p, X_train, y_train), space=hyperopt_params, algo=tpe.suggest, max_evals=nbr_evals, trials=trials,rstate=np.random.default_rng(42))

    # Convert the results list to a DataFrame
    results_df = pd.DataFrame(results_bayesian)
    
    # Create a unique folder name based on current datetime
    now = datetime.now()
    folder_name = now.strftime("%Y-%m-%d-%H-%M-%S")
    
    filename = save_results('HyperOpt', folder_name)    
    results_df.to_csv(filename, index=False)

    print("Best hyperparameters Bayesian:", best)
    best_hyperparameters.append(best)
    time_list.append(int(time.time() - start_time))
    print("--- %s seconds ---" % (time.time() - start_time))
    


def proposed_method(iterations_number, hyperopt_params):   
    '''
    Proposed Method
    '''
    print(f"Proposed Method (Bayesian_v1) {iterations_number} iterations with {metric_name} metric:####################################################################################\n")
   
    execution_time_firstloop = execution_time_secondloop1 = execution_time_secondloop2 = 0
    list_score_sample = []
    list_combination_sample = []
    total_acc = []
    total_auc = []
    total_f1 = []
  
    start_time = time.time()
    n_iterations = iterations_number
       
    # Create a unique folder name based on current datetime
    now = datetime.now()
    folder_name = now.strftime("%Y-%m-%d_%H-%M-%S")
   
    # BEGIN FIRST STEP: Bootstrap sampling ###############################################################################
    for i in range(n_iterations):
        print("################Iteration ",i, "#################################")
        reset_seeds(1234 + i)
       
        global results_bayesian_bootstrap
        results_bayesian_bootstrap = []
        X_resampled, y_resampled = resample(X_train, y_train, replace=True, stratify = y_train)
        
        trials = Trials()
        best = fmin(fn=lambda p: objective_bs(p,X_resampled, y_resampled), space=hyperopt_params, algo=tpe.suggest, max_evals=nbr_evals, trials=trials)
        
        best_params = space_eval(hyperopt_params, best)
        
        best_model = KerasClassifier(
        build_fn=create_model,
        verbose=0,
        batch_size=32,
        epochs=100
        )

        best_model.set_params(**best_params)
        best_model.fit(X_resampled, y_resampled, validation_data=(X_test, y_test))

        y_pred = best_model.predict(X_test)
        fpr, tpr, thresholds = roc_curve(y_test, y_pred)
        # calculate the g-mean for each threshold
        gmeans = sqrt(tpr * (1-fpr))
        ix = argmax(gmeans)
        best_threshold = thresholds[ix]

        y_pred = (y_pred >= best_threshold).astype(int)
        
        acc = accuracy_score(y_test, y_pred)
        auc = roc_auc_score(y_test, y_pred)
        f1score = f1_score(y_test, y_pred)

        total_acc.append(acc)
        total_auc.append(auc)
        total_f1.append(f1score)

        #Get the best hyperparameters combination
        list_combination_sample.append(best_params)   

        #Save HyperOpt object
        name_file = 'Hyperopt_' + str(i) + '.csv'          
        path = save_results('Hyperopt_GS', folder_name,proposed=True)
        file_path = os.path.join(path,name_file)

        # Convert the results list to a DataFrame
        results_df = pd.DataFrame(results_bayesian_bootstrap)

        # Save the results to a CSV file
        results_df.to_csv(file_path, index=False)       
        
    execution_time_firstloop = execution_time_secondloop1 = execution_time_secondloop2 = int(time.time() - start_time)
    print("Execution time of first step: --- %s seconds ---" % (execution_time_firstloop))
    list_dict = {'best_combination':list_combination_sample,'accuracy':total_acc, 'auc':total_auc, 'f1':total_f1} 
    df = pd.DataFrame(list_dict) 
    
    path = save_results('Hyperopt_GS', folder_name,proposed=True)
    filename = os.path.join(path, f"Hyperopt_final_loop1.csv")   
    df.to_csv(filename, index=False)
    
    accuracy_scores = df['accuracy']
    auc_scores = df['auc']
    f1_scores = df['f1']
    
    # Percentile bootstrap
    bootstrap_metric(name_dataset,metric_name,accuracy_scores, auc_scores, f1_scores)
    # list of combinations through 10 iterations
    list_combination_sample = df['best_combination']
    
    # list of scores through 10 iterations
    if metric_name == 'accuracy':
        list_score_sample = df['accuracy']
    if metric_name == 'auc':
        list_score_sample = df['auc']
    else:
        list_score_sample = df['f1']

    dropout_final_firstloop,learning_final_firstloop,neurons_final_firstloop = confidence_interval(list_score_sample,list_combination_sample)

    # BEGIN SECOND LOOP FOR 5 ITERATIONS OF BO ###############################################################################
    search_params = {
        'dropout_rate': hp.choice('dropout_rate', dropout_final_firstloop),
        'learning_rate': hp.choice('learning_rate',learning_final_firstloop),
        'neurons': hp.choice('neurons',neurons_final_firstloop)
    }
    
    list_score_sample = []
    list_combination_sample = []
    total_acc = []
    total_auc = []
    total_f1 = []
    model = KerasClassifier(build_fn = create_model,verbose = 0,batch_size = 32,epochs = 100)

    start_time = time.time()
    n_iterations = iterations_number
    
    # Create a unique folder name based on current datetime
    now = datetime.now()
    for i in range(n_iterations):
        print("################Iteration ",i, "#################################")
        reset_seeds(1234 + i)
        
        X_resampled, y_resampled = resample(X_train, y_train, replace=True, stratify = y_train)
    
        trials = Trials()
        best = fmin(fn=lambda p: objective_bs(p,X_resampled, y_resampled), space=search_params, algo=tpe.suggest, max_evals=nbr_evals, trials=trials)
        
        best_params = space_eval(search_params, best)
        
        best_model = KerasClassifier(
        build_fn=create_model,
        verbose=0,
        batch_size=32,
        epochs=100
        )

        best_model.set_params(**best_params)
        best_model.fit(X_resampled, y_resampled, validation_data=(X_test, y_test))

        y_pred = best_model.predict(X_test)
        fpr, tpr, thresholds = roc_curve(y_test, y_pred)
      
        gmeans = sqrt(tpr * (1-fpr))
        ix = argmax(gmeans)
        best_threshold = thresholds[ix]

        y_pred = (y_pred >= best_threshold).astype(int)
        
        acc = accuracy_score(y_test, y_pred)
        auc = roc_auc_score(y_test, y_pred)
        f1score = f1_score(y_test, y_pred)

        total_acc.append(acc)
        total_auc.append(auc)
        total_f1.append(f1score)

        #Get the best hyperparameters combination
        list_combination_sample.append(best_params)   

        #Save HyperOpt object
        name_file = 'BO_5iterations_loop1' + str(i) + '.csv'          
        path = save_results('Hyperopt_GS', folder_name,proposed=True)
        file_path = os.path.join(path,name_file)

        # Convert the results list to a DataFrame
        results_df = pd.DataFrame(results_bayesian_bootstrap)

        # Save the results to a CSV file
        results_df.to_csv(file_path, index=False)       
        
    execution_time_secondloop2 += int(time.time() - start_time)
    print("Execution time of first step: --- %s seconds ---" % (execution_time_secondloop2))
    list_dict = {'best_combination':list_combination_sample,'accuracy':total_acc, 'auc':total_auc, 'f1':total_f1} 
    df = pd.DataFrame(list_dict) 
    
    path = save_results('Hyperopt_GS', folder_name,proposed=True)
    filename = os.path.join(path, f"RS_final.csv")   
    df.to_csv(filename, index=False)
    
    accuracy_scores = df['accuracy']
    auc_scores = df['auc']
    f1_scores = df['f1']
    
    # Percentile bootstrap
    bootstrap_metric(name_dataset,metric_name,accuracy_scores, auc_scores, f1_scores)
    # list of combinations through 10 iterations
    list_combination_sample = df['best_combination']
    
    # list of scores through 10 iterations
    if metric_name == 'accuracy':
        list_score_sample = df['accuracy']
    if metric_name == 'auc':
        list_score_sample = df['auc']
    else:
        list_score_sample = df['f1']



    dropout_final,learning_final,neurons_final = confidence_interval(list_score_sample,list_combination_sample)   
    print('search spcae',int(len(dropout_final)*len(learning_final)*len(neurons_final)))
    search_space.append(int(len(dropout_final)*len(learning_final)*len(neurons_final)))
 
    # Begin Grid Search for RS results
    start_time = time.time()
    # Create the model
    model = KerasClassifier(build_fn = create_model,verbose = 0,batch_size = 32,epochs = 100)

    # Define the grid search parameters
    dropout_rate = dropout_final
    learning_rate = learning_final
    neurons = neurons_final

    # # Make a dictionary of the grid search parameters
    param_grids = dict(dropout_rate = dropout_rate, learning_rate= learning_rate, neurons=neurons)

    metric_scorer = scorer()
    # Build and fit the GridSearchCV
    # metric_scorer = make_scorer(f1_score)
    grid = GridSearchCV(estimator = model,param_grid = param_grids,cv = StratifiedKFold(shuffle=True,random_state=42), verbose = 10, scoring = metric_scorer)

    grid_result = grid.fit(X_train, y_train)
    # Summarize the results
    print('Best hyperparameters - Proposed method : {}, using {}'.format(grid_result.best_score_,grid_result.best_params_))
    #Save Gridsearch object
    df_GSfinal = pd.DataFrame(grid_result.cv_results_)

    path = save_results('Hyperopt_GS', folder_name,proposed=True)
    file_GS = "results_GS_BO" + metric_name + ".csv"
    filename = os.path.join(path, file_GS)  
    df_GSfinal.to_csv(filename, index=False)

    best_hyperparameters.append(grid_result.best_params_)
    execution_time_secondloop2 += int(time.time() - start_time)
    time_list.append(execution_time_secondloop2)
    print("--- %s seconds ---" % (execution_time_secondloop2))
    

def performance(best_hyperparameters):
    '''
    Results of all methods (comparison)
    '''
    print("Comparison of performances: ####################################################################################\n")
    # Create the KerasClassifier with the model function and random seed
    model = KerasClassifier(
        build_fn=create_model,
        verbose=0,
        batch_size=32,
        epochs=100
    )

    accuracy_total =[]
    auc_total = []
    f1_total = []
    precision_total = []
    recall_total = []
    f2_total = []
    geometric_total =  []
    cohen_total = []
    mcc_total = []
    spe_total = []
    
    for hyperparameters in best_hyperparameters:
        model.set_params(**hyperparameters)

        # Train the model on the current hyperparameter combination
        model.fit(X_train, y_train, validation_data=(X_test, y_test))

        y_pred = model.predict(X_test)
        fpr, tpr, thresholds = roc_curve(y_test, y_pred)
        # calculate the g-mean for each threshold
        gmeans = sqrt(tpr * (1-fpr))
        ix = argmax(gmeans)
        best_threshold = thresholds[ix]

        y_pred = (y_pred >= best_threshold).astype(int)
   
        accuracy = accuracy_score(y_test, y_pred)
        auc =  roc_auc_score(y_test, y_pred)
        f1 =  f1_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, average='binary')
        recall = recall_score(y_test, y_pred, average='binary')
        f2 = fbeta_score(y_test, y_pred, beta=2.0)
        geometric = geometric_mean_score(y_test, y_pred, average='binary')
        cohen = cohen_kappa_score(y_test, y_pred)
        mcc = matthews_corrcoef(y_test, y_pred)
        spe = recall_score(np.logical_not(y_test) , np.logical_not(y_pred))
        
        auc_total.append(auc)
        accuracy_total.append(accuracy)
        f1_total.append(f1)
        f2_total.append(f2)
        precision_total.append(precision)
        recall_total.append(recall)
        geometric_total.append(geometric)
        cohen_total.append(cohen)
        mcc_total.append(mcc)
        spe_total.append(spe)
    
    #dictionary of lists
    methods_name = ["Grid Search", "Random Search", "Bayesian","Proposed Method"]
   
    dict = {'Method':methods_name,
            'AUC': auc_total,
            'Accuracy': accuracy_total,
            'F1 score': f1_total,
            'Precision': precision_total,
            'Recall': recall_total,
            'Specificity': spe_total,
            'F2 score': f2_total,
            'Geometric': geometric_total,
            'Kappe': cohen_total,
            'MCC': mcc_total,
            'Best hyperparameters': best_hyperparameters,
            'Execution time (second)': time_list}
    
    
    dict_space = {'Method': ["Grid Search","Proposed Method"],
              'Search_space': search_space}
    
    df = pd.DataFrame(dict)
    df_space = pd.DataFrame(dict_space)
    current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
   
    path = '../results/' + name_dataset +'/' + current_datetime + '_' + 'final.csv'
    path_space= '../results/' + name_dataset +'/' + current_datetime + '_' + 'space_final.csv'
    df.to_csv(path, index=False)
    df_space.to_csv(path_space, index=False)
    
def main():
    global name_dataset,argument_number, metric_name, file_name
    ap = argparse.ArgumentParser()
    
    ap.add_argument('--dataset', dest='dataset', required=True, help='--name of dataset ', type=str)
    ap.add_argument('--numbers', dest='numbers', required=True, help='--numbers takes a list of n elements for iterations ', type=int, default=10)
    ap.add_argument('--metric', dest='metric', required=True, help='--metric for proposed method', type=str, default='accuracy')
    ap.add_argument('--path', dest='path', required=True, help='--path file for search space ', type=str)
    
    args = ap.parse_args()
    try:
        print("Name of dataset entered:",  args.dataset)
        print("Number of iterations entered:",  args.numbers)
        print("Metric entered:",  args.metric)
        print("Config file:", args.path)
        name_dataset = args.dataset
        metric_name = args.metric
        argument_number = args.numbers
        file_name = args.path
        
    except IndexError:
        print('no arguments passed(name of dataset, number of iterations, metric for proposed method or YAML file')
        sys.exit()
        
    # Load the YAML file and parse its contents into a Python dictionary
    file_path = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', 'config', file_name))
    with open(file_path, 'r') as file:
        hyperparameters_dict = yaml.safe_load(file)

    global X_train, X_test, y_train, y_test, kernel_initializer, activation_function, n_input, best_hyperparameters, results_bayesian, time_list,search_space,nbr_evals
    # Get train set and test set
    X_train = pd.read_csv(hyperparameters_dict['path']['xtrain'])
    X_test = pd.read_csv(hyperparameters_dict['path']['xtest'])
    y_train = pd.read_csv(hyperparameters_dict['path']['ytrain'])
    y_test = pd.read_csv(hyperparameters_dict['path']['ytest'])

    # Fix hyperparameters
    kernel_initializer = 'normal'
    activation_function = 'relu'

    n_input = hyperparameters_dict['n_input']
    nbr_evals = hyperparameters_dict['evals']
    # Results list
    best_hyperparameters = []
    search_space = []
    # Initialize a list to store the results of bayesian
    results_bayesian = []
    results_bayesian_bootstrap = []

    # Initialize a list to store the execution time of each method
    time_list = []
    global space_grid
    space_grid = hyperparameters_dict['space_grid']
    # Define the Grid Search parameter space
    grid_search_params = {
        'dropout_rate': hyperparameters_dict['grid_search']['dropout_rate']['values'],
        'learning_rate': hyperparameters_dict['grid_search']['learning_rate']['values'],
        'neurons': hyperparameters_dict['grid_search']['neurons']['values']
    }

    # Define the Random Search parameter space
    random_search_params = {
        'dropout_rate': np.arange(hyperparameters_dict['random_search']['dropout_rate']['min'], hyperparameters_dict['random_search']['dropout_rate']['max'], hyperparameters_dict['random_search']['dropout_rate']['step']),
        'learning_rate': sp_randFloat(hyperparameters_dict['random_search']['learning_rate']['min'], hyperparameters_dict['random_search']['learning_rate']['max']),
        'neurons': sp_randInt(hyperparameters_dict['random_search']['neurons']['min'], hyperparameters_dict['random_search']['neurons']['max'] + 1)
    }


    # Define the HyperOpt parameter space
    hyperopt_params = {
        'dropout_rate': hp.quniform('dropout_rate', hyperparameters_dict['hyperopt']['dropout_rate']['min'], hyperparameters_dict['hyperopt']['dropout_rate']['max'],hyperparameters_dict['hyperopt']['dropout_rate']['step']),
        'learning_rate': hp.loguniform('learning_rate', np.log(hyperparameters_dict['hyperopt']['learning_rate']['min']), np.log(hyperparameters_dict['hyperopt']['learning_rate']['max'])),
        'neurons': hp.randint('neurons', hyperparameters_dict['hyperopt']['neurons']['min'], hyperparameters_dict['hyperopt']['neurons']['max']+1)
    }
    
    print( "Four methods\n")
         
    grid_method(grid_search_params)
    random_method(random_search_params)
    bayesian_method(hyperopt_params)
    proposed_method(argument_number, hyperopt_params)
    
    performance(best_hyperparameters)

if __name__ == "__main__":
    main()