#!/usr/bin/python5


from sys import exit
from math import sqrt
import numpy as np
from numpy import array
from collections import namedtuple
from scipy.optimize import fmin_l_bfgs_b
import pandas as pd
from itertools import chain
from PyAstronomy import pyasl
#def running_median_insort
from collections import deque
from bisect import insort, bisect_left
from itertools import islice
from os.path import basename
import  pickle
import datetime
import math
import json



def linear(x, forecast, alpha = None, beta = None):
    """ Returns a forecast calculated with linear exponential smoothing.
    If `alpha` or `beta` are ``None``, the method will optimize the :func:`MSE` and find the
    most suitable parameters. The method returns these optimized parameters and also the one-step-forecasts, 
    for each value of `x`.

    :param list x: the series to be forecasted
    :param int forecast: the timeperiod of the forecast. F.e. ``forecast=24*7`` for one week (assuming hourly data)
    :param float alpha: the level component
    :param float beta: the trend component

    :returns: (forecast, parameters, one-step-forecasts)
    """
 
    Y = x[:]
 
    if (alpha == None or beta == None):
 
        initial_values = array([0.3, 0.1])
        boundaries = [(0, 1), (0, 1)]
        hw_type = 0#'linear'
 
        parameters = fmin_l_bfgs_b(MSE, x0 = initial_values, args = (Y, hw_type), bounds = boundaries, approx_grad = True)
        alpha, beta = parameters[0]
 
    a = [Y[0]]
    b = [Y[1] - Y[0]]
    y = [a[0] + b[0]]
    
    named_parameters = namedtuple("Linear", ["alpha", "beta"], False)(alpha,beta)

    for i in range(len(Y) + forecast):
 
        if i == len(Y):
            Y.append(a[-1] + b[-1])
            
        __exponential_smoothing_step(Y, i, named_parameters, (a, b, None, y), 0)
 
    return Y[-forecast:], (alpha, beta), y[:-forecast]
 

def additive(x, m, forecast, alpha = None, beta = None, gamma = None, 
        initial_values_optimization=[0.002, 0.0, 0.0002], optimization_type="MSE"):
    """ Returns a forecast calculated with the seasonal exponential smoothing method (additive Holt-Winters). 
    This method will consider seasonality and can be configured by setting the length of the seasonality `m`. If `alpha` or `beta` or `gamma` are ``None``, the method will optimize the :func:`MSE` and find the most suitable parameters. The method returns these optimized parameters and also the one-step-forecasts,  for each value of `x`.

    If optimization is used, a list of starting parameters can be supplied by `initial_values_optimization`. The algorithm will start searching in the neighbourhood of these values. It can't be guaranteed, that all values in the boundaries (0,1) are considered.

    :param list x: the series to be forecasted
    :param int m: the seasonality, f.e. ``m=24`` for daily seasonality (a daily cycle in data is expected)
    :param int forecast: the timeperiod of the forecast. F.e. ``forecast=24*7`` for one week (assuming hourly data)
    :param float alpha: the level component
    :param float beta: the trend component
    :param float gamma: the seasonal component component
    :param list initial_values_optimization: a first guess of the parameters to use when optimizing. 
    :param string optimization_criterion: type of minimization measure, if minimization is used

        *   "MSE" - use :func:`MSE`
        *   "MASE" - use :func:`MASE` (slowest)


    :returns: (forecast, parameters, one-step-forecasts)

    """
 
    Y = x[:]
 
    if (alpha == None or beta == None or gamma == None):
 
        initial_values = array(initial_values_optimization)
        boundaries = [(0, 1), (0, 1), (0, 1)]
        hw_type = 1#'additive'
        optimization_criterion = MSE if optimization_type == "MSE" else MASE
 
        parameters = fmin_l_bfgs_b(optimization_criterion, x0 = initial_values, args = (Y, hw_type, m), bounds = boundaries, approx_grad = True,factr=10**6)
        alpha, beta, gamma = parameters[0]
 
    a = [sum(Y[0:m]) / float(m)]
    b = [(sum(Y[m:2 * m]) - sum(Y[0:m])) / m ** 2]
    s = [Y[i] - a[0] for i in range(m)]
    y = [a[0] + b[0] + s[0]]
    #named_parameters = namedtuple("Additive", ["alpha", "beta","gamma"], False)(alpha,beta,gamma)
    named_parameters = namedtuple("Additive", ["alpha", "beta","gamma"])(alpha,beta,gamma)
    for i in range(len(Y) + forecast):
 
        if i == len(Y):
            Y.append(a[-1] + b[-1] + s[-m])
            
        __exponential_smoothing_step(Y, i, named_parameters, (y, a, b, s, None), 1)
 
    return Y[-forecast:], (alpha, beta, gamma), y[:-forecast]
 

def multiplicative(x, m, forecast, alpha=None, beta=None, gamma=None, initial_values_optimization=[0.002, 0.0, 0.0002], optimization_type="MSE"):
    """This method uses the multiplicative Holt-Winters method. It often delivers better results for our use-case than the additive method.
    For parameters, see :func:`additive` ."""
    Y = x[:]
    test_series = []
    if (alpha == None or beta == None or gamma == None):
 
        initial_values = array(initial_values_optimization)
        boundaries = [(0, 1), (0, 0.05), (0, 1)]
        hw_type = 2 #'multiplicative'
        optimization_criterion = MSE if optimization_type == "MSE" else MASE
        
        train_series = Y[:-m*2]
        test_series = Y[-m*2:]
        
        Y = train_series

        parameters = fmin_l_bfgs_b(optimization_criterion, x0 = initial_values, args = (train_series, hw_type, m, test_series), 
                                   bounds = boundaries, approx_grad = True,factr=10**3)
        alpha, beta, gamma = parameters[0]
    

    a = [sum(Y[0:m]) / float(m)]
    b = [(sum(Y[m:2 * m]) - sum(Y[0:m])) / m ** 2]
    s = [Y[i] / a[0] for i in range(m)]
    y = [(a[0] + b[0]) * s[0]]
    
    #named_parameters = namedtuple("Multiplicative", ["alpha", "beta","gamma"], False)(alpha,beta,gamma)
    named_parameters = namedtuple("Multiplicative", ["alpha", "beta","gamma"])(alpha,beta,gamma)
    for i in range(len(Y) + forecast+len(test_series)):
 
        if i >= len(Y):
            Y.append((a[-1] + b[-1]) * s[-m])
            
        __exponential_smoothing_step(Y, i, named_parameters, (y, a, b, s, None), 2)
        
    return Y[-forecast:], (alpha, beta, gamma), y[:-forecast]


#m = intraday, m2 = intraweek seasonality

def double_seasonal(x, m, m2, forecast, alpha = None, beta = None, gamma = None,delta=None,
                    autocorrelation=None, initial_values_optimization=[0.1, 0.0, 0.2, 0.2, 0.9], optimization_type="MSE"):
    """ Returns a forecast calculated with the double seasonal holt-winters method ( `Taylor 2003 <http://r.789695.n4.nabble.com/file/n888942/ExpSmDoubleSeasonal.pdf>`_ ).
    This method considers two seasonalies. This is great for electrical demand forecasting, as demands have daily and weekly seasonalities. The method also uses autocorrelation,
    to forecast patterns in residuals.
    The trend component is ignored for this method, as electrical demands mostly dont have a trend.

    For all parameters, see  :func:`additive`.

    :param int m: intraday seasonality (``m = 24``, for hourly data)
    :param int m2: intraweek seasonality (``m = 24*7``, for hourly data)


    :returns: (forecast, parameters, one-step-forecasts)

    """
    Y = x[:]
    test_series = []
    if (alpha == None or beta == None or gamma == None):
        initial_values = array(initial_values_optimization)
        boundaries = [(0, 1), (0, 1), (0, 1), (0,1), (0,1)]
        hw_type = 3 #'double seasonal
        optimization_criterion = MSE if optimization_type == "MSE" else MASE
        train_series = Y[:-m2*1]
        test_series = Y[-m2*1:]
        Y = train_series
        parameters = fmin_l_bfgs_b(optimization_criterion, x0 = initial_values, args = (train_series, hw_type, (m,m2), test_series), 
                                   bounds = boundaries, approx_grad = True,factr=10**3)
        alpha, beta, gamma, delta, autocorrelation = parameters[0]
    a = [sum(Y[0:m]) / float(m)]
    b = [(sum(Y[m:2 * m]) - sum(Y[0:m])) / m ** 2]
    s = [Y[i] / a[0] for i in range(m)]
    s2 = [Y[i] / a[0] for i in range(0,m2,m)]
    y = [a[0] +b[0]+ s[0] + s2[0]]   #b:trend,s:season,s2:season2,a:level
    #named_parameters = namedtuple("Multiplicative", ["alpha", "beta","gamma","delta","autocorrelation"], False)(alpha,beta,gamma,delta,autocorrelation)
    named_parameters = namedtuple("Multiplicative", ["alpha", "beta","gamma","delta","autocorrelation"])(alpha,beta,gamma,delta,autocorrelation)
    for i in range(len(Y) + forecast+len(test_series)):
 
        if i >= len(Y):
            Y.append(a[-1] + b[-1]+ s[-m] + s2[-m2])
        __exponential_smoothing_step(Y, i, named_parameters, y, a, b, s, s2, 3)    
    return Y[-forecast:], (alpha, beta, gamma, delta, autocorrelation),y[:-forecast]
#forcast value ,parameters,estimat value
# a,b,s,s2  are decompositions;y  is estimate ;"alpha","beta","gamma","delte are parametres
#double season 
def MSE(params, *args):   
    """ ``Internal Method``. Calculates the Mean Square Error of one run of holt-winters with the supplied arguments.
    The MSE is actually computed from the MSE of the one-step-forecast error and the error between the forecast and a testseries. 

    :param list params: (alpha, ...) the parameters
    :param list \*args: (input_series, hw type, m), with hwtype in [0:3] depicting hw method
    """
    forecast, onestepfcs = _holt_winters(params,*args)
    test_data = args[3]
    train = args[0]
    mse_outofsample = sum([(m - n) ** 2 for m, n in zip(test_data, forecast)]) / len(test_data)
    mse_insample = sum([(m - n) ** 2 for m, n in zip(train, onestepfcs)]) / len(train)
    
    return mse_insample + mse_outofsample
#double season 
def MASE(params, *args):   
    """ Calculates the Mean-Absolute Scaled Error (see :py:meth:`server.forecasting.forecasting.StatisticalForecast.MASE`). For parameters see :func:`MSE`.
    """
    input = args[0]
    forecast = _holt_winters(params,*args)
    
    training_series = np.array(input)
    testing_series = np.array(args[3])
    prediction_series = np.array(forecast)
    n = training_series.shape[0]
    d = np.abs(  np.diff(training_series) ).sum()/(n-1)
    
    errors = np.abs(testing_series - prediction_series )
    return errors.mean()/d



        
#double season
def _holt_winters(params, *args):   
    """ ``Internal Method``. Calculates one holt-winters run. This method is used in calculating of MSE."""
    train = args[0][:]
    hw_type = args[1]
    m = args[2]
    test_data = args[3]
    if hw_type == 0:
        alpha, beta = params
        linear(train, len(test_data), alpha, beta)
    elif hw_type == 1:
        alpha, beta, gamma = params
        forecast, params, onestepfcs = additive(train, m, len(test_data), alpha=alpha,beta=beta,gamma=gamma)
    elif hw_type == 2:
        alpha, beta, gamma = params
        forecast, params, onestepfcs = multiplicative(train, m, len(test_data), alpha=alpha,beta=beta,gamma=gamma)
    elif hw_type == 3:
            alpha, beta, gamma, delta, autocorrelation = params
            forecast, params, onestepfcs = double_seasonal(train, m[0],m[1], len(test_data), 
                                                             alpha=alpha,beta=beta,gamma=gamma,delta=delta, 
                                                             autocorrelation=autocorrelation,optimization_type="MSE")
    else:
    
        exit('type must be either linear, additive, multiplicative or double seasonal')
    
    return forecast, onestepfcs
        


def __exponential_smoothing_step(input, index, params,forecast, level, trend, seasonal, seasonal2,hw_type ):
    """ ``Internal Method``. Calculates one step of the smoothing method supplied by `hw_type`."""
    # 0 = linear, 1 = addditive, 2 = multiplicative, 3 = double seasonal
    i = index
    Y = input
    
    a = level
    b = trend
    s = seasonal
    s2 = seasonal2
    y = forecast
    if hw_type == 0:
        a.append(params.alpha * Y[i] + (1 - params.alpha) * (a[i] + b[i]))
        b.append(params.beta * (a[i + 1] - a[i]) + (1 - params.beta) * b[i])
        y.append(a[i + 1] + b[i + 1])
    
    elif hw_type == 1:
        a.append(params.alpha * (Y[i] - s[i]) + (1 - params.alpha) * (a[i] + b[i]))
        b.append(params.beta * (a[i + 1] - a[i]) + (1 - params.beta) * b[i])
        s.append(params.gamma * (Y[i] - a[i] - b[i]) + (1 - params.gamma) * s[i])
        y.append(a[i + 1] + b[i + 1] + s[i + 1])
    
    elif hw_type == 2:
        a.append(params.alpha * (Y[i] / s[i]) + (1 - params.alpha) * (a[i] + b[i]))
        b.append(params.beta * (a[i + 1] - a[i]) + (1 - params.beta) * b[i])
        s.append(params.gamma * (Y[i] / (a[i] + b[i])) + (1 - params.gamma) * s[i])
        y.append((a[i + 1] + b[i + 1]) * s[i + 1])
        
    elif hw_type == 3:
        #see Short-term electricity demand forecasting using 
        #double seasonal exponential smoothing (Tayler 2003)
        a.append( params.alpha * (Y[i] - b[i]-s2[i] - s[i]) + (1 - params.alpha) * (a[i]))
        b.append( params.beta * (Y[i] - a[i]-s2[i] - s[i]) + (1 - params.beta) * (b[i]))
        s.append(params.delta *  (Y[i] - a[i] -b[i]- s2[i]) + (1 - params.delta) * s[i])
        s2.append(params.gamma * (Y[i] - a[i] -b[i]- s[i]) + (1 - params.gamma) * s2[i])
        autocorr = params.autocorrelation * (Y[i] - (a[i] +b[i]+ s[i] + s2[i])) 
        y.append(a[i + 1] +b[i+1]+ s[i + 1] + s2[i + 1] + autocorr)      
def running_median_insort(seq, window_size):
    """Contributed by Peter Otten"""
    seq = iter(seq)
    d = deque()
    s = []
    result = []
    for item in islice(seq, window_size):
        d.append(item)
        insort(s, item)
        result.append(s[len(d)//2])
    m = window_size // 2
    for item in seq:
        old = d.popleft()
        d.append(item)
        del s[bisect_left(s, old)]
        insort(s, item)
        result.append(s[m])
    return result       

