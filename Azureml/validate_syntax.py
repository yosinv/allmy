import sys, os, json, inspect, time, copy
from datetime import datetime as dt


frame = inspect.currentframe()
step = 'class: {}, func: {}'.format(__name__, inspect.getframeinfo(frame).function)
start_time = dt.now()
print('step: {} started. at: {}'.format(step, dt.now()))


print(step)