import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
data = pd.read_csv("outputs2.csv", header=None, sep='\t')
print(data)
flag=True
data= data.iloc[:, 1:3]
data=np.array(data)
print(data)
print(data.shape)
flag=True
parallel=np.zeros((int(data.shape[0]/2), 1))
serial=np.zeros((int(data.shape[0]/2), 1))
if(flag):
    parallel=data[:,0]
    serial = data[:,1]
else:
    for i in range(data.shape[0]):
        if(i%2==0):
            parallel[int(i/2)]=data[i]
        else:
            serial[int(i/2)]=data[i]
print(parallel)
print(serial)
xx=(np.arange(1,serial.shape[0]+1))
one=np.ones(serial.shape[0])
plt.figure(figsize=(16,8))
plt.plot(xx, parallel, marker='o', color='red', label='Time by parallel version')
plt.plot(xx, serial, marker ='o', color='green', label='Time by serial version')
plt.xlabel('Bench-mark id')
plt.ylabel('Time taken in seconds')
plt.title("Time taken vs Benchmark id ")
plt.legend()
plt.show()
speed_up=serial/parallel
plt.plot(xx, one,  color='red', label='Time by parallel version')
plt.plot(xx, speed_up, marker='o', color='green', label='Time by parallel version')
plt.xlabel('Bench-mark id')
plt.ylabel('Speed up ')
plt.title("Speed up vs Benchmark id ")
plt.legend(loc=1)
plt.show()