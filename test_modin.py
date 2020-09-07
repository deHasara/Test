'''
import os
os.environ["MODIN_ENGINE"] = "dask"
from dask.distributed import Client
if __name__ =="__main__":
    client = Client()
    import modin.pandas as pd
    print(pd.__version__)
'''
import time
import ray
ray.init()  #http://localhost:8265/
import modin.pandas as pd

df = pd.read_csv("/home/hasara/python code/csv/test modin/join/mycsvnew1.csv")
df1 = pd.read_csv("/home/hasara/python code/csv/test modin/join/mycsvjoin1.csv")
#print(df)
#result = df.join(df1, how = 'inner')
s = time.time()
result = df.merge(df1, how = 'inner')
e = time.time()
print(type(result))
#result = pd.concat([df, df1])
print(result)
print('merge time: {}'.format(e-s)) #seconds
 
