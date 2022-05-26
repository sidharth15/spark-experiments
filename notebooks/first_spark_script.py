#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession


# ### Create SparkContext object
# 
# If we are running this on a cluster - master should be set with the master_name, which would usually by YARN or mesos, depending on the cluster type.
# 
# Here, in `local[x]`, `x` denotes the number of worker threads to run and number of partitions should be created when using RDD, DataFrame and Dataset. Ideally set x to the number of CPU cores.

# In[2]:


# spark = SparkSession.builder.master("local[1]").appName("sid_spark").getOrCreate()
spark = SparkSession.builder.master("spark://172.31.24.44:7077").appName("master_slave_app").getOrCreate()


# ### Create RDD from Python list

# In[3]:


# Creates an RDD from Python list
rdd = spark.sparkContext.parallelize([1,2,3,4,5])


# In[4]:


print(rdd.count())


# In[5]:


print("Number of partitions in RDD:", rdd.getNumPartitions())


# ### Manually set the number of partitions in an RDD

# In[6]:


# To specify manually how many partitions to use
rdd_partitioned = spark.sparkContext.parallelize([1,2,3,4,5], 5)

print("Number of partitions in RDD:", rdd_partitioned.getNumPartitions())


# ### Create empty RDD

# In[7]:


# empty RDD with no partition
rdd_empty = spark.sparkContext.emptyRDD


# In[8]:


# empty RDD with manual partition
rdd_empty_partitioned = spark.sparkContext.parallelize([], 10)


# ### Repartition and Coalesce
# 
# Repartitioning and coalesce changes the number of partitions in the RDD to the number specified.
# Repartition shuffles the entire data, but coalesce will shuffle only the minimum required number.
# 
# For e.g., in an RDD with 4 partitions:
# 
# `repartition(2)`: shuffles all 4 partitions and creates 2 new partitions
# 
# `coalesce(2)`: shuffles `4-2=2` partitions and gives resultant 2 partitions

# In[9]:


repart_rdd = rdd_partitioned.repartition(4)
coal_rdd = rdd_partitioned.coalesce(2)

print("Number of partitions:", repart_rdd.getNumPartitions())
print("Number of partitions:", coal_rdd.getNumPartitions())


# ### Persist/Cache RDD

# In[10]:


from pyspark import StorageLevel

rdd_persisted = rdd.persist(StorageLevel.MEMORY_ONLY)


# In[11]:


rdd_unpersisted = rdd_persisted.unpersist()


# ### Shared Variables
# 
# #### 1. Broadcast variables
# 
# Broadcast variables are useful to cache some common lookup data in all the worker nodes. They are sent to the executors when they are first used. Important thing is that the values can be read from the executors but cannot be modified.

# In[12]:


states = {"NY":"New York", "CA":"California", "FL":"Florida"}
broadcastStates = spark.sparkContext.broadcast(states)

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]

rdd = spark.sparkContext.parallelize(data)

def state_convert(code):
    return broadcastStates.value[code]

result = rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).collect()
print(result)


# #### 2. Accumulator variables
# 
# These are used for some accumulating functionality - like count or sum, i.e., each executor/task can write or update accumulator variables - but they cannot read it. The value of accumulator variables are available only to the driver.

# In[13]:


accum = spark.sparkContext.accumulator(0)
rdd = spark.sparkContext.parallelize([1,2,3,4,5])
rdd.foreach(lambda x: accum.add(x))
print(accum.value) # access by the driver


# ### DataFrames

# In[14]:


data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)


# In[15]:


df.show()
df.printSchema()


# In[ ]:




