"""
You can play here!

"""

import os
import database as bd
from InternalControl import cInternalControl

objControl= cInternalControl()


#Get cassandra columns
keyspace=objControl.keyspace 
table=objControl.table
columns_list=[]
query="select id from "+keyspace+"."+table+" where secuencia=0 ALLOW FILTERING;"
#column_list can be used to name the columns in dataframe
bd.getTotalOfRecords(query) 
#Fecth any data you want here...the query   
    



  
   

