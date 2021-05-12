"""
You can play here!

"""

import os
import database as bd
from InternalControl import cInternalControl

objControl= cInternalControl()

 def main():
    #Get cassandra columns
    keyspace=objControl.keyspace 
    table=objControl.table
    columns_list=[]
    query="select id from "+objControl.keyspace+"."+objControl.table+" where secuencia=0 ALLOW FILTERING;"
    #column_list can be used to name the columns in dataframe
    bd.getTotalOfRecords(query) 
    #Fecth any data you want here...the query   
    



if __name__=='__main__':
    main()    
   

