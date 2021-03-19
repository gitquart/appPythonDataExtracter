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
    query="select column_name from system_schema.columns WHERE keyspace_name = '"+keyspace+"' AND table_name = '"+table+"';"
    #column_list can be used to name the columns in dataframe
    columns_list=bd.getShortQuery(query) 
    #Fecth any data you want here...the query   
    



    
if __name__=='__main__':
    main()    
   