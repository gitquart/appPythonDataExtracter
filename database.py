from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from openpyxl import Workbook
from openpyxl import load_workbook
from InternalControl import cInternalControl

objControl= cInternalControl()
cloud_config= {
        'secure_connect_bundle':'secure-connect-'+objControl.db+'.zip'
    }


def getCluster():
    #Connect to Cassandra
    objCC=CassandraConnection()
    user=''
    password=''
    if objControl.db=='dbquart':
        user=objCC.cc_user
        password=objCC.cc_pwd
    else:
        user=objCC.cc_user_test
        password=objCC.cc_pwd_test

    auth_provider = PlainTextAuthProvider(user,password)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)

    return cluster
"""
getLargeQuery
Use getLargeQuery if you want to print  the results set of any table with several records. i.e. 10 K or more records
You can get a DataFrame from a list : https://www.geeksforgeeks.org/create-a-pandas-dataframe-from-lists/
"""
def getLargeQuery(query):
    cluster = getCluster()
    session = cluster.connect()
    session.default_timeout=70     
    statement = SimpleStatement(query, fetch_size=1000)
    for row in session.execute(statement):
        ls=[]
        for col in row:
            ls.append(str(col))
           
    cluster.shutdown() 
    return ls


"""
getTotalOfRecords
Use getTotalOfRecords to know the total records of a Large Table
Large Table: Any table with information
"""

def getTotalOfRecords(query):
    cluster = getCluster()
    session = cluster.connect()
    session.default_timeout=70     
    statement = SimpleStatement(query, fetch_size=1000)
    count=0
    print('Start count...')
    for row in session.execute(statement):
        count+=1
        
    print('Total rows:',str(count))        
    cluster.shutdown()     


"""
getShortQuery
Use getShortQuery if you want to get the resultSet of a Small table
Small table: Any control table in Datastax, i.e. 50 records
"""
def getShortQuery(query):
    res=''
    cluster=getCluster()
    session = cluster.connect()
    session.default_timeout=70 
    future = session.execute_async(query)
    res=future.result()
    cluster.shutdown()

    return res 


     
class CassandraConnection():
    cc_user='quartadmin'
    cc_pwd='P@ssw0rd33'
    cc_user_test='test'
    cc_pwd_test='testquart'