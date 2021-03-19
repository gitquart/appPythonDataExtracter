"""
You can play here!

"""

import os
import database as bd
from InternalControl import cInternalControl

objControl= cInternalControl()

def main():
    dexist=False
    dexist=os.path.exists(objControl.excel_dir)
    if dexist==False:
        os.mkdir(objControl.excel_dir)
    wb = Workbook()
    ws = wb.active
    title=objControl.excel_sheet
    ws.title = title
    lsFields=[]
    #Get cassandra columns
    keyspace=objControl.keyspace
    table=objControl.table
    columns_list=[]
    query="select column_name from system_schema.columns WHERE keyspace_name = '"+keyspace+"' AND table_name = '"+table+"';"
    columns_list=bd.getShortQuery(query)
    coln=1
    for col in columns_list:
        #Write(row,column)
        #Headers (h1,...)
        h1 = ws.cell(row = 1, column = coln)
        if objControl.todosCampos:
            h1.value = col[0]
            lsFields.append(col[0])
        else:
            h1.value = col
            lsFields.append(col)
        coln=coln+1
    #Reading list of fields into commas for the next query  
    #Starts  the reading of periods    
    flag=os.path.isfile(dir_excel)
    if flag:
        #Expedient xls already exists
        print('Printing excel... ')
        if not objControl.iterar:
            query="select "+fieldsForQuery+" from "+keyspace+"."+table+" where year>0 ALLOW FILTERING "
            bd.getLargeQueryAndPrintToExcel(query,dir_excel,title)
        else:
            #lsCondicion=['1997','1998','1999','2000','2001','2002','2003','2004','2005','','2015','2016','2017','2018','2019','2020']    
            for condicion in range(2015,2021):
                query="select "+fieldsForQuery+"  from "+keyspace+"."+table+" where year="+str(condicion)+" "
                bd.getLargeQueryAndPrintToExcel_Special(query,dir_excel,title)



    print('The excel is ready!')        
                         
            
                    

if __name__=='__main__':
    main()    
   