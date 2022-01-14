import pyodbc
import pandas as pd

cnxn = pyodbc.connect(Driver='{SQL Server};Server=tcp:c1ylwjuviw.database.windows.net,1433;Database=DbGeniusCS;Uid=********;Pwd=HNG:*********:@y;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
#cnxn.autocommit = True


def UpdateExcTariffs(ColumnName,tableInputs):
    cursor = cnxn.cursor()
    sql_file = open('ExcTariffsUpdateQuery.txt','r').read()
    params_for_SQL = (ColumnName, tableInputs)
    cursor.execute(sql_file,params_for_SQL)
    cnxn.commit()
    return True

def UpdateExcTariffsDiff(ColumnName,tableInputs):
    cursor = cnxn.cursor()
    sql_file = open('ExcTariffsUpdateQueryTariff.txt','r').read()
    params_for_SQL = (ColumnName, tableInputs)
    cursor.execute(sql_file,params_for_SQL)
    cnxn.commit()
    return True

def IniTariffCleaning(): 
    cursor = cnxn.cursor()
    cursor.execute("Exec [Simulator].[SpInitialiseTariffCleaning]")
    cnxn.commit()
    return True
    
def GetReqIdPCWId():
    #list of RequestId and PCWSiteId for the day
    sql = "{call [Simulator].[SpGetRequestPCWList]}"
    ReqIdPCWId = pd.read_sql_query(sql=sql,con=cnxn)  
    return ReqIdPCWId  

def GetDataFrame(ColumnName,PCWSiteId,RequestId):  
    #getting data for column where its bit is 0
    sql = "{call [Simulator].[SpGetExclusiveDataFrame](?,?,?)}"
    dataframe = pd.read_sql_query(sql=sql,con=cnxn,params=[ColumnName,PCWSiteId,RequestId])
    return dataframe

def LogError(RowIdExclusiveTariffs,ColumnName,ErrorCode,ErrorMessage):
    cursor = cnxn.cursor()
    cursor.execute("EXEC [Simulator].[SpLogErrorSimExclTariffCleaning]  @RowIdExclusiveTariffs = ?, @ColumnName = ?,  @ErrorCode = ?, @ErrorMessage = ?",RowIdExclusiveTariffs,ColumnName,ErrorCode,ErrorMessage)
    cnxn.commit()
    return True

