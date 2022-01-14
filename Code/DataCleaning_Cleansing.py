import DataCleaning_queries as DQ
import pandas as pd
import numpy as np
import re
import re
import json
from datetime import datetime



#def SupplierName(ReqId,PCWID):
    #use lookup (to be created)
 #   return 


#1. Cleaning Discount
def Discount(PCWId,ReqId):
    ReqId_local = ReqId
    PCWId_local = PCWId
    ColName = 'Discounts'

    df = DQ.GetDataFrame(ColName,PCWId_local,ReqId_local)  #returns RowId and ColumnName data
    df['Discounts'].str.lower()
    for i, item in enumerate(df['TariffType']):   
        CurrentRowId = int(df.RowIdExclusiveTariffs[i])
        ErrorCode = 1
        try:         
          if(df.at[i,ColName]=='not applicable' or pd.isna(df.at[i,ColName])):
            df.at[i,ColName]="0"
          
          df.at[i,'Discounts'] = re.sub("[^0123456789\.]","",df.at[i,'Discounts'])
          df.at[i,'Discounts'] = pd.to_numeric(df.at[i,'Discounts'])
          
        except ValueError as e:
              e=str(e)
              errorMsg = df.at[i,'Discounts']+' '+e  #you can rearrange the error message
              DQ.LogError(CurrentRowId,ColName,ErrorCode,errorMsg)
              df.at[i,'IsError']=True                #Rows that have error will be marked True

    df['RequestId']=0
    df['SupplierName']=None
    df['TariffName']=None
    df['StandingCharge']=0
    df['UnitRate']=0
    df['ExitFees']=0
    df['Green']=0
    df['NightUnitRate']=0
    df['PCWSiteId']=0
    df.drop(df[df['IsError']==True].index, inplace = True)  #Dropping all rows from data frame that have errors only sending cleaned row to DB
    df=df.reset_index()
    df.Discounts = pd.to_numeric(df.Discounts,errors='coerce')  #imp for all the functions to convert to numeric
    df = df[['RowIdExclusiveTariffs','RequestId','SupplierName','TariffName','StandingCharge','UnitRate','NightUnitRate',
                'Green','Discounts','ExitFees','PCWSiteId']]
    finalDF = df.to_json(orient='records')             #convert df to json format. Imp step before calling update
    DQ.UpdateExcTariffs(ColName,finalDF)
    print("Discount Done")


#2. Cleaning Green 
def isGreen(PCWId,ReqId):
     PCWId_local = PCWId
     ReqId_local = ReqId
     ColName = 'Green'
     df = DQ.GetDataFrame(ColName,PCWId_local,ReqId_local)  #returns RowId and ColumnName data
     df['Green'].replace('None','False',inplace=True)       #Replacing None to False so that they can be detected easily
     for i, item in enumerate(df['Green']):   
      CurrentRowId = int(df.RowIdExclusiveTariffs[i])
      ErrorCode = 1   
      try:      
        if(df.at[i,ColName]==' ' or pd.isna(df.at[i,ColName]) or df.at[i,ColName] is None or df.at[i,ColName]=='None' ): 
            df.at[i,ColName]= False
        if(df.at[i,ColName]==1 or df.at[i,ColName]=='True' or df.at[i,ColName]=='TRUE'):
            df.at[i,ColName]=True
        else:
            df.at[i,ColName]=False
      except ValueError as e:
            errorMsg = df.at[i,'isGreen']+' '+e  #you can rearrange the error message
            DQ.LogError(CurrentRowId,ColName,ErrorCode,errorMsg)
            df.at[i,'IsError']=True      # Error rows being marked as True

     df['RequestId']=0
     df['SupplierName']=None
     df['TariffName']=None
     df['StandingCharge']=0
     df['UnitRate']=0
     df['ExitFees']=0
     df['Discounts']=0
     df['NightUnitRate']=0
     df['PCWSiteId']=0
     df.drop(df[df['IsError']==True].index, inplace = True) #Dropping rows that have error from data frame
     df=df.reset_index()
     finalDF = df.to_json(orient='records')             #convert df to json format. Imp step before calling update
     DQ.UpdateExcTariffs(ColName,finalDF)
     print("isGreen Done")
    
    
#3.Cleaning TarrifType data.
#merge columns for TariffType and TariffEndsOn
def TariffType(PCWId,ReqId):
  PCWId_local = PCWId
  ReqId_local = ReqId
  TariffType = 'TariffType'
  ColName = 'TariffType'
  df = DQ.GetDataFrame(ColName,PCWId_local,ReqId_local)  #returns RowId and ColumnName data 
  df["TariffType"] = df["TariffType"].str.lower()
  df["TariffEndsOn"] = df["TariffEndsOn"].str.lower()
  Fixed = df[TariffType].str.contains('fixed')
  Variable = df[TariffType].str.contains('variable')
  df[ColName] = np.where(Fixed, 'fixed',np.where(Variable, 'variable', df[ColName].str.replace('-', ' ')))

  searchfor = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'
  ,'january','feburary','march','april','june','july','august','september','october','november','december']
  regex_searchFor = '|'.join(searchfor) 
  #update
  df = df.replace(r'^\s*$', np.nan, regex=True)  #converting empty strings to NaN
  for i, item in enumerate(df['TariffType']):   
      CurrentRowId = int(df.RowIdExclusiveTariffs[i])
      ErrorCode = 1
      try:  
          if(df.at[i,ColName] != 'variable' and df.at[i,ColName] != 'fixed'):
              ErrorMessage = "Static value detected in TariffType/TariffEndson"
              DQ.LogError(CurrentRowId,ColName,ErrorCode,ErrorMessage)
              df.at[i,'IsError']=True  # Error rows being marked as True
          if((df.at[i,ColName] == 'variable')):
              df.loc[i,'IsFixedDuration'] = False
              df.loc[i,'IsFixedDynamic'] = False
              df.loc[i,'FixedMonths'] = None
              df.at[i,'ValidTo']= "01-Jan-1900"
          if((df.at[i,ColName] == 'fixed')):
              if "months" in str(df.at[i,'TariffEndsOn']):
                  df.at[i,'FixedMonths'] = str(re.sub("[^0123456789\.]","",str(df.at[i,'TariffEndsOn']))[:2])
                  df.at[i,'IsFixedDuration'] = True
                  df.at[i,'IsFixedDynamic'] = True
                  df.at[i,'ValidTo']= "31-Dec-2100"  
              if( pd.isna(df.at[i,'TariffEndsOn'])):
                  ErrorMessage = "NULL value Detected in TariffEndson"
                  DQ.LogError(CurrentRowId,ColName,ErrorCode,ErrorMessage)
                  df.at[i,'IsError']=True  # Error rows being marked as True           
          if((df.at[i,ColName] == 'fixed')):
              for j in searchfor:
                  if j in str(df.at[i,'TariffEndsOn']):
                    arer = (lambda x: x if any(i in x for i in searchfor) else None)
                    out = arer(df.at[i,'TariffEndsOn'])
                    df.loc[i,'ValidTo']=out
                    df.at[i,'IsFixedDuration'] = True
                    df.at[i,'IsFixedDynamic'] = False
                    df.at[i,'FixedMonths'] =0
              
             
      except ValueError as e:
          errorMsg = df.at[i,'TariffType']+' '+e  #you can rearrange the error message
          DQ.LogError(CurrentRowId,ColName,ErrorCode,errorMsg)
          df.at[i,'IsError']=True  ## Error rows being marked as True
    
  df['FixedMonths'] = df['FixedMonths'].fillna(int(0))
  df['FixedMonths']=df['FixedMonths'].astype(int)
  df['RequestId']=0
  df['SupplierName']=None
  df['StandingCharge']=0
  df['UnitRate']=0
  df['Green']=0
  df['Discounts']=0
  df['NightUnitRate']=0
  df['PCWSiteId']=0
  df['ExitFees']=0
  df.drop(df[df['IsError']==True].index, inplace = True) # Dropping all rows that have error from dataframe
  df=df.reset_index()
  df.FixedMonths = pd.to_numeric(df.FixedMonths,errors='coerce')  #imp for all the functions
  finalDF = df.to_json(orient='records')             #convert df to json format. Imp step before calling update
  DQ.UpdateExcTariffsDiff('TariffType',finalDF)
  print("Tariff Done")

#4. Cleaning UnitRate
def UnitRate(PCWId,ReqId):
  PCWId_local = PCWId
  ReqId_local = ReqId
  ColName = 'UnitRate'

  df = DQ.GetDataFrame(ColName,PCWId_local,ReqId_local)  #returns RowId and ColumnName data 
  df = df.replace(r'^\s*$', np.nan, regex=True)

  df["UnitRate"] = df["UnitRate"].str.lower()
  for i, item in enumerate(df['UnitRate']):
        CurrentRowId = int(df.RowIdExclusiveTariffs[i])
        ErrorCode = 1
        try:
            if(df.at[i,ColName]=='' or pd.isna(df.at[i,ColName])):
                ErrorMessage = "NULL value Detected in UnitRate"
                DQ.LogError(CurrentRowId,ColName,ErrorCode,ErrorMessage)
                df.at[i,'IsError']=True  # Error rows being marked as True
            if "night" in str(df.at[i,'UnitRate']):
                if "&&" in str(df.at[i,'UnitRate']):
                    string1=df.at[i,'UnitRate'].split("&&",1)
                    df.at[i,'UnitRate']=string1[0]
                if "and" in str(df.at[i,'UnitRate']):
                    string1=df.at[i,'UnitRate'].split("and",1)
                    df.at[i,'UnitRate']=string1[0]
                if "night" in str(df.at[i,'UnitRate']):
                    string =df.at[i,'UnitRate'].split("night",1)
                    df.at[i,'UnitRate']=string[0]
            df.at[i,'UnitRate'] = re.sub("[^0123456789\.]","",str(df.at[i,'UnitRate']))
            df.at[i,'UnitRate'] = pd.to_numeric(df.at[i,'UnitRate'])
        except ValueError as e:
            e=str(e)
            errorMsg = df.at[i,'UnitRate']+' '+e
            errorMsg = df.at[i,'UnitRate']+' '+e  #you can rearrange the error message
            DQ.LogError(CurrentRowId,ColName,ErrorCode,errorMsg)
            df.at[i,'IsError']=True  # Error rows being marked as True

  df['UnitRate'] = df['UnitRate'].fillna(0)   #filling NAN to 0, do this for all the functions
  df['RequestId']=0
  df['SupplierName']=None
  df['TariffName']=None
  df['StandingCharge']=0
  df['ExitFees']=0
  df['Green']=0
  df['Discounts']=0
  df['NightUnitRate']=0
  df['PCWSiteId']=0
  df.drop(df[df['IsError']==True].index, inplace = True)  #Dropping all error rows from dataframe
  df=df.reset_index()
  df.UnitRate = pd.to_numeric(df.UnitRate,errors='coerce')  #imp for all the functions
  df = df[['RowIdExclusiveTariffs','RequestId','SupplierName','TariffName','StandingCharge','UnitRate','NightUnitRate',
              'Green','Discounts','ExitFees','PCWSiteId']]  
  finalDF = df.to_json(orient='records')             #convert df to json format. Imp step before calling update
  DQ.UpdateExcTariffs(ColName,finalDF)
  print("Unit Rate Done")

 
#5. Cleaning PaymentMethod
def PaymentMethod(PCWId,ReqId):  #take from lookup
    ReqId_local = ReqId
    PCWId_local = PCWId
    ColName = 'PaymentMethod'
    
    df = DQ.GetDataFrame(ColName,PCWId_local,ReqId_local)
    df['PaymentMethod'].str.lower()
    PaymentTypes = ['Monthly Direct Debit','Prepayment','Cash and Cheque','Quarterly Direct Debit']
    df['PaymentMethod'] = df['PaymentMethod'].str.replace('MonthlyDirectDebit', 'Monthly Direct Debit')
    df['PaymentMethod'] = df['PaymentMethod'].str.replace('Fixed Direct Debit', 'Monthly Direct Debit')
    df['PaymentMethod'] = df['PaymentMethod'].str.replace('monthly direct debit', 'Monthly Direct Debit')
    df['PaymentMethod'] = df['PaymentMethod'].str.replace('Monthly Fixed Direct Debit', 'Monthly Direct Debit')
    df['PaymentMethod'] = df['PaymentMethod'].str.replace('PrePaymentMeter', 'Prepayment')
    df['PaymentMethod'] = df['PaymentMethod'].str.replace('QuarterlyDirectDebit', 'Quarterly Direct Debit')
    df['PaymentMethod'] = df['PaymentMethod'].str.replace('CashOrCheque', 'Cash and Cheque')              #  confirm from ashton     
    df['PaymentMethod'] = df['PaymentMethod'].str.replace('Pay On Receipt Of Bill', 'Quarterly Cash/Cheque')    #  confirm from ashton 
    df['PaymentId']=None

    for i, item in enumerate(df['PaymentMethod']):
        CurrentRowId = int(df.RowIdExclusiveTariffs[i])
        ErrorCode = 1
        ErrorMessage = "Unexpected Data for Payment method"
        try:
            if(df.at[i,ColName] == PaymentTypes[0]):
                df.at[i,'PaymentId']=1
            elif(df.at[i,ColName] == PaymentTypes[1]):
                df.at[i,'PaymentId']=2
            elif(df.at[i,ColName] == PaymentTypes[2]):
                df.at[i,'PaymentId']=3
            elif(df.at[i,ColName] == PaymentTypes[2]):
                df.at[i,'PaymentId']=4
        except ValueError as e:
            DQ.LogError(CurrentRowId,ColName,ErrorCode,e)
            df.at[i,'IsError']=True  # Error rows being marked as True

    df['RequestId']=0
    df['SupplierName']=None
    df['TariffName']=None
    df['StandingCharge']=0
    df['ExitFees']=0
    df['Green']=0
    df['Discounts']=0
    df['NightUnitRate']=0
    df['PCWSiteId']=0
    df['UnitRate']=0
    df.drop(df[df['IsError']==True].index, inplace = True)  # Dropping all rows that have error from data frame
    df=df.reset_index()
    df = df[['RowIdExclusiveTariffs','RequestId','SupplierName','TariffName','StandingCharge','UnitRate','NightUnitRate',
              'Green','Discounts','ExitFees','PCWSiteId','PaymentId']]  
    finalDF = df.to_json(orient='records')             #convert df to json format. Imp step before calling update
    DQ.UpdateExcTariffs(ColName,finalDF)
    print("Payment method done")


#6. Cleaing ExitFees
def ExitFees(PCWId,ReqId): 
    #put comments saying which pcw site id is which pcw
    #conditions based on PCWSiteId. if site 5 then check Isdualfuel=2 then just divide the number on Exit fees
    PCWId_local = PCWId
    ReqId_local = ReqId
    ColName = 'ExitFees'
    df = DQ.GetDataFrame(ColName,PCWId_local,ReqId_local)
    df['ExitFees'] = df['ExitFees'].str.replace('no fee', '0')
    df[ColName] = df[ColName].str.lower()

    for i, item in enumerate(df['ExitFees']):
        CurrentRowId = int(df.RowIdExclusiveTariffs[i])
        ErrorCode = 1
        ErrorMessage = "Unexpected Data for Exit Fees"
        try:
            if(df.at[i,ColName]=='' or pd.isna(df.at[i,ColName])):
                ErrorMessage = "NULL value Detected in ExitFees"
                DQ.LogError(CurrentRowId,ColName,ErrorCode,ErrorMessage)
                df.at[i,'IsError']=True  # Error rows being marked as True
            if(df.at[i,ColName]=='not applicable' or pd.isna(df.at[i,ColName])):
                df.at[i,ColName]="0"        
            if "per fuel" in df.at[i,'ExitFees']:
                    df.at[i,'ExitFees'] = re.sub("[^0123456789\.]","",df.at[i,'ExitFees'])
            if PCWId==5 or PCWId==6:
                if df.at[i,'IsDualFuel']==1:
                    df.at[i,'ExitFees'] = re.sub("[^0123456789\.]","",df.at[i,'ExitFees'])
                    df.at[i,'ExitFees'] = pd.to_numeric(df.at[i,'ExitFees'])/2
                else:
                    df.at[i,'ExitFees'] = re.sub("[^0123456789\.]","",df.at[i,'ExitFees'])
            else:
                    df.at[i,'ExitFees'] = re.sub("[^0123456789\.]","",df.at[i,'ExitFees'])
        except ValueError as e:
           DQ.LogError(CurrentRowId,ColName,ErrorCode,e)
           df.at[i,'IsError']=True # Error rows being marked as True

    df.ExitFees = pd.to_numeric(df.ExitFees)
    df['RequestId']=0
    df['SupplierName']=None
    df['TariffName']=None
    df['StandingCharge']=0
    df['UnitRate']=0
    df['Green']=0
    df['Discounts']=0
    df['NightUnitRate']=0
    df['PCWSiteId']=0
    df.drop(df[df['IsError']==True].index, inplace = True)  # Dropping all eror rows from data frame
    df=df.reset_index()
    df.ExitFees = pd.to_numeric(df.ExitFees)
    df = df[['RowIdExclusiveTariffs','RequestId','SupplierName','TariffName','StandingCharge','UnitRate','NightUnitRate',
              'Green','Discounts','ExitFees','PCWSiteId']]
    finalDF = df.to_json(orient='records')             #convert df to json format. Imp step before calling update
    DQ.UpdateExcTariffs(ColName,finalDF)
    print("Exit fees done")
 
   

  
#7. Cleaning NightUnitRate
def NightUnitRateTest(PCWId,ReqId):
    PCWId_local = PCWId
    ReqId_local = ReqId
    ColName = 'NightUnitRate'
    mixed=0
    df = DQ.GetDataFrame(ColName,PCWId_local,ReqId_local)  #returns RowId and ColumnName data
    df = df.replace(r'^\s*$', np.nan, regex=True)  #converting empty strings to NaN
    for i, item in enumerate(df['NightUnitRate']): 
        CurrentRowId = int(df.RowIdExclusiveTariffs[i])
        ErrorCode = 1
        try:
            if ((df.at[i,'HasElectricity']==1) and (df.at[i,'FuelTypeId']==2) and (df.at[i,'IsEconomy7']==1)):  
                # getting the values from mixed string with &&
                if ("&&" in str(df.at[i,'NightUnitRate'])) and (mixed == 0) :
                  string1=df.at[i,'NightUnitRate'].split("&&",1)
                  df.at[i,'NightUnitRate']=string1[1]
                  mixed = 1
                # getting the values from mixed string with &&
                if ("night" in str(df.at[i,'NightUnitRate'])) and (mixed == 0):
                    string =df.at[i,'NightUnitRate'].split("night",1)
                    df.at[i,'NightUnitRate']=string[1]
                mixed = 0
                if df.at[i,'NightUnitRate'] == " " or pd.isna(df.at[i,'NightUnitRate']): 
                     ErrorMessage = "Unexpected Data for Night Unit Rate"
                     DQ.LogError(CurrentRowId,ColName,ErrorCode,ErrorMessage)
                     df.at[i,'IsError']=True  # Error rows being marked as True
                else:
                      df.at[i,'NightUnitRate'] = re.sub("[^0123456789\.]","",str(df.at[i,'NightUnitRate']))
                  
            elif (df.at[i,'HasGas']==1 and df.at[i,'FuelTypeId']==1):
                if df.at[i,'NightUnitRate'] == " " or pd.isna(df.at[i,'NightUnitRate']):            
                    pass
                else:
                  df.at[i,'NightUnitRate'] = 0   
           
            elif ((df.at[i,'HasElectricity']==1) and (df.at[i,'FuelTypeId']==2) and (df.at[i,'IsEconomy7']==0)): 
                  df.at[i,'NightUnitRate'] = 0
                  
        except ValueError as e:
            errorMsg = df.at[i,'NightUnitRate']+' '+e  #you can rearrange the error message
            DQ.LogError(CurrentRowId,ColName,ErrorCode,errorMsg)
            df.at[i,'IsError']=True  # Error rows being marked as True

    df['RequestId']=0
    df['SupplierName']=None
    df['TariffName']=None
    df['StandingCharge']=0
    df['UnitRate']=0
    df['Green']=0
    df['Discounts']=0
    df['ExitFees']=0
    df['PCWSiteId']=0
    df.drop(df[df['IsError']==True].index, inplace = True) # Dropping all error rows from data frame
    df=df.reset_index()
    df = df[['RowIdExclusiveTariffs','RequestId','SupplierName','TariffName','StandingCharge','UnitRate','NightUnitRate',
              'Green','Discounts','ExitFees','PCWSiteId']]   #pass all the columns as per select...from json in ExcTariffsUpdateQuery.txt
    #print(type(df['NightUnitRate']))
    df.NightUnitRate = pd.to_numeric(df.NightUnitRate,errors='coerce')  #imp for all the functions
    finalDF = df.to_json(orient='records')             #convert df to json format. Imp step before calling update
    DQ.UpdateExcTariffs(ColName,finalDF)
    print("NightRate Done")

#8. Cleaning StandingCharge
def StandingCharge(PCWId,ReqId):
        PCWId_local = PCWId
        ReqId_local = ReqId
        ColName = 'StandingCharge'
        df = DQ.GetDataFrame(ColName,PCWId_local,ReqId_local) 
        searchList=['months from supply start','has no end date']
        search_exist=0
        for i, item in enumerate(df[ColName]):
            CurrentRowId = int(df.RowIdExclusiveTariffs[i])
            ErrorCode = 1
            try:
               if df[ColName].str.contains('\(').any():  
                    string_new=str(df.at[i,ColName]).split(" ",1)
                    df.at[i,ColName]=string_new[0]
               if(df.at[i,ColName]=='' or pd.isna(df.at[i,ColName])):
                     ErrorMessage = "NULL value Detected in Standing Charge"
                     DQ.LogError(CurrentRowId,ColName,ErrorCode,ErrorMessage)
                     df.at[i,'IsError']=True # Marking Error rows as True
               else:
                    for j in searchList:
                        if(j in str(df.at[i,ColName])):
                            ErrorMessage = "Unexpected Data for Standing Charge"
                            DQ.LogError(CurrentRowId,ColName,ErrorCode,ErrorMessage)
                            df.at[i,'IsError']=True # Marking error rows as True
                            search_exist=1
                    if (search_exist==0):
                        df.at[i,ColName] = re.sub("[^0123456789\.]","",df.at[i,ColName])
               search_exist=0      
            except ValueError as e:
                errorMsg = df.at[i,'StandingCharge']+' '+e  #you can rearrange the error message
                DQ.LogError(CurrentRowId,ColName,ErrorCode,errorMsg)
                df.at[i,'IsError']=True # Marking Error rows as True

        df['RequestId']=0
        df['SupplierName']=None
        df['TariffName']=None
        df['NightUnitRate']=0
        df['UnitRate']=0
        df['Green']=0
        df['Discounts']=0
        df['ExitFees']=0
        df['PCWSiteId']=0
        df.drop(df[df['IsError']==True].index, inplace = True)
        df=df.reset_index()
        df.StandingCharge = pd.to_numeric(df.StandingCharge,errors='coerce')
        df = df[['RowIdExclusiveTariffs','RequestId','SupplierName','TariffName','StandingCharge','UnitRate','NightUnitRate',
                'Green','Discounts','ExitFees','PCWSiteId']]
        finalDF = df.to_json(orient='records')             #convert df to json format. Imp step before calling update
        DQ.UpdateExcTariffs(ColName,finalDF)
        print("StandingCharge Done")