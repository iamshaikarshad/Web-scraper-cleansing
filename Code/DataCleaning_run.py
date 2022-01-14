import DataCleaning_queries as DQ
import DataCleaning_Cleansing as DC
#import DataCleaning_statusEmail as DE #to be created


def runProcess():
    try:
        #Fetching parameters to loop on
        ReqIdPCWId   =   DQ.GetReqIdPCWId() 
        print(ReqIdPCWId)
        DQ.IniTariffCleaning() 
        for i, row in ReqIdPCWId.iterrows():
            ReqId = int(row['RequestId'])  
            PCWId = int(row['PCWSiteId'])  
            #calling individual functions for cleaning        
            DC.TariffType(PCWId,ReqId)   
            DC.PaymentMethod(PCWId,ReqId)
            DC.ExitFees(PCWId,ReqId)                            
            DC.UnitRate(PCWId,ReqId)     
            DC.NightUnitRateTest(PCWId,ReqId) 
            DC.StandingCharge(PCWId,ReqId)
            DC.isGreen(PCWId,ReqId)  
            DC.Discount(PCWId,ReqId)     
            #send summary mail
            
    except Exception as e:
        error = str(e)
        print(error)
        #  DE.sendAlert(error)
runProcess()
