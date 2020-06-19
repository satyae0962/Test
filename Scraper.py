import requests
from datapackage import Package
import csv
import pandas as pd
import json
from pyspark.sql import SparkSession
from pandas.io.json import json_normalize
from datetime import datetime

# Global variable 
# List of urls
url = ['https://datahub.io/core/oil-prices/datapackage.json','https://datahub.io/core/oil-prices#data-cli']
spark = SparkSession.builder.getOrCreate()
spark.conf.set("fs.azure.account.key.scmdevdpdatalake.dfs.core.windows.net", "xy+w+9tLBMRso0+DQQjnHSDX4KE8CPZmM6oIUSsAenef3IK5rsa7nm1Gk4UDS6sIVE/dEQe1hLMk6YTIEsEnGQ==")

def no_threads():
    threads =len(url)
    print("Number of threads : " +str(threads))
    
def datapackage_scrape():
    """
    Function scrapes data for the given url using.
    BRENT_CRUDE_USD
    WTI_USD
    """
    curr_date = datetime.today().strftime('%Y-%m-%d')
    filename = "wit_{0}".format(curr_date)
    print("Filename : " +str(filename))
    package = Package('https://datahub.io/core/oil-prices/datapackage.json')
    print(package.resource_names)
    csv_list = ['brent-daily_csv','wti-daily_csv']
    for i in csv_list:
        for resource in package.resources:
            if resource.descriptor['datahub']['type'] == 'derived/csv' and resource.descriptor['name'] == '{0}'.format(i):
                data = resource.read()
                print(len(data))
                df = pd.DataFrame(data,columns=['date','price'])
                print(df)
                spark_df = spark.createDataFrame(df)
                spark_df.coalesce(1).write.format("csv").option("header", True).mode("overwrite").save("/mnt/dplake/data/ThirdParty/datahubApi/{0}".format(i))
                
        #with open(r"C:\Users\satya.narayana\Desktop\Data PlatForm\Crude_Oil_Data_new\{0}".format(i),'w', newline='') as file:
        #with open("/dbfs/FileStore/tables/treaded-etl/{0}".format(i),'w',newline='') as file:
            #writer = csv.writer(file)
            #writer.writerows(data)
            
    
def oil_price_api():
    """
    add current timestamp to the final file
    """
    #url = "https://api.oilpriceapi.com/v1/prices/latest"
    file_path = "/mnt/dplake/data/ThirdParty/oilpriceApi"
    curr_date = datetime.today().strftime('%Y-%m-%d')
    filename = "wit_{0}".format(curr_date)
    print("Filename : " +str(filename))
    url = "https://api.oilpriceapi.com/v1/prices/latest/?by_code=WTI_USD"
    api_token = "98b35e0e416a785ba81f6111739f718a"
    headers = {"Content-type": "application/json", "Authorization": "Token %s" % api_token}
    latest_price = requests.get(url,headers=headers)
    print(latest_price.text)
    json_price = json.loads(latest_price.text)
    final_price = json_price['data']
    status = json_price['status']
    if status == 'success':
        prices_data  = json.loads(json.dumps(final_price))
        print(final_price)
        temp_df = json_normalize(prices_data)
        spark_df = spark.createDataFrame(temp_df)
        run_time = datetime.now()
        #spark_df.withColumn("run_time",run_time)
        spark_df.coalesce(1).write.format("csv").option("header", True).mode("append").save("/mnt/dplake/data/ThirdParty/oilpriceApi/{0}".format(filename))        
    else:
        print("Unable to retrieve data")
        
if __name__ == "__main__":
    no_threads()
    datapackage_scrape()
    oil_price_api()