from pyspark.sql import SparkSession
import json
import requests
import datetime
import hashlib
import hmac
import base64



#Retrieve your Log Analytics Workspace ID from your Key Vault Databricks Secret Scope
wks_id = "6ab80183-9cf3-47ed-b938-24e5cf3e294e"

#Retrieve your Log Analytics Primary Key from your Key Vault Databricks Secret Scope
wks_shared_key = "oikQO5e+DpBN8RdnBcQ7V6HVazKUBw2ITjwh+CSYcdPv2tW/XGU35xcy73exmPE4vcm5+kYBUWc1GtJ8qbwMYQ=="

#Build the API signature
def build_signature(customer_id, shared_key, date, content_length, method, content_type, resource):
    x_headers = 'x-ms-date:' + date
    string_to_hash = method + "\n" + str(content_length) + "\n" + content_type + "\n" + x_headers + "\n" + resource
    bytes_to_hash = str.encode(string_to_hash,'utf-8')  
    decoded_key = base64.b64decode(shared_key)
    encoded_hash = (base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest())).decode()
    authorization = "SharedKey {}:{}".format(customer_id,encoded_hash)
    return authorization

#Build and send a request to the POST API
def post_data(customer_id, shared_key, body, log_type):
    method = 'POST'
    content_type = 'application/json'
    resource = '/api/logs'
    rfc1123date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
    content_length = len(body)
    signature = build_signature(wks_id, shared_key, rfc1123date, content_length, method, content_type, resource)
    uri = 'https://' + wks_id + '.ods.opinsights.azure.com' + resource + '?api-version=2016-04-01'
    
    headers = {
        'content-type': content_type,
        'Authorization': signature,
        'Log-Type': log_type,
        'x-ms-date': rfc1123date
    }

    print("uri :",uri)
    print("body :",body)
    print("headers:",headers)
    response = requests.post(uri,data=body, headers=headers)
    if (response.status_code >= 200 and response.status_code <= 299):
        print ('Accepted')
    else:
        print ("Response code: {}".format(response.status_code))
        print("Reason: {}".format(response.reason))
        print("Reason: {}".format(response.text))
        print("Reason: {}".format(response.content))


class TaskLoggin():
    
    def LogAnalytics(self,log_type,jobid,pipelinename,fn_name,runid,tasktype,task_name,level_num,status,param,i_param,o_param,cluster_id,log_category,log_message,task_start_ts,task_end_ts):
        
        """ json_data = {}
            json_data['JobID'] = jobid
            json_data['PipelineName'] = pipelinename
            json_data['fn_name'] = fn_name
            json_data['RunID'] = runid
            json_data['TaskType'] = tasktype
            json_data['Task'] = task_name
            json_data['Level_Num'] = level_num
            json_data['Params'] = status
            json_data['In_Param'] = param
            json_data['Out_Params'] = i_param
            json_data['Cluster_Id'] = o_param
            json_data['JobID'] = cluster_id
            json_data['Log_Category'] = log_category
            json_data['Log_Message'] = log_message
            json_data['task_start_ts'] = task_start_ts
            json_data['task_end_ts'] = task_end_ts 
        """

        json_data = [{"JobID": jobid,"PipelineName": pipelinename,"fnname": fn_name,"RunID": runid, \
            "TaskType": tasktype,"Task": task_name, \
            "Level_Num": level_num,"Status": status,"Params": param,"InParam": i_param, \
            "OutParams": o_param,"ClusterId": cluster_id,"LogCategory": log_category,  \
            "LogMessage": log_message,"taskstart_ts": task_start_ts,"taskend_ts": task_end_ts  }]

        print(json_data)
        #Post the log
        body = json.dumps(json_data) 
        post_data(wks_id, wks_shared_key, body, log_type)
        