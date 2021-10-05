import json
import boto3
import requests as req
import csv
import xml.etree.ElementTree as ET
import os
#import pandas as pd
 

def lambda_handler(event, context):
    logonToken = exporttoken()
    
    start_date="20170301" 
    end_date = "20170331"
    
    refreshdoc(logonToken, start_date, end_date)
    
    exportDoc(logonToken, start_date, end_date)
    
    #convertToCSV('/tmp/reports.xls')
    
    uploadFile('/tmp/reports.xls',end_date)
    
    expiretoken(logonToken)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Success in api call and uploading data in s3!')
    }
    

# --------------------------------------
# Get parameter from AWS parameter store
# --------------------------------------

def getParameter(paramName):
    ssm = boto3.client('ssm')
    parameter = ssm.get_parameter(Name=paramName, WithDecryption=True)
    return parameter['Parameter']['Value']

    
# ----------------------------------
# Call SAP to generate token for API
# ----------------------------------

def exporttoken():
    global response
   
    apiBaseUrl = getParameter('SAP_BO_API_DEV_URL')
    apiUser = getParameter('SAP_BO_API_DEV_USER')
    apiPassword = getParameter('SAP_BO_API_DEV_PASSWORD')
    
    url = apiBaseUrl + 'biprws/logon/long'
    
    headers = {
        "Content-Type" : "application/xml"
    }
    
    
    xmls_part1 = '<attrs xmlns="http://www.sap.com/rws/bip"><attr name="userName" type="string">';
    user = apiUser;
    xmls_part2 = '</attr><attr name="password" type="string">';
    password = apiPassword;
    xmls_part3 = '</attr><attr name="auth" type="string" possibilities="secEnterprise,secLDAP,secWinAD,secSAPR3">secEnterprise</attr></attrs>';
    xmls = xmls_part1+user+xmls_part2+password+xmls_part3
    
    response = req.post(url,data=xmls,headers=headers) 
    
    
    
    logonToken = parseXML(response.content)
    
    return logonToken
    
    
    
# ------------------------------------
# Parse XML response to extract token
# ------------------------------------
    
def parseXML(contentString):
    root = ET.fromstring(contentString)
    for child in root:      
        if('content' in child.tag ):
            content = child
            for contentchild in content:
                if('attrs' in contentchild.tag ):
                    attrs = contentchild
                    for attrschild in attrs:
                        return attrschild.text    

# ------------------------------------
# Call SAP API to logoff the token
# ------------------------------------    
def expiretoken(logonToken):
    apiBaseUrl = getParameter('SAP_BO_API_DEV_URL')
    apiUser = getParameter('SAP_BO_API_DEV_USER')
    apiPassword = getParameter('SAP_BO_API_DEV_PASSWORD')
    url = apiBaseUrl+'biprws/logoff'
    
    headers = {
        "Content-Type" : "application/xml",
        'Accept': 'application/xml',
        'X-SAP-LogonToken': logonToken
    }
    
    xmls_part1 = '<attrs xmlns="http://www.sap.com/rws/bip"><attr name="userName" type="string">';
    user = apiUser;
    xmls_part2 = '</attr><attr name="password" type="string">';
    password = apiPassword;
    xmls_part3 = '</attr><attr name="auth" type="string" possibilities="secEnterprise,secLDAP,secWinAD,secSAPR3">secEnterprise</attr></attrs>';
    xmls = xmls_part1+user+xmls_part2+password+xmls_part3;
    
    response = req.post(url,data=xmls,headers=headers) 
    print(response.content)
    
    if response.status_code == 200:
        print('Success ExpireToken!')
    elif response.status_code == 404:
        print('Not Found.')
    else:
        print('Error in ExpireToken')
        
# ---------------------------
# Perform refresh SAP report
# ---------------------------

def refreshdoc(logonToken, start_date, end_date):
    apiBaseUrl = getParameter('SAP_BO_API_DEV_URL')
    url = apiBaseUrl+'biprws/raylight/v1/documents/837629/parameters?formattedValues=true'
    dataParam_part1 = '{"parameters": {"parameter": {"id": 0,"answer": {"values": {"value": [';
    dataParam_part2 = ']}}},"parameter": {"id": 1,"answer": {"values": {"value": [';
    dataParam_part3 = ']}}}}}';
    dataParam = dataParam_part1 + start_date + dataParam_part2 + end_date + dataParam_part3;

    headers = {
        'Content-Type': 'application/json',
        'X-SAP-LogonToken': logonToken,
        'Accept': 'application/json'
        };

    print("Starting doc refresh.."+logonToken);

    response = req.put(url, dataParam, headers=headers) 
            
    if response.status_code == 200:
        print('Success refreshdoc!')
    elif response.status_code == 404:
        print('Not Found.')
    else:
        print('Error in refreshdoc') 
    
    
# ----------------------------------------------
# Call SAP API and save the response in xls file
# ----------------------------------------------

def exportDoc(logonToken, start_date, end_date):
    apiBaseUrl = getParameter('SAP_BO_API_DEV_URL')
    url = apiBaseUrl +'biprws/raylight/v1/documents/837629'
    
    dataParam_part1 = '{"parameters": {"parameter": {"id": 0,"answer": {"values": {"value": [';
    dataParam_part2 = ']}}},"parameter": {"id": 1,"answer": {"values": {"value": [';
    dataParam_part3 = ']}}}}}'; 
    dataParam = dataParam_part1 + start_date + dataParam_part2 + end_date + dataParam_part3;
    
    
    headers = {
        'Content-Type': 'application/json',
         'X-SAP-LogonToken': logonToken,
         'Accept': 'application/vnd.ms-excel' 
        };

    response = req.get(url, dataParam, headers=headers) 
    
    with open('/tmp/reports.xls','wb') as f:
        f.write(response.content)
    
    file_size = os.path.getsize('/tmp/reports.xls')
    print("File Size is :", file_size, "bytes")


# ------------------------
# Conver xls to csv
# ------------------------    

#def convertToCSV(fileName):
#    read_file = pd.read_excel(fileName)
#    read_file.to_csv('/tmp/'+ "/reports.csv",index = None,header=True)
    
    
# ------------------------
# Upload File to S3 bucket
# ------------------------    
    
def uploadFile(fileName, end_date):
    s3 = boto3.client('s3')
    bucket_name = 'mysaptest1'
    s3.upload_file(fileName, bucket_name, 'reports_'+end_date+'.xls')
    
    

    
    
    
    