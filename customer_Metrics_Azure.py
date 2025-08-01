import json
import os
import logging
import sys
import time
from datetime import datetime, timezone, timedelta
import pytz
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib
import numpy as np
import pandas as pd
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
from azure.core.exceptions import HttpResponseError
from azure.identity import DefaultAzureCredential, ClientSecretCredential
import requests
 
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
 
logger = logging.getLogger(__name__)
 
SEND_MAILS = []
cumulative_message_count, khoros_api_count, sourceCommunityName, targetCommunityName, scheduleType, customerNames, CustomerTenantIdsCount = [], [], [], [], [], [], []
customerNames_request = []
customer_ids = []
required_times = []
totalPercentageResourceValueByCustomer = []
WeightageColumnForCustomer = []
ESRI_CLASSIC_REQUEST_COUNT = ""
 
WORKSPACE_ID = os.getenv("WORKSPACE_ID")
REQUIRED_MAILS = os.getenv("REQUIRED_MAILS")
CRON_PASSWD = os.getenv("CRON_PASSWD")
MANAGED_IDENTITY_CLIENT_ID = os.getenv("MANAGED_IDENTITY_CLIENT_ID")
METRICS_URL = os.getenv("METRICS_URL")
REQUIRED_DAYS = os.getenv("REQUIRED_DAYS")
ITERATIONS_REQUIRED = os.getenv("ITERATIONS_REQUIRED")
CLUSTER_ENV = os.getenv("CLUSTER_ENV")
 
# To get list of mails
mail = REQUIRED_MAILS.split(",")
 
for to_mails in mail:
    SEND_MAILS.append(to_mails)
 
 
 
def managedIdentity():
 
    managed_identity_access_token = DefaultAzureCredential(managed_identity_client_id=MANAGED_IDENTITY_CLIENT_ID).get_token("https://graph.microsoft.com/.default").token
    return managed_identity_access_token
 
 
# timestamp to send  2023-12-04T05:15:08.070Z
# get call
# Yes it will be get call, ex-->
# /getconnectordetailsbycustomerid/{customerId}?fromDate=2023-12-04T05:15:08.070Z&toDate=2023-12-09T05:15:08.070Z
 
 
def getConnectorDetailsByCustomerId(communityId, from_time_ist, to_time_ist, access_token, tenant_id, customer_name):
    logger.info(
        f"Fetching messages count for community id {communityId} with tenant id {tenant_id} from {from_time_ist} to {to_time_ist} for customer {customer_name}")
 
    # initiate request by passing CustomerId, From and To timestamps
    metrics_url = f"{METRICS_URL}/smartconxstatics/{communityId}/{from_time_ist}/{to_time_ist}/{tenant_id}"
 
    headers = {
        "Authentication-Provider": "aad",
        "Access-Token": access_token
    }
 
    metrics_response = requests.get(url=metrics_url, headers=headers, verify="/app/.cer")
    logger.info(f"metrics url {metrics_url}")
    logger.info(f"status code {metrics_response.status_code}")
    message_counts = 0
    khoros_api_count_cumulative_value = 0
    if metrics_response.status_code == 200 and metrics_response.json():
        logger.info("success response")
            
        logger.info(f"customers count response for customer {customer_name} {metrics_response.json()}")
        khoros_api_count_cumulative_value += int(metrics_response.json()["khorosAPICounts"]["userRoles"])
        khoros_api_count_cumulative_value += int(metrics_response.json()["khorosAPICounts"]["nodes"])
        khoros_api_count_cumulative_value += int(metrics_response.json()["khorosAPICounts"]["boardRoles"])
        khoros_api_count_cumulative_value += int(metrics_response.json()["khorosAPICounts"]["messages"])
        khoros_api_count_cumulative_value += int(metrics_response.json()["khorosAPICounts"]["users"])
        khoros_api_count_cumulative_value += int(metrics_response.json()["khorosAPICounts"]["privateMessages"])
        
        logger.info(f"khoros api count for customer {customer_name} {khoros_api_count_cumulative_value}")
        
        
        for required_response in metrics_response.json()["items"]:
 
            message_counts += int(required_response["messageCount"])
            
        logger.info(f"total message count for customer {customer_name} is {message_counts}")
        
        khoros_api_and_messages_count = [khoros_api_count_cumulative_value, message_counts]
        logger.info(f"Return messages count and khoros api count {khoros_api_and_messages_count}")
        return khoros_api_and_messages_count
    else:
        logger.info(f"status code is not 200, response status code: {metrics_response.status_code}")
        logger.info(f"retrying one more time for metrics url {metrics_url}")
        time.sleep(60)
        metrics_response = requests.get(url=metrics_url, headers=headers, verify="/app/.cer")
        logger.info(f"metrics url {metrics_url}")
        logger.info(f"status code {metrics_response.status_code}")
        if metrics_response.status_code == 200 and metrics_response.json():
            logger.info("success response")
                
            logger.info(f"customers count response for customer {customer_name} {metrics_response.json()}")
            khoros_api_count_cumulative_value += int(metrics_response.json()["khorosAPICounts"]["userRoles"])
            khoros_api_count_cumulative_value += int(metrics_response.json()["khorosAPICounts"]["nodes"])
            khoros_api_count_cumulative_value += int(metrics_response.json()["khorosAPICounts"]["boardRoles"])
            khoros_api_count_cumulative_value += int(metrics_response.json()["khorosAPICounts"]["messages"])
            khoros_api_count_cumulative_value += int(metrics_response.json()["khorosAPICounts"]["users"])
            khoros_api_count_cumulative_value += int(metrics_response.json()["khorosAPICounts"]["privateMessages"])
            
            logger.info(f"khoros api count for customer {customer_name} {khoros_api_count_cumulative_value}")
            
            
            for required_response in metrics_response.json()["items"]:
 
                message_counts += int(required_response["messageCount"])
                
            logger.info(f"total message count for customer {customer_name} is {message_counts}")
            
            khoros_api_and_messages_count = [khoros_api_count_cumulative_value, message_counts]
            logger.info(f"Return messages count and khoros api count {khoros_api_and_messages_count}")
            return khoros_api_and_messages_count
 
 
def send_mail(email_body):
    logger.info("Sending Mail Report")
    # smtp connections:
    smtp_server = "smtp.office365.com"
    smtp_port = 587
    username = ".com"
    password = f"{CRON_PASSWD}"
    from_addr = ".com"
    to_addr = SEND_MAILS
    subject = f"iTalent {CLUSTER_ENV} usage for each customer"
 
    message = MIMEMultipart()
    message["From"] = from_addr
    message["To"] = ", ".join(to_addr)
    message["Subject"] = subject
 
    message.attach(MIMEText(email_body, "html"))
 
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(user=username, password=password)
        server.send_message(message)
        logger.info("Email with attachment sent successfully.")
 
 
def getFrontDoorRequestCount(tenant_ids, from_time_ist, to_time_ist):
 
    # required Time Generated Format to be  | where TimeGenerated between (datetime("2023-12-06 10:50:00") .. datetime("2023-12-06 11:30:00"))
    # convert to UTC because KQL only accepts UTC timezones
    convert_to_utc_from_time = from_time_ist - timedelta(minutes=330)
 
    convert_to_utc_to_time = to_time_ist - timedelta(minutes=330)
 
    # condition to check if list len is 1, because tuple method adds trailing comma for single element list
    # the below logic is helpful
    if len(tenant_ids) == 1:
        tenants_ids_tuple = f"(\"{str(tenant_ids[0])}\")"
    else:
        tenants_ids_tuple = tuple(tenant_ids)
 
    query = f"""
                    AzureDiagnostics
                    | where ResourceProvider == "MICROSOFT.NETWORK" and Category == "FrontdoorAccessLog"
                    | where TimeGenerated between (datetime("{str(convert_to_utc_from_time).split(".")[0]}") .. datetime("{str(convert_to_utc_to_time).split(".")[0]}"))
                    | where requestUri_s has_any {tenants_ids_tuple}    
                    | summarize count = tostring(count())
            """
 
    logger.info(f"KQL query: {query}")
 
    try:
        response = client.query_workspace(
            workspace_id=f"{WORKSPACE_ID}",
            query=query,
            timespan=None
 
        )
        if response.status == LogsQueryStatus.PARTIAL:
            error = response.partial_error
            data = response.partial_data
            logger.info(f"{error}")
 
        elif response.status == LogsQueryStatus.SUCCESS:
            data = response.tables
 
        for required_data in data:
            frontDoorRequests = pd.DataFrame(data=required_data.rows, columns=required_data.columns)
            pd.set_option("display.max_rows", None)
            pd.set_option("display.max_columns", None)
            pd.set_option("display.width", None)
            pd.set_option("display.max_colwidth", None)
 
            count_value = frontDoorRequests.to_json(index=False)
            count_json_value = json.loads(count_value)
            customer_front_door_count = count_json_value["count"]["0"]
            # CustomerTenantIdsCount.append(count_json_value["count"]["0"])
            logger.info(f"Front Door Request Count for customers {customer_front_door_count}")
            return customer_front_door_count
 
    except HttpResponseError as err:
        logger.info("Something fatal happened:")
        logger.info(f"{err}")
 
 
def getCommunityIdsForCustomers(customerId, access_token, customer_name):
    logger.info(f"Fetching commmunity ids for customer name {customer_name} with customer id {customer_id}")
 
    # initiate request by passing CustomerId, From and To timestamps
    metrics_url = f"{METRICS_URL}/smartconxstatics/getcommunities/{customerId}"
 
    headers = {
        "Authentication-Provider": "aad",
        "Access-Token": access_token
    }
 
    metrics_response = requests.get(url=metrics_url, headers=headers, verify="/app/.cer")
    logger.info(f"API url passed: {metrics_url}")
    logger.info(f"status code after response received from Community Ids API Call for the customer {customer_name}: {metrics_response.status_code}")
    logger.info(f"response received from Community Ids API Call for customer {customer_name} is {metrics_response.json()}")
    return metrics_response.json()
 
 
 
def getPreviousDate(get_date):
    logger.info("get previous date")
    previous_date  = datetime.strptime(get_date, "%Y-%m-%dT%H:%M:%S.%f") - timedelta(days=int(REQUIRED_DAYS))
 
    return previous_date
 
 
def percentageOfResourceUsedByCustomer(customers_table):
    totalPercentageMetricValue = [0] * len(customers_table.index)
 
    customers_table["Percentage Usage of Resource"] = totalPercentageMetricValue
    logger.info(f"After Adding Default resource usage Values: {customers_table}")
 
    # get Syndication requests and multiply by 5
    for customers in customers_table["CustomerName"]:
        logger.info(f"Applying Resource Usage Logic for customer {customers}")
        
        if customers == "ESRI":
            logger.info(f"Multiply Syndication Request with no of targets communities for the customer: {customers}")
            
            # for ESRI Classic
            logger.info(f"The Request Count for the ESRI CLASSIC Customer {ESRI_CLASSIC_REQUEST_COUNT}")
            classic_syndication_requests = int(ESRI_CLASSIC_REQUEST_COUNT) * 5 * 7
            
            
            # Khoros_Api_Count is zero for ESRI_CLASSIC, so excluding form below percentage calculation
            esri_classic_percentageMetricValue = int(classic_syndication_requests)
            logger.info(f"Resource Usage count for customer ESRI_CLASSIC: {esri_classic_percentageMetricValue}")
            customers_table = customers_table._append({
                        "CustomerName": "ESRI_CLASSIC",
                        "Syndication Requests": ESRI_CLASSIC_REQUEST_COUNT,
                        "Pulled Messages": "0",
                        "Khoros API Count": "0",
                        "Percentage Usage of Resource": esri_classic_percentageMetricValue
            }, ignore_index=True)
            logger.info(f"After appending ESRI_CLASSIC customer {customers_table}")
 
 
            # for ESRI Share
            ESRI_REQUEST_COUNT = int(customers_table[customers_table["CustomerName"] == customers]["Syndication Requests"].values[0])
            ESRI_SHARE_REQUEST_COUNT = max(0, int(ESRI_REQUEST_COUNT) - int(ESRI_CLASSIC_REQUEST_COUNT))
            logger.info(f"The Request Count for the ESRI SHARE Customer {ESRI_SHARE_REQUEST_COUNT}")
            
            share_syndication_requests = int(ESRI_SHARE_REQUEST_COUNT) * 5
            share_khoros_api_count_value = int(customers_table[customers_table["CustomerName"] == customers]["Khoros API Count"].values[0])
            esri_share_percentageMetricValue = int(share_syndication_requests + share_khoros_api_count_value)
            
            esri_share_cumulative_message_count = customers_table[customers_table["CustomerName"] == customers]["Pulled Messages"].values[0]
            logger.info(f"Resource Usage count for customer ESRI_SHARE: {esri_share_percentageMetricValue}")
            
            customers_table = customers_table._append({
                        "CustomerName": "ESRI_SHARE",
                        "Syndication Requests": ESRI_SHARE_REQUEST_COUNT,
                        "Pulled Messages": esri_share_cumulative_message_count,
                        "Khoros API Count": share_khoros_api_count_value,
                        "Percentage Usage of Resource": esri_share_percentageMetricValue
            }, ignore_index=True)
            logger.info(f"After appending ESRI_SHARE customer {customers_table}")
            
        
        elif customers == "JMP":
            logger.info(f"Multiply Syndication Request with no of targets communities for the customer: {customers}")
            syndication_requests = int(customers_table[customers_table["CustomerName"] == customers]["Syndication Requests"].values[0]) * 5
            khoros_api_count_value = int(customers_table[customers_table["CustomerName"] == customers]["Khoros API Count"].values[0])
            percentageMetricValue = int(syndication_requests + khoros_api_count_value)
            logger.info(f"Resource Usage count for customer {customers}: {percentageMetricValue}")
            customers_table.loc[customers_table['CustomerName'] == customers, 'Percentage Usage of Resource'] = int(percentageMetricValue)
            
            logger.info(f"Customers table before JMP {customers_table}")
            JMP_REQUEST_COUNT = int(customers_table[customers_table["CustomerName"] == customers]["Syndication Requests"].values[0])
            REQUIRED_JMP_REQUEST_COUNT = max(0, int(JMP_REQUEST_COUNT) - int(JMP_CLASSIC_REQUEST_COUNT))
            customers_table.loc[customers_table['CustomerName'] == customers, 'Syndication Requests'] = int(REQUIRED_JMP_REQUEST_COUNT)
 
            # for JMP CLASSIC
            logger.info(f"The Front Door Request Count for the JMP CLASSIC Customer {JMP_CLASSIC_REQUEST_COUNT}")
            # Multiply with 5 for internal calls and 9 target communities
            jmp_classic_syndication_requests = int(JMP_CLASSIC_REQUEST_COUNT) * 5 * 9
 
            # Khoros_Api_Counst is zero for ESRI_CLASSIC, so excluding form below Resource percentage calculation
            jmp_classic_percentageMetricValue = int(jmp_classic_syndication_requests)
            logger.info(f"Resource Usage count for customer JMP_CLASSIC: {jmp_classic_percentageMetricValue}")
            customers_table = customers_table._append({
                        "CustomerName": "JMP_CLASSIC",
                        "Syndication Requests": JMP_CLASSIC_REQUEST_COUNT,
                        "Pulled Messages": "0",
                        "Khoros API Count": "0",
                        "Percentage Usage of Resource": jmp_classic_percentageMetricValue
            }, ignore_index=True)
            logger.info(f"After appending JMP_CLASSIC customer {customers_table}")
 
 
        elif customers == "GoogleEdu":   
            logger.info(f"Multiply Syndication Request with no of targets communities for the customer: {customers}")
            syndication_requests = int(customers_table[customers_table["CustomerName"] == customers]["Syndication Requests"].values[0]) * 5 * 32
            khoros_api_count_value = int(customers_table[customers_table["CustomerName"] == customers]["Khoros API Count"].values[0])
            percentageMetricValue = int(syndication_requests + khoros_api_count_value)
            logger.info(f"Resource Usage count for customer {customers}: {percentageMetricValue}")
            customers_table.loc[customers_table['CustomerName'] == customers, 'Percentage Usage of Resource'] = int(percentageMetricValue)
        
        elif customers == "PANW":
            logger.info(f"Multiply Syndication Request with no of targets communities for the customer: {customers}")
            
            # for PANW Classic
            logger.info(f"The Request Count for the PANW_CLASSIC Customer {PANW_CLASSIC_REQUEST_COUNT}")
            classic_syndication_requests = int(PANW_CLASSIC_REQUEST_COUNT) * 5 * 10
            
            
            # Khoros_Api_Count is zero for PANW_CLASSIC, so excluding form below percentage calculation
            panw_classic_percentageMetricValue = int(classic_syndication_requests)
            logger.info(f"Resource Usage count for customer PANW_CLASSIC: {panw_classic_percentageMetricValue}")
            customers_table = customers_table._append({
                        "CustomerName": "PANW_CLASSIC",
                        "Syndication Requests": PANW_CLASSIC_REQUEST_COUNT,
                        "Pulled Messages": "0",
                        "Khoros API Count": "0",
                        "Percentage Usage of Resource": panw_classic_percentageMetricValue
            }, ignore_index=True)
            logger.info(f"After appending PANW_CLASSIC customer {customers_table}")
 
 
            # for PANW Syndication
            PANW_REQUEST_COUNT = int(customers_table[customers_table["CustomerName"] == customers]["Syndication Requests"].values[0])
            PANW_SYNDICATION_REQUEST_COUNT = max(0, int(PANW_REQUEST_COUNT) - int(PANW_CLASSIC_REQUEST_COUNT))
            logger.info(f"The Request Count for the PANW Syndication Customer {PANW_SYNDICATION_REQUEST_COUNT}")
            
            panw_syndication_requests = int(PANW_SYNDICATION_REQUEST_COUNT) * 5
            panw_syndication_khoros_api_count_value = int(customers_table[customers_table["CustomerName"] == customers]["Khoros API Count"].values[0])
            panw_syndication_percentageMetricValue = int(panw_syndication_requests + panw_syndication_khoros_api_count_value)
            
            panw_syndication_cumulative_message_count = customers_table[customers_table["CustomerName"] == customers]["Pulled Messages"].values[0]
            logger.info(f"Resource Usage count for customer PANW_SYNDICATION: {panw_syndication_percentageMetricValue}")
            
            customers_table = customers_table._append({
                        "CustomerName": "PANW_SYNDICATION",
                        "Syndication Requests": PANW_SYNDICATION_REQUEST_COUNT,
                        "Pulled Messages": panw_syndication_cumulative_message_count,
                        "Khoros API Count": panw_syndication_khoros_api_count_value,
                        "Percentage Usage of Resource": panw_syndication_percentageMetricValue
            }, ignore_index=True)
            logger.info(f"After appending PANW SYNDICATION customer {customers_table}")
            
        
        else:
            logger.info(f"Multiply Syndication Request with no of targets communities for the customer: {customers}")
            syndication_requests = int(customers_table[customers_table["CustomerName"] == customers]["Syndication Requests"].values[0]) * 5
            khoros_api_count_value = int(customers_table[customers_table["CustomerName"] == customers]["Khoros API Count"].values[0])
            percentageMetricValue = int(syndication_requests + khoros_api_count_value)
            logger.info(f"Resource Usage count for customer {customers}: {percentageMetricValue}")
            customers_table.loc[customers_table['CustomerName'] == customers, 'Percentage Usage of Resource'] = int(percentageMetricValue)
 
 
    return setPercentageMetricValueForCustomer(customers_table)
 
 
def setPercentageMetricValueForCustomer(customers_table):
 
    # get the percentage of Resource usage
 
    logger.info(f"Total Percentage usage for all the customers before Calculating percentage {customers_table['Percentage Usage of Resource']}")
    SumOfTotalMetricCount = np.sum(customers_table["Percentage Usage of Resource"])
 
    # calculate customer level resource usage percentage
    for forCustomerName in customers_table["CustomerName"]:
        # rrdd
        if int(customers_table[customers_table["CustomerName"] == forCustomerName]["Percentage Usage of Resource"].values[0]) != 0:
            # round up the percentage value to 2 decimals
            forCustomerValue = int(customers_table[customers_table["CustomerName"] == forCustomerName]["Percentage Usage of Resource"].values[0])
            customers_table.loc[customers_table['CustomerName'] == forCustomerName, 'Percentage Usage of Resource'] = round(int(forCustomerValue)/int(SumOfTotalMetricCount) * 100, 2)
        else:
            customers_table.loc[customers_table['CustomerName'] == forCustomerName, 'Percentage Usage of Resource'] = 0.00
 
    return customers_table
 
 
def setWeightageColumnForCustomer(body_table):
    
    MultiplyNumber = "1" # for by default 1 for all customers
    logger.info(f"Adding MultiplyNumber {MultiplyNumber} to weightageColumn for Customers")
    
    # Append to Array
    for customers in body_table["CustomerName"]:
        WeightageColumnForCustomer.append(MultiplyNumber)
        logger.info(f"Added Weightage column for customer {customers}")
 
 
def customWeightageColumnForCustomer(customers_table):
    customers_table.loc[customers_table['CustomerName'] == "JMP_CLASSIC", 'Weightage'] = 9
    customers_table.loc[customers_table['CustomerName'] == "GoogleEdu", 'Weightage'] = 32
    customers_table.loc[customers_table['CustomerName'] == "ESRI_CLASSIC", 'Weightage'] = 7
    customers_table.loc[customers_table['CustomerName'] == "PANW_CLASSIC", 'Weightage'] = 10
    return customers_table
 
    
 
def excludeServiceAvailbilityRequests(tenant_ids, from_time_ist, to_time_ist):
 
    # required Time Generated Format to be  | where TimeGenerated between (datetime("2023-12-06 10:50:00") .. datetime("2023-12-06 11:30:00"))
    # convert to UTC because KQL only accepts UTC timezones
    convert_to_utc_from_time = from_time_ist - timedelta(minutes=330)
 
    convert_to_utc_to_time = to_time_ist - timedelta(minutes=330)
 
    # condition to check if list len is 1, because tuple method adds trailing comma for single element list
    # the below logic is helpful
    if len(tenant_ids) == 1:
        tenants_ids_tuple = f"(\"{str(tenant_ids[0])}\")"
    else:
        tenants_ids_tuple = tuple(tenant_ids)
 
    query = f"""
                    AzureDiagnostics
                    | where ResourceProvider == "MICROSOFT.NETWORK" and Category == "FrontdoorAccessLog"
                    | where TimeGenerated between (datetime("{str(convert_to_utc_from_time).split(".")[0]}") .. datetime("{str(convert_to_utc_to_time).split(".")[0]}"))
                    | where requestUri_s has_any {tenants_ids_tuple}    
                    | summarize count = tostring(count())
            """
 
    logger.info(f"KQL query: {query}")
 
    try:
        response = client.query_workspace(
            workspace_id=f"{WORKSPACE_ID}",
            query=query,
            timespan=None
 
        )
        if response.status == LogsQueryStatus.PARTIAL:
            error = response.partial_error
            data = response.partial_data
            logger.info(f"{error}")
 
        elif response.status == LogsQueryStatus.SUCCESS:
            data = response.tables
 
        for required_data in data:
            frontDoorRequests = pd.DataFrame(data=required_data.rows, columns=required_data.columns)
            pd.set_option("display.max_rows", None)
            pd.set_option("display.max_columns", None)
            pd.set_option("display.width", None)
            pd.set_option("display.max_colwidth", None)
 
            count_value = frontDoorRequests.to_json(index=False)
            count_json_value = json.loads(count_value)
            customer_front_door_count = count_json_value["count"]["0"]
            # CustomerTenantIdsCount.append(count_json_value["count"]["0"])
            logger.info(f"Front Door Request Count for customers {CustomerTenantIdsCount}")
            return customer_front_door_count
 
    except HttpResponseError as err:
        logger.info("Something fatal happened:")
        logger.info(f"{err}")
 
 
if __name__ == "__main__":
 
    credential = DefaultAzureCredential(managed_identity_client_id=MANAGED_IDENTITY_CLIENT_ID)
    client = LogsQueryClient(credential=credential)
    logger.info("connected to log analytics query client")
    
    # initiate Customer api call
 
    access_token = managedIdentity()
 
    collab_service_url = f"{METRICS_URL}/smartconxstatics/getcommunitytenantids"
    logger.info(f"SmartConX Stats Metrics URL : {collab_service_url}")
    headers = {
        "Authentication-Provider": "aad",
        "Access-Token": access_token
    }
 
    customers_response = requests.get(url=collab_service_url, headers=headers, verify="/app/icsservices.cer")
    logger.info(f"Response recevied after initating Customer Tenant_ids API Call: {customers_response.json()}")
 
    front_door_to_time_ist = datetime.now(pytz.timezone("Asia/Kolkata"))
 
    fron_door_formatted_to_time_ist = front_door_to_time_ist.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    getTotalIntervalDaysForFrontDoor = int(ITERATIONS_REQUIRED) * int(REQUIRED_DAYS)
    front_door_from_time_ist = datetime.strptime(fron_door_formatted_to_time_ist, "%Y-%m-%dT%H:%M:%S.%f") - timedelta(days=int(getTotalIntervalDaysForFrontDoor))
    
    
    # loop through the Customers Response
    if customers_response.status_code == 200:
        # get communities ids for customers
        logger.info(f"Response Code for Customer Tenant_ids API Call is 200")
        for customers in customers_response.json():
            # if customers["customerName"] in ( "PANW", "ESRI", "InterSystems", "JMP"):
 
                customer_tenant_ids = customers["tenantIds"]
                customer_name = customers["customerName"]
                customer_id = customers["customerId"]
                customerNames.append(customer_name)
                customer_ids.append(customer_id)
                logger.info(f"Total CustomersNames: {customerNames}")
                logger.info(f" ******************** Customer: {customer_name} ********************")
                logger.info(f"Fetching Stats for the Customer: {customer_name}")    
                communityIds = getCommunityIdsForCustomers(access_token=access_token, customerId=customer_id, customer_name=customer_name)
                logger.info(f"List of Community ids for the Customer: {customer_name} are {communityIds}")
                
                required_days_khoros_api_response_count = 0
                required_days_messages_count_response_count = 0
                
                for communityId in communityIds:
                    to_time_ist = datetime.now(pytz.timezone("Asia/Kolkata"))
                    
                    i = 0
                    while i < int(ITERATIONS_REQUIRED):
            
                        time.sleep(60)
                        required_to_time_ist = to_time_ist.strftime("%Y-%m-%dT%H:%M:%S.%f")
                        required_to_time_ist = required_to_time_ist[:-3]
                        from_time_ist = getPreviousDate(required_to_time_ist)
                        required_from_time_ist = from_time_ist.strftime("%Y-%m-%dT%H:%M:%S.%f")
                        required_from_time_ist = str(required_from_time_ist)[:-3]
                        logger.info(f"Passing From_Time is {required_from_time_ist} and To_TIME is {required_to_time_ist}")
                        
                        
                        one_day_response_count = getConnectorDetailsByCustomerId(communityId=communityId, from_time_ist=required_from_time_ist + "Z",
                                                to_time_ist=required_to_time_ist + "Z", access_token=access_token,
                                                tenant_id=customer_tenant_ids[0], customer_name=customer_name)
                        
                        required_days_khoros_api_response_count += int(one_day_response_count[0])
                        required_days_messages_count_response_count += int(one_day_response_count[1])
                        logger.info(f"Count of khoros api count for the customer id {customer_id}, and customer name {customer_name} is {required_days_khoros_api_response_count}")
                        logger.info(f"Count of messages count for the customer id {customer_id}, and customer name {customer_name} is {required_days_messages_count_response_count}")
                        
                        to_time_ist = from_time_ist
                        logger.info(f"i value {i}")
                        i += 1
 
                totalFrontDoorCount = getFrontDoorRequestCount(tenant_ids=customer_tenant_ids, from_time_ist=front_door_from_time_ist, to_time_ist=front_door_to_time_ist)
                CustomerTenantIdsCount.append(totalFrontDoorCount)
                cumulative_message_count.append(required_days_messages_count_response_count)
                khoros_api_count.append(required_days_khoros_api_response_count)    
 
    else:
        logger.info(f"failed API call for customer tenant_ids with status code {customers_response.status_code}")
    
    email_body = ""
    
    customerNames.append("AARP")
    cumulative_message_count.append("0")
    khoros_api_count.append("0")
    excludeServiceAvailabilityCount = excludeServiceAvailbilityRequests(tenant_ids=["/availability"], from_time_ist=front_door_from_time_ist, to_time_ist=front_door_to_time_ist)
    requiredFrontCount = getFrontDoorRequestCount(tenant_ids=["aarp"], from_time_ist=front_door_from_time_ist, to_time_ist=front_door_to_time_ist)
    CustomerTenantIdsCount.append(str(int(requiredFrontCount) - int(excludeServiceAvailabilityCount)))
    
 
    customerNames.append("Higher Logic")
    cumulative_message_count.append("0")
    khoros_api_count.append("0")
    CustomerTenantIdsCount.append(getFrontDoorRequestCount(tenant_ids=["higher-logic"], from_time_ist=front_door_from_time_ist, to_time_ist=front_door_to_time_ist))
 
    logger.info(f"length of pandas table {len(customerNames)} {len(CustomerTenantIdsCount)} {len(cumulative_message_count)} {len(khoros_api_count)}" )
    logger.info(f"Data in pandas table {customerNames}, { CustomerTenantIdsCount}, {cumulative_message_count}, {khoros_api_count}")
    customers_table = {
        "CustomerName": customerNames,
        "Syndication Requests": CustomerTenantIdsCount,
        "Pulled Messages": cumulative_message_count,
        "Khoros API Count": khoros_api_count
    }
 
 
    body_table = pd.DataFrame(customers_table)
    # get ESRI Classic Front Requests count
    ESRI_CLASSIC_REQUEST_COUNT = getFrontDoorRequestCount(tenant_ids=["/posttoclassic/xks2jp1hMTA2Mg=="], from_time_ist=front_door_from_time_ist, to_time_ist=front_door_to_time_ist)
    JMP_CLASSIC_REQUEST_COUNT = getFrontDoorRequestCount(tenant_ids=["/procsessmessages/", "/posttoclassic/x508qy48MTA1OA=="], from_time_ist=front_door_from_time_ist, to_time_ist=front_door_to_time_ist)
    PANW_CLASSIC_REQUEST_COUNT = getFrontDoorRequestCount(tenant_ids=["/classicconnectorservice/classic/", "mkyludyfMTA3MQ=="], from_time_ist=front_door_from_time_ist, to_time_ist=front_door_to_time_ist)
    # Remove InterSystem and Aruba Networks
    body_table = body_table.drop(body_table[body_table['CustomerName'] == "Aruba Networks"].index)
    body_table = body_table.drop(body_table[body_table['CustomerName'] == "InterSystems"].index)
    logger.info(f"After Dropping Specific Customers from Dataframe {body_table}")
    body_table = percentageOfResourceUsedByCustomer(body_table)
 
    # Appending Coulumn to the Existing DataFrame with name ##Percentage Usage of Resource##
    logger.info(f"DataFrame After Adding Percentage Usage of Resource {body_table}")
 
    # Remove InterSystem and Aruba Networks, ESRI
    body_table = body_table.drop(body_table[body_table['CustomerName'] == "Aruba Networks"].index)
    body_table = body_table.drop(body_table[body_table['CustomerName'] == "InterSystems"].index)
    body_table = body_table.drop(body_table[body_table['CustomerName'] == "ESRI"].index)
    body_table = body_table.drop(body_table[body_table['CustomerName'] == "PANW"].index)
    # body_table["Percentage Usage of Resource"] = totalPercentageResourceValueByCustomer
    
    # Add WeightAge Coulumn for Customers
    setWeightageColumnForCustomer(body_table)
    
    body_table["Weightage"] = WeightageColumnForCustomer
 
    body_table = customWeightageColumnForCustomer(body_table)
    
    logger.info(f"Pandas Data frame After Removing Specific Customers ---------- {body_table}")
    
    logger.info(f"{body_table}")
 
    
    # Below Code is for Adding From TimeStamp and To TimeStamp in the Mail
    intervalToTime_ist = datetime.now(pytz.timezone("Asia/Kolkata"))
 
    # Format the interval time in IST
    formattedinterval_to_time_ist = intervalToTime_ist.strftime("%Y-%m-%d")
 
    # Convert formatted time back to a datetime object and subtract 7 days
    getTotalIntervalDays = int(ITERATIONS_REQUIRED) * int(REQUIRED_DAYS)
    intervalFromTime_ist = datetime.strptime(formattedinterval_to_time_ist, "%Y-%m-%d") - timedelta(days=int(getTotalIntervalDays))
    intervalFromTime_ist = intervalFromTime_ist.strftime("%Y-%m-%d")
    logger.info(f"date time interval {intervalFromTime_ist}, {formattedinterval_to_time_ist}")
    
    email_body += f"<p>The Total Request Count for the Customers from <strong> {intervalFromTime_ist}  </strong> to <strong> {formattedinterval_to_time_ist}  </strong> are as below:<br /></p>"
 
    email_body += body_table.to_html(index=False)
    
    email_body += "<br /><p> Thanks,<br /> DevOps Team</p>"
 
    send_mail(email_body=email_body)
