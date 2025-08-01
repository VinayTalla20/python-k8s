import json
import os
import logging
import sys
import time
from datetime import datetime, timezone, timedelta

import google
import pytz
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib
import numpy as np
import pandas as pd
import requests
from google.auth.exceptions import DefaultCredentialsError

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
 
REQUIRED_MAILS = os.getenv("REQUIRED_MAILS", "")
CRON_PASSWD = os.getenv("CRON_PASSWD", "")
METRICS_URL = os.getenv("METRICS_URL", "")
REQUIRED_DAYS = os.getenv("REQUIRED_DAYS", "7")
ITERATIONS_REQUIRED = os.getenv("ITERATIONS_REQUIRED", "1")
CLUSTER_ENV = os.getenv("CLUSTER_ENV", "")
METADATA_URL = "http://metadata.google.internal/computeMetadata/v1/"
METADATA_HEADERS = {"Metadata-Flavor": "Google"}
SERVICE_ACCOUNT = "default"
SERVICE_ACCOUNT_TO_CHECK = os.getenv("SERVICE_ACCOUNT_TO_CHECK", "")
LB_FRONTEND = os.getenv("LB_FRONTEND", "")
LOG_SOURCE_VIEW_ID = os.getenv("LOG_SOURCE_VIEW_ID", "")
LOKI_DATASOURCE_HOST = os.getenv("LOKI_DATASOURCE_HOST", "http://localhost:3100")

# To get list of mails
mail = REQUIRED_MAILS.split(",")

for to_mails in mail:
    SEND_MAILS.append(to_mails)

def checkServiceAccountEmailExists():
    """
    Retrieves email id or unique id of the default service account from the metadata server.

    Returns:
        The email id or unique id
    """
    logger.info("Check whether the Service Account email is attached")

    url = f"{METADATA_URL}instance/service-accounts/default/email"

    # Request an access token from the metadata server.
    r = requests.get(url, headers=METADATA_HEADERS)
    r.raise_for_status()

    # Extract the access token from the response.
    email_id = r.text

    logger.info(f"Fetched email_id of service account: {email_id}")

    return str(email_id).strip()

def getAccessTokenUsingWorkloadIdentity() -> str:
    """
    Retrieves access token from the metadata server.

    Returns:
        The access token.
    """
    url = f"{METADATA_URL}instance/service-accounts/{SERVICE_ACCOUNT}/token"

    if checkServiceAccountEmailExists() == str(SERVICE_ACCOUNT_TO_CHECK).strip():

        # Request an access token from the metadata server.
        r = requests.get(url, headers=METADATA_HEADERS)
        r.raise_for_status()

        # Extract the access token from the response.
        accessToken = r.json()["access_token"]

        logger.info(f"Generated AccessToken: {accessToken}")

        return accessToken

    else:
        logger.error("The service account is not matched with the required account mail")
        raise  DefaultCredentialsError

# timestamp to send  2023-12-04T05:15:08.070Z
# get call
# Yes it will be get call, ex-->
# /getconnectordetailsbycustomerid/{customerId}?fromDate=2023-12-04T05:15:08.070Z&toDate=2023-12-09T05:15:08.070Z
def getConnectorDetailsByCustomerId(communityId, from_time_ist, to_time_ist, tenant_id, customer_name):
    logger.info(
        f"Fetching messages count for community id {communityId} with tenant id {tenant_id} from {from_time_ist} to {to_time_ist} for customer {customer_name}")

    # initiate request by passing CustomerId, From and To timestamps
    metrics_url = f"{METRICS_URL}/statics/{communityId}/{from_time_ist}/{to_time_ist}/{tenant_id}"

    metrics_response = requests.get(url=metrics_url, verify="/app/icsservices.cer")
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
        metrics_response = requests.get(url=metrics_url, verify="/app/icsservices.cer")
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

def getCommunityIdsForCustomers(customerId, customer_name):
    logger.info(f"Fetching commmunity ids for customer name {customer_name} with customer id {customer_id}")

    # initiate request by passing CustomerId, From and To timestamps
    metrics_url = f"{METRICS_URL}/{customerId}"

    metrics_response = requests.get(url=metrics_url, verify="/app/services.cer")
    logger.info(f"API url passed: {metrics_url}")
    logger.info(
        f"status code after response received from Community Ids API Call for the customer {customer_name}: {metrics_response.status_code}")
    logger.info(
        f"response received from Community Ids API Call for customer {customer_name} is {metrics_response.json()}")
    return metrics_response.json()

def getPreviousDate(get_date):
    logger.info("get previous date")
    previous_date = datetime.strptime(get_date, "%Y-%m-%dT%H:%M:%S.%f") - timedelta(days=int(REQUIRED_DAYS))

    return previous_date

def getPreviousDateForGrafanaLoki(start_date):
    REQUIRED_INTERVAL_HOURS = int(REQUIRED_DAYS) * 24
    previous_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ") - timedelta(
        hours=int(REQUIRED_INTERVAL_HOURS))
    previous_date = previous_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info(f"returning previous date {previous_date}")
    return previous_date

def getRequestCountFromLogs():
    logger.info(f"This is to get request count from GrafanaLoki datasource based on the count of logs")
    end_time = datetime.now(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    start_time = getPreviousDateForGrafanaLoki(end_time)

    query = f'sum(count_over_time({{container="marketo-service"}} |~ `URL:https://.*\\.mktorest\\.com/rest/v1/leads\\.json`[{REQUIRED_DAYS}d]))'
    url = f"{LOKI_DATASOURCE_HOST}/loki/api/v1/query_range"
    params = {
        "limit": 2000,
        "start": start_time,
        "end": end_time,
        "query": query
    }
    response = requests.get(url, params=params, timeout=600)

    if response.status_code == 200 and response.json()["data"]["result"]:
        logger.info("Success response")
        requestCount = response.json()["data"]["result"][0]["values"]
        RequiredRequestCount = requestCount[len(requestCount) - 1][1]
        logger.info(f"Request count fetched from logs is: {RequiredRequestCount}")
        return RequiredRequestCount
    else:
        logger.info(f"Error No Exception founds {response.json}")
        return 0

def getServerRequestCount(jsonPayload):
    ACCESS_TOKEN = getAccessTokenUsingWorkloadIdentity()

    LogsApiURL = "https://logging.googleapis.com/v2/entries:list"
    Headers = {
            "Authorization": f"Bearer {ACCESS_TOKEN}"
        }

    logResponse = requests.post(url=LogsApiURL, headers=Headers, json=jsonPayload)

    if logResponse.status_code == 200 and logResponse.json():
        print("successfully fetched log entries")
        return logResponse.json()
    else:
        return None

def calculateServerRequestCount(tenant_ids):

    totalIntervalDays = int(ITERATIONS_REQUIRED) * int(REQUIRED_DAYS)

    utcTimeNow = datetime.now(pytz.utc)
    requiredToTime = utcTimeNow.strftime("%Y-%m-%dT%H:%M:%S")
    requiredFromTime = datetime.strptime(requiredToTime, "%Y-%m-%dT%H:%M:%S") - timedelta(days=int(totalIntervalDays))
    requiredFromTime = requiredFromTime.strftime("%Y-%m-%dT%H:%M:%S")

    logger.info(f"Fetching Server Requests From Time: {requiredFromTime}Z To Time:{requiredToTime}Z")

    # condition to check if list len is 1, because tuple method adds trailing comma for single element list
    # the below logic is helpful

    convert_To_string = ""

    if len(tenant_ids) == 1:
        convert_To_string = f"(\"{str(tenant_ids[0])}\")"
    else:
        tenants_ids_tuple = tuple(tenant_ids)

        for index, modified_tenant_ids in enumerate(tenants_ids_tuple):
            logger.info(modified_tenant_ids)
            if index == len(tenants_ids_tuple) - 1:
                # last element so no need to append "OR" at last
                convert_To_string += " " + '"' + modified_tenant_ids + '"'
            else:
                convert_To_string += " " + '"' + modified_tenant_ids + '"' + " " + "OR" + " "
                # For Google Log Explorer "has" condition is defined as below
                #   httpRequest.requestUrl: ("vvxebddfMTA3Mg==" OR "gu9j303iMTA2MQ==" OR "fs6hs2loMTA3Mw==" OR "procsessmessages" OR "grafana")

    convert_To_string = convert_To_string.lstrip()

    query = f"""
            resource.type="http_load_balancer"
            resource.labels.forwarding_rule_name="{LB_FRONTEND}"
            httpRequest.requestUrl : ({convert_To_string})
            timestamp >= "{requiredFromTime}Z" AND timestamp <= "{requiredToTime}Z"  -- UTC time
            """

    jsonPayload = {
        "resourceNames": [f"{LOG_SOURCE_VIEW_ID}"],
        "filter": f"{query}"
    }

    logger.info(f"LQL Query: {query}")


    requestCount = 0
    logResponse = getServerRequestCount(jsonPayload)

    if logResponse is not None:
        requestCount += len(logResponse["entries"])

        while "nextPageToken" in logResponse:
            logger.info("Response is multiple page")
            jsonPayload["pageToken"] = logResponse["nextPageToken"]
            logResponse = getServerRequestCount(jsonPayload)
            requestCount += len(logResponse["entries"])

    logger.info(f"total Count: {requestCount}")
    return  requestCount

def percentageOfResourceUsedByCustomer(customers_table):
    # Consider 0 for all Customers, below is the list with 0 values
    totalPercentageMetricValue = [0] * len(customers_table.index)

    # Ading a new Column with name "Percentage Usage of Resource" with values
    customers_table["Percentage Usage of Resource"] = totalPercentageMetricValue
    logger.info(f"After Adding Default resource usage Values: {customers_table}")

    # get Syndication requests for each Customer and multiply by 5 default
    for customers in customers_table["CustomerName"]:
        logger.info(f"Applying Resource Usage Logic for customer {customers}")

        if customers == "GoogleEdu":
            logger.info(f"Multiply Syndication Request with no of targets communities for the customer: {customers}")

            # for GoogleEdu Classic
            GOOGLE_EDU_CLASSIC_REQUEST_COUNT = calculateServerRequestCount(["/procsessmessages/"])
            logger.info(f"The Request Count for the GoogleEdu Classic Customer {GOOGLE_EDU_CLASSIC_REQUEST_COUNT}")
            google_classic_syndication_requests = int(GOOGLE_EDU_CLASSIC_REQUEST_COUNT) * 5 * 38

            # Khoros_Api_Count will be zero for GoogleEdu Classic, so excluding form below percentage calculation
            google_classic_percentageMetricValue = int(google_classic_syndication_requests)
            logger.info(f"Resource Usage count for customer GoogleEdu Classic: {google_classic_percentageMetricValue}")
            customers_table = customers_table._append({
                "CustomerName": "GoogleEdu Classic",
                "Syndication Requests": GOOGLE_EDU_CLASSIC_REQUEST_COUNT,
                "Pulled Messages": "0",
                "Khoros API Count": "0",
                "Percentage Usage of Resource": google_classic_percentageMetricValue
            }, ignore_index=True)
            logger.info(f"After appending GoogleEdu Classic customer {customers_table}")

        else:
            logger.info(f"Multiply Syndication Request with no of targets as 5(default) for the customer: {customers}")
            syndication_requests = int(
                customers_table[customers_table["CustomerName"] == customers]["Syndication Requests"].values[0]) * 5
            khoros_api_count_value = int(
                customers_table[customers_table["CustomerName"] == customers]["Khoros API Count"].values[0])
            # consider
            percentageMetricValue = int(syndication_requests + khoros_api_count_value)
            logger.info(f"Resource Usage count for customer {customers}: {percentageMetricValue}")
            customers_table.loc[customers_table['CustomerName'] == customers, 'Percentage Usage of Resource'] = int(
                percentageMetricValue)

    return setPercentageMetricValueForCustomer(customers_table)

def setPercentageMetricValueForCustomer(customers_table):
    # get the percentage of Resource usage

    logger.info(
        f"Total Percentage usage for all the customers before Calculating percentage {customers_table['Percentage Usage of Resource']}")

    SumOfTotalMetricCount = np.sum(customers_table["Percentage Usage of Resource"])

    # calculate each customer level resource usage percentage based on Total Resource Usage
    for forCustomerName in customers_table["CustomerName"]:
        # check the value is not equal to 0
        if int(customers_table[customers_table["CustomerName"] == forCustomerName][
                   "Percentage Usage of Resource"].values[0]) != 0:

            forCustomerValue = int(customers_table[customers_table["CustomerName"] == forCustomerName][
                                       "Percentage Usage of Resource"].values[0])
            # round up the percentage value to 2 decimals
            customers_table.loc[
                customers_table['CustomerName'] == forCustomerName, 'Percentage Usage of Resource'] = round(
                int(forCustomerValue) / int(SumOfTotalMetricCount) * 100, 2)
        else:
            customers_table.loc[
                customers_table['CustomerName'] == forCustomerName, 'Percentage Usage of Resource'] = 0.00

    return customers_table

def setWeightageColumnForCustomer(body_table):
    MultiplyNumber = "1"  # default 1 for all customers
    logger.info(f"Adding MultiplyNumber {MultiplyNumber} to weightageColumn for Customers")

    # Append to Array
    for customers in body_table["CustomerName"]:
        WeightageColumnForCustomer.append(MultiplyNumber)
        logger.info(f"Added Weightage column for customer {customers}")

def customWeightageColumnForCustomer(customers_table):

    customers_table.loc[customers_table['CustomerName'] == "GoogleEdu Classic", 'Weightage'] = 38

    return customers_table

def send_mail(email_body):
    logger.info("Sending Mail Report")
    # smtp connections:
    smtp_server = "smtp.office365.com"
    smtp_port = 587
    username = ""
    password = f"{CRON_PASSWD}"
    from_addr = ""
    to_addr = SEND_MAILS
    subject = f"GCP {CLUSTER_ENV} usage for each customer"

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


if __name__ == "__main__":

    # initiate Customer api call
    # this is access_token is not used
    collab_service_url = f"{METRICS_URL}/smartconxstatics/getcommunitytenantids"
    logger.info(f"SmartConX Stats Metrics URL : {collab_service_url}")

    customers_response = requests.get(url=collab_service_url, verify="/app/icsservices.cer")
    logger.info(f"Response received after initiating Customer Tenant_ids API Call: {customers_response.json()}")

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
            communityIds = getCommunityIdsForCustomers(customerId=customer_id,
                                                       customer_name=customer_name)
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
                    logger.info(
                        f"Passing From_Time is {required_from_time_ist} and To_TIME is {required_to_time_ist}")

                    one_day_response_count = getConnectorDetailsByCustomerId(communityId=communityId,
                                                                             from_time_ist=required_from_time_ist + "Z",
                                                                             to_time_ist=required_to_time_ist + "Z",
                                                                             tenant_id=customer_tenant_ids[0],
                                                                             customer_name=customer_name)

                    required_days_khoros_api_response_count += int(one_day_response_count[0])
                    required_days_messages_count_response_count += int(one_day_response_count[1])
                    logger.info(
                        f"Count of khoros api count for the customer id {customer_id}, and customer name {customer_name} is {required_days_khoros_api_response_count}")
                    logger.info(
                        f"Count of messages count for the customer id {customer_id}, and customer name {customer_name} is {required_days_messages_count_response_count}")

                    to_time_ist = from_time_ist
                    logger.info(f"i value {i}")
                    i += 1

            totalServerRequestCount = calculateServerRequestCount(tenant_ids=customer_tenant_ids)
            CustomerTenantIdsCount.append(totalServerRequestCount)
            cumulative_message_count.append(required_days_messages_count_response_count)
            khoros_api_count.append(required_days_khoros_api_response_count)

    else:
        logger.info(f"failed API call for customer tenant_ids with status code {customers_response.status_code}")

    # Below Code is for Adding From TimeStamp and To TimeStamp in the Mail
    intervalToTime_ist = datetime.now(pytz.timezone("Asia/Kolkata"))

    # Format the interval time in IST
    formattedinterval_to_time_ist = intervalToTime_ist.strftime("%Y-%m-%d")

    # Convert formatted time back to a datetime object and subtract 7 days
    getTotalIntervalDays = int(ITERATIONS_REQUIRED) * int(REQUIRED_DAYS)
    intervalFromTime_ist = datetime.strptime(formattedinterval_to_time_ist, "%Y-%m-%d") - timedelta(
        days=int(getTotalIntervalDays))
    intervalFromTime_ist = intervalFromTime_ist.strftime("%Y-%m-%d")

    customers_table = {
        "CustomerName": customerNames,
        "Syndication Requests": CustomerTenantIdsCount,
        "Pulled Messages": cumulative_message_count,
        "Khoros API Count": khoros_api_count
    }

    body_table = pd.DataFrame(customers_table)

    # Add Google-Marketo Customer
    body_table = body_table._append({
        "CustomerName": "GoogleEdu Marketo",
        "Syndication Requests": getRequestCountFromLogs(),
        "Pulled Messages": "0",
        "Khoros API Count": "0"
    }, ignore_index=True)

    # add percentage of Resource Usage Calculation column
    body_table = percentageOfResourceUsedByCustomer(body_table)

    # Add WeightAge Coulumn for Customers which indicates no.of community targets
    setWeightageColumnForCustomer(body_table)

    body_table["Weightage"] = WeightageColumnForCustomer

    # Remove specific Customer Row from the Table
    body_table = body_table.drop(body_table[body_table['CustomerName'] == "GoogleEdu"].index)

    # add custom weightage for specific customers
    body_table = customWeightageColumnForCustomer(body_table)

    email_body = f"<p>The Total Request Count for the Customers from <strong> {intervalFromTime_ist}  </strong> to <strong> {formattedinterval_to_time_ist}  </strong> are as below:<br /></p>"

    email_body += body_table.to_html(index=False)

    email_body += "<br /><p> Thanks,<br /> DevOps Team</p>"

    send_mail(email_body=email_body)
