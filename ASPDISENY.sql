
USE ROLE CRE_RL_ARCH_ADMIN;
USE DATABASE CRE_DB;
USE SCHEMA CRE_LND;

update CRE_DB.CRE_LND.CRE_EP_URL_LND SET API_LIMIT=1000 where API_NAME='ASP_GET_EVNT_EP';


*************************************************************************************

CREATE OR REPLACE PROCEDURE CRE_DB.CRE_LND.CRE_SP_ASP_EVENT_LND("API_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','requests','pandas','python-dateutil')
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (CRE_EAI_ASP)
EXECUTE AS OWNER
AS '
# Copyright (c) 2024 The Walt Disney Company - All Rights Reserved
#
# Version History
# ---------------                                                     
# Version 1.0 (10/JUN/2025): Initial version created by Bikash Biswal

import requests
import snowflake.snowpark as snowpark
import pandas as pd
from datetime import datetime, timedelta, timezone
import math
from dateutil.parser import parse
def main(session, API_NAME):
    # Step 1: Retrieve the URL from the CRE_EP_URL_LND table based on api_name and environment
    result = session.sql(f"SELECT url,API_LIMIT FROM CRE_EP_URL_LND WHERE API_NAME = ''{API_NAME}''").collect()
    if not result:
        return f"Error: No URL found for API: {API_NAME}"
    url = result[0][0]
    limit=result[0][1]
    # Step 2: Get the Bearer token
    token_result = session.sql("CALL CRE_SP_ASP_TKN_EP(''ASP_POST_EP'')").collect()
    BauthToken = token_result[0][0]
    # Step 3: Set headers for API call
    headers = {
        ''Authorization'': BauthToken
    }
    # Step 4: Preparing the start and end times for the API call
    # Using UTC timezone for consistency
    startAt = (datetime.now(timezone.utc) - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
    endAt = (datetime.now(timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
    # Initial url to get total size
    initial_url = (
     f"{url}{startAt}&endAt={endAt}"
     f"&start=0&limit={limit}")
    # Initial request to get total size
    initial_response = requests.get(initial_url, headers=headers)
    if initial_response.status_code != 200:
        return f''Error: {initial_response.status_code}, {initial_response.text}''
    initial_data = initial_response.json()
    if not initial_data.get(''items''):
        return "No data to load"
    total_size = initial_data.get("size")
    total_pages = math.ceil(total_size / limit)
    
    extracted_data = []
    # Step 5: Extract specific fields
    # Loop through each page to fetch data
    for page in range(total_pages):
        start = page * limit
        pagination_url = (
            f"{url}{startAt}&endAt={endAt}"
            f"&start={start}&limit={limit}"
        )
        response = requests.get(pagination_url, headers=headers)
        if response.status_code == 200:
            json_data = response.json()
            for item in json_data.get(''items'', []):
                status_history = item.get("statusHistory", [])
                resources = item.get("resources", [{}])
                attendees = item.get("attendees", [{}])
                recurrence = item.get("recurrence", {})
                resource = resources[0] if resources else {}
                attendee = attendees[0] if attendees else {}
                extracted_data.append({
                    "EVENT_ID": item.get("id"),
                    "RESERVATION_ID": item.get("reservationId"),
                    "RESERVATION_STATUS": item.get("status"),
                    "STATUS_REASON": status_history[-1].get("reason") if status_history else None,
                    "START_TIME_ZONE": item.get("startTimeZone"),
                    "END_TIME_ZONE": item.get("endTimeZone"),
                    "RESERVATION_START_AT": item.get("reservationStartAt"),
                    "RESERVATION_END_AT": item.get("reservationEndAt"),
                    "CREATED_AT": item.get("createdAt"),
                    "UPDATED_AT": item.get("updatedAt"),
                    "TITLE": item.get("title"),
                    "RESOURCE_ID": resource.get("resourceId"),
                    "RESOURCE_TYPE": resource.get("resourceType"),
                    "RESOURCE_SUB_TYPE": resource.get("resourceSubType"),
                    "RESOURCE_NETWORK_NAME": resource.get("resourceNetworkName"),
                    "RESOURCE_FLOOR_NAME": resource.get("resourceFloorName"),
                    "RESOURCE_NETWORK_ID": resource.get("resourceNetworkId"),
                    "RESOURCE_NAME": resource.get("resourceName"),
                    "RESOURCE_STATUS": resource.get("status"),
                    "RECURRENCE_PATTERN_TYPE": recurrence["pattern"]["type"] if recurrence.get("pattern") else None,
                    "RECURRENCE_RANGE_TYPE": recurrence["range"]["type"] if recurrence.get("range") else None,
                    "RECURRENCE_START_DATE": recurrence["range"]["startDate"] if recurrence.get("range") else None,
                    "RECURRENCE_END_DATE": recurrence["range"]["endDate"] if recurrence.get("range") else None,
                    "NUMBER_OF_OCCURRENCES": recurrence["range"]["numberOfOccurrences"] if recurrence.get("range") else None,
                    "USER_ID": attendee.get("userId"),
                    "EMAIL": attendee.get("email"),
                    "DISPLAY_NAME": attendee.get("displayName"),
                    "ATTENDEE_STATUS": attendee.get("status")
                })
    # Step 6: Convert extracted data to DataFrame
    df = pd.DataFrame(extracted_data)
    # Add metadata columns
    df[''LOAD_BY''] = session.sql("SELECT CURRENT_USER()").collect()[0][0]
    df[''LOAD_TIME''] = session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0]
    df[''TASK_NAME''] = session.sql("SELECT SYSTEM$TASK_RUNTIME_INFO(''CURRENT_ROOT_TASK_NAME'')").collect()[0][0]
    df[''RUN_ID''] = session.sql("SELECT SYSTEM$TASK_RUNTIME_INFO(''CURRENT_TASK_GRAPH_RUN_GROUP_ID'')").collect()[0][0]
    # Step 7: Write the DataFrame to Snowflake
    session.write_pandas(df, "ASP_EVENT_LND", auto_create_table=False, overwrite=True)
    return f"Successfully loaded {len(df)} records into the ASP_EVENT_LND table"
';

***************************************************************************************
CREATE OR REPLACE TASK CRE_DB.CRE_LND.CRE_TSK_ASP_EVENT_LND
	warehouse=CRE_WH_ELT_SVC_S
	schedule='USING CRON 0 1 * * * America/Los_Angeles'
	as BEGIN
CALL CRE_SP_ASP_EVENT_LND('ASP_GET_EVNT_EP');
END;