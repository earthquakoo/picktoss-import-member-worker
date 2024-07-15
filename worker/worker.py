import os
import json
import logging

from core.database.database_manager import DatabaseManager
from core.sqs.sqs_client import SQSClient


logging.basicConfig(level=logging.INFO)


def handler(event, context):
    db_manager = DatabaseManager(host=os.environ["PICKTOSS_DB_HOST"], user=os.environ["PICKTOSS_DB_USER"], password=os.environ["PICKTOSS_DB_PASSWORD"], db=os.environ["PICKTOSS_DB_NAME"])
    sqs_client = SQSClient(access_key=os.environ["PICKTOSS_ACCESS_KEY"], secret_key=os.environ["PICKTOSS_SECRET_KEY"], region_name="us-east-1", queue_url=os.environ["PICKTOSS_QUEUE_URL"])

    f = open("/var/task/virtual_member_data/virtual_member_data.json", 'rt', encoding="UTF8")
    members = json.load(f)
    
    member_groups = [members[i:i + 100] for i in range(0, len(members), 100)]


    # get_member_query = "SELECT * FROM member"
    # members: list[dict] = db_manager.execute_query(get_member_query)
    # member_groups = [members[i:i + 100] for i in range(0, len(members), 100)] 
    for group in member_groups:
        members_data = {item['id']: item for item in group}
        sqs_client.put(members_data)


    return {"statusCode": 200, "message": "hi"}