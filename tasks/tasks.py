from db.mongo import mongo_pipelines
from util import date_util
from config import app_config
import logging
from flask import request
from bson.objectid import ObjectId
from datetime import datetime
from db.mongo.db_context import get_mongo_client
import traceback

logger = logging.Logger(__name__)
config = app_config.env_config()
MAX_NUM_TASKS = config['MaxNumTasks']
client = get_mongo_client()
VALID_TASK_FIELDS = [
    "title", "description", "due_date"
]

def get_tasks(user_context, task_id=None, request=None):
    task_collection = client[config['mongoDbName']]['task']
    user_id = user_context.get('userid', None)
    if user_id is None:
        raise ValueError('userid not found')
    ret_val = []
    for item in task_collection.aggregate(
            mongo_pipelines.task_pipeline_for_user(user_id, int(MAX_NUM_TASKS))):
        item["_id"] = str(item["_id"])
        item["disp_create_date"] = date_util.convert_date(get_task_item_value(item, "create_date"))
        item["disp_due_date"] = date_util.convert_date(get_task_item_value(item, "due_date"))
        del(item['assigned_to'])
        ret_val.append(item)
    return ret_val

def create_task(user_context, req: request, task_id=None):
    tasks_to_create = req.json
    task_collection = client[config['mongoDbName']]['task']
    user_id = user_context.get('userid', None)
    if user_id is None:
        raise ValueError('userid not found')
    valid_tasks_for_creation = []
    invalid_tasks_for_creation = []
    while tasks_to_create:
        popped_task = tasks_to_create.pop()
        if is_task_data_valid(popped_task):
            popped_task['create_date'] = int(datetime.now().timestamp())
            popped_task['assigned_to'] = ObjectId(user_id)
            set_null_values_on_task(popped_task)
            if not popped_task['due_date'] is None:
                popped_task['due_date'] = date_util.convert_into_date(
                    popped_task['due_date']['dd_year'],
                    popped_task['due_date']['dd_month'],
                    popped_task['due_date']['dd_day']
                )
            valid_tasks_for_creation.append(popped_task)
        else:
            invalid_tasks_for_creation.append(popped_task)
    if(len(invalid_tasks_for_creation) > 0):
        logger.warning(f'Collection of tasks were invalid for creation: {invalid_tasks_for_creation}')
    result = task_collection.insert_many(valid_tasks_for_creation)
    return {'acknowledged': result.acknowledged, 'insertedIds': list(map(lambda id: str(id), result.inserted_ids))}

def update_task(user_context, req: request, task_id):
    raise NotImplementedError("Not Implemented.")

def delete_task(user_context, request, task_id):
    task_collection = client[config['mongoDbName']]['task']
    user_id = user_context.get('userid', None)
    if user_id is None:
        raise ValueError('userid not found')
    pipeline = mongo_pipelines.single_task_pipeline(task_id)
    task_to_delete = task_collection.find_one(pipeline)
    if task_to_delete is None:
        return {"success": False, "reason": "Task not found"}
    if task_to_delete['assigned_to'] != ObjectId(user_id) and not is_task_admin(user_id):
        return {"success": False, "reason": "User cannot delete task"}
    try:
        deletion_result = task_collection.delete_one(pipeline)
    except:
        traceback.print_exc()
        return {"success": False, "reason": "Attempt to delete failed."}
    res = {"success": deletion_result.acknowledged}
    if not deletion_result.acknowledged:
        res['reason'] = "Deletion not acknowledged.  Likely server error."
    return res


TASK_FN_MAPPING = {
    'GET': get_tasks,
    'POST': update_task,
    'PUT': create_task,
    'DELETE': delete_task
}

#TODO maybe move to generic function
def is_task_data_valid(task_data):
    if not isinstance(task_data, dict):
        return False
    if "title" not in task_data.keys():
        return False
    for task_data_key in task_data.keys():
        if task_data_key not in VALID_TASK_FIELDS:
            return False
    return True

def set_null_values_on_task(task_data):
    task_data.setdefault('due_at_time', False)
    x = [task_data.setdefault(attr, None) for attr in ['due_date', 'description']]

def get_task_item_value(item, attr):
    if item is None:
        return None
    if attr in item.keys():
        return item[attr]
    return None

#TODO Implement and move to common fn
def is_task_admin(userid):
    return False