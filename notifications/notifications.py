from db.mongo import mongo_pipelines
from util import date_util
from config import app_config
import logging
from flask import request
from bson.objectid import ObjectId
from datetime import datetime
from time import sleep
from db.mongo.db_context import get_mongo_client
import traceback

logger = logging.Logger(__name__)
config = app_config.env_config()
MAX_NUM_NOTIFICATIONS = int(config['MaxNumNotifications'])
client = get_mongo_client()
VALID_NOTIFICATION_FIELDS = [
    "description", "create_date", "notice_date", "ackd", "source", "recipient"
]
pending_notifications = {}

def get_notifications(user_context, request: request, notification_id=None):
    notification_collection = client[config['mongoDbName']]['notification']
    userid = user_context.get('userid', None)
    if userid is None:
        raise ValueError("userid not found")
    notifications = []
    notification_cursor = notification_collection.aggregate(
        mongo_pipelines.notifications_pipeline_for_user(userid, MAX_NUM_NOTIFICATIONS))
    for notification in notification_cursor:
        notification['_id'] = str(notification['_id'])
        notification['disp_create_date'] = date_util.convert_date(
            get_notification_item_value(notification, 'create_date'))
        del(notification['recipient'])
        notifications.append(notification)
    return notifications

def set_notifications_as_notified(user_context, request: request, notificat_id=None):
    to_mark_as_notified = request.json
    to_mark_as_notified = client[config['mongoDbName']]['notification']\
        .find_many({"_id": {"$in": to_mark_as_notified}})
    userid = user_context['userid']
    valid_for_update = []
    invalid_for_update = []
    for notification in to_mark_as_notified:
        if notification['userid'] != userid:
            invalid_for_update.append(notification['_id'])
        else:
            valid_for_update.append(ObjectId(notification['_id']))
    logger.warning(f"Unable to update notification IDs {invalid_for_update}")
    if len(valid_for_update) == 0:
        return {"ignored": invalid_for_update}
    notification_collection = client[config['mongoDbName']]['notification']
    update_result = notification_collection.update_many(
        {"_id": {"$in": valid_for_update}},
        {"$set" : {"notice_date": int(datetime.now().timestamp())}}
    )
    update_result['ignored'] = invalid_for_update
    return update_result

def create_notification(user_context, request: request, notification_id=None):
    notification_to_create = request.json
    notification_collection = client[config['mongoDbName']]['notification']
    if user_context.get('userid', None) is not None:
        source = f"user:{user_context['userid']}"
    elif user_context.get('serviceid', None) is not None:
        source = f"svc:{user_context['serviceid']}"
    else:
        raise ValueError('userid or serviceid not found')
    if is_notification_data_valid(notification_to_create):
        recipient_id = notification_to_create['recipient']
        notification_to_create['create_date'] = int(datetime.now().timestamp())
        notification_to_create['source'] = source
        notification_to_create['recipient'] = ObjectId(notification_to_create['recipient'])
        creation_result = notification_collection.insert_one(notification_to_create)
        result = { 'acknowledged': creation_result.acknowledged }
        if creation_result.acknowledged:
            result['inserted_id'] = str(creation_result.inserted_id)
            global pending_notifications
            notification_to_create['_id'] = creation_result.inserted_id
            if recipient_id not in pending_notifications.keys():
                pending_notifications[recipient_id] = []
            pending_notifications[recipient_id].append(notification_to_create)
        return result


def delete_notification(user_context, request: request, notification_id):
    notification_collection = client[config['mongoDbName']]['notification']
    user_id = user_context.get('userid', None)
    if user_id is None:
        raise ValueError('userid not found')
    pipeline = mongo_pipelines.single_notification_pipeline(notification_id)
    notification_to_delete = notification_collection.find_one(pipeline)
    if notification_to_delete is None:
        return {"success": False, "reason": "Notification not found"}
    if notification_to_delete['recipient'] != ObjectId(user_id) and not is_notification_admin(user_id):
        return {"success": False, "reason": "User cannot delete notification"}
    try:
        deletion_result = notification_to_delete.delete_one(pipeline)
    except:
        traceback.print_exc()
        return {"success": False, "reason": "Attempt to delete notification failed."}
    res = {"success": deletion_result.acknowledged}
    if not deletion_result.acknowledged:
        res['reason'] = "Deletion not acknowledged.  Likely server error."
    return res

def mark_notification_as_ackd(user_context, request: request, notification_id):
    notification_collection = client[config['mongoDbName']]['notification']
    user_id = user_context.get('userid', None)
    if user_id is None:
        raise ValueError('userid not found')
    pipeline = mongo_pipelines.single_notification_pipeline(notification_id)
    notification_to_ack = notification_collection.find_one(pipeline)
    if notification_to_ack is None:
        return {"success": False, "reason": "Notification not found"}
    if notification_to_ack['recipient'] != ObjectId(user_id) and not is_notification_admin(user_id):
        return {"success": False, "reason": "User cannot acknowledge notification"}
    try:
        ack_result = notification_to_ack.update_one(pipeline, { "$set": {"ackd": True}})
    except:
        traceback.print_exc()
        return {"success": False, "reason": "Attempt to acknowledge notification failed."}
    res = {"success": ack_result.acknowledged}
    if not ack_result.acknowledged:
        res['reason'] = "Deletion not acknowledged.  Likely server error."
    return res

def get_notifications_summary(user_context, request: request, notification_id=None):
    #TODO still need to implement as summary
    return get_notifications(user_context, request, notification_id)

def get_push_notifications(user_context, request: request, notification_id=None):
    global pending_notifications
    userid = user_context.get("userid", None)
    if userid is None:
        raise ValueError("userid not found")
    interval = float(config['NotificationWaitInterval'])
    timeout = float(config['NotificationWaitTimeout'])
    right_now = datetime.now().timestamp()
    time_to_timeout = right_now + timeout
    while right_now < time_to_timeout:
        if userid in pending_notifications.keys() and len(pending_notifications[userid]) > 0:
            notifications_to_ship, pending_notifications[userid] = pending_notifications[userid], []
            right_now = datetime.now().timestamp()
            for notification in notifications_to_ship:
                notification['notice_date'] = int(right_now)
                del(notification['recipient'])
                notification['_id'] = str(notification['_id'])
                notification['disp_create_date'] = date_util.convert_date(
            get_notification_item_value(notification, 'create_date'))
            return notifications_to_ship
        sleep(interval)
        right_now = datetime.now().timestamp()
    return []


#TODO:  Move to generic function
def get_notification_item_value(item, attr):
    if item is None:
        return None
    if attr in item.keys():
        return item[attr]
    return None

#TODO:  Move to generic function
def set_null_values_on_notification(notification_data):
    notification_data.setdefault('ackd', False)
    x = [notification_data.setdefault(attr, None) for attr in ['description']]

NOTIFICATION_FN_MAPPING = {
    'GET': get_notifications_summary,
    'GET_FULL': get_notifications,
    'GET_PUSH': get_push_notifications,
    'PUT': create_notification,
    'DELETE': delete_notification,
    'PUT_ACK': mark_notification_as_ackd,
    'POST_NOTIFIED': set_notifications_as_notified
}

#TODO maybe move to generic function
def is_notification_data_valid(task_data):
    if not isinstance(task_data, dict):
        return False
    if "description" not in task_data.keys():
        return False
    if "recipient" not in task_data.keys():
        return False
    for task_data_key in task_data.keys():
        if task_data_key not in VALID_NOTIFICATION_FIELDS:
            return False
    return True

#TODO Implement and move to common fn
def is_notification_admin(userid):
    return False