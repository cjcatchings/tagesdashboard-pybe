from bson.objectid import ObjectId

def obj_pipeline_for_user(userid, matching_field, limit=-1):
    pipeline = [
        {
            '$match': {matching_field: ObjectId(userid) }
        },
        {'$sort': {'create_date': -1}}
    ]
    if(limit > -1):
        pipeline.append({'$limit': limit})
    return pipeline

def single_obj_pipeline(objid):
    return { '_id': ObjectId(objid)}

def task_pipeline_for_user(userid, limit):
    return obj_pipeline_for_user(userid, "assigned_to", limit)

def notifications_pipeline_for_user(userid, limit):
    return obj_pipeline_for_user(userid, "recipient", limit)

def single_task_pipeline(taskid):
    return single_obj_pipeline(taskid)

def single_notification_pipeline(notificationid):
    pipeline = single_obj_pipeline(notificationid)
    pipeline[1]['$sort']['ackd'] = -1
    return pipeline