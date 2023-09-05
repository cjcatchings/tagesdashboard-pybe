from flask import Flask, request, abort
from flask_cors import CORS
import signal, sys, logging
from config.app_config import env_config
logger = logging.Logger(__name__)

#Import DB context so we can set our global DB client for use in other modules
from db.mongo import db_context
cfg = env_config()
mongo_client = db_context.init_mongo_client()
MONGO_DB = cfg['mongoDbName']

#Import endpoint handler modules
from auth import mongo_flask_auth
from tasks import tasks
from notifications import notifications

interrupt_fired = False

db = mongo_client[MONGO_DB]
#TODO move to DB initializer
user_collection = db['login_user']
task_collection = db['task']
notification_collection = db['notification']

flask_app = Flask(__name__)
flask_app.url_map.strict_slashes = False
cors = CORS(flask_app)
flask_app.config['CORS_HEADERS'] = 'Content-Type'

def catch_signal(sig, frame):
    global interrupt_fired
    if not interrupt_fired:
        mongo_client.close()
        interrupt_fired = True
    sys.exit(0)

signal.signal(signal.SIGINT, catch_signal)
signal.signal(signal.SIGTERM, catch_signal)

@flask_app.route("/tasks", defaults={'task_id': None}, methods=['GET', 'PUT'] )
@flask_app.route("/tasks/<task_id>", methods=['POST', 'DELETE'])
@mongo_flask_auth.validate_token
@mongo_flask_auth.user_context_required
def mongo_tasks(task_id, user_context=None):
    try:
        ret_val = tasks.TASK_FN_MAPPING[request.method](user_context, request, task_id)
    except:
        logging.exception("Bad request.")
        abort(400)
    return ret_val

@flask_app.route("/notifications", defaults={'notification_id': None}, methods=['GET', 'PUT'])
@flask_app.route("/notifications/<notification_id>", methods=['POST', 'DELETE'])
@flask_app.route("/notifications/notified", defaults={'notification_id': None}, methods=['POST'])
@flask_app.route("/notifications/<notification_id>/ack", methods=['PUT'])
@mongo_flask_auth.validate_token
@mongo_flask_auth.user_context_required
def mongo_notifications(notification_id, user_context=None):
    req_method = request.method
    if request.method == 'PUT' and request.url.endswith("ack"):
        req_method = 'PUT_ACK'
    elif request.method == 'GET' and 'full' in request.args:
        req_method = 'GET_FULL'
    elif request.method == 'POST' and request.url.endswith("notified"):
        req_method = "POST_NOTIFIED"
    try:
        ret_val = notifications.NOTIFICATION_FN_MAPPING[req_method](user_context, request, notification_id)
    except:
        logging.exception("Bad request.")
        abort(400)
    return ret_val

@flask_app.route("/pushnotifications", methods=['GET'])
@mongo_flask_auth.validate_token
@mongo_flask_auth.user_context_required
def push_notifications(user_context=None):
    return notifications.NOTIFICATION_FN_MAPPING['GET_PUSH'](user_context, request, None)

@flask_app.route("/auth/validate_token", methods=['POST'])
@mongo_flask_auth.validate_token
@mongo_flask_auth.user_context_required
def do_validate_auth_token(user_context=None):
    resp = {
        "authenticated": True,
        "status": "Authenticated",
    }
    if bool(request.args.get("withPayload", "False")):
        resp['name'] = user_context['name']
    return resp

@flask_app.route("/auth/authn", methods=['POST'])
def auth():
    content_type = request.headers.get('Content-Type')
    if(content_type == 'application/json'):
        req_body = request.json
        return mongo_flask_auth.authenticate_user(req_body.get('username', ''), req_body.get('password', ''))
    else:
        return 'Content-Type not supported!'

@flask_app.route("/fantasyfootball", defaults={'path': None}, methods=['GET','PUT','POST','DELETE'])
@flask_app.route("/fantasyfootball/<path:path>", methods=['GET','PUT','POST','DELETE'])
@mongo_flask_auth.validate_token
@mongo_flask_auth.user_context_required
def fantasy_football(path, user_context=None):
    path_split = path.split("/")
    command = path_split[0]
    return "implement me first!"

if __name__ == "__main__":
    flask_app.run(host="0.0.0.0", port=5000, debug=True)