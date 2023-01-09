from distutils.log import debug
from json import JSONEncoder
import os
from datetime import date
from flask import Flask
from flask.json import JSONDecoder
from flask_restx import Api
from flask_cors import CORS
from krx_crawler import krxApi

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
CORS(app)

isDebug = os.getenv('DEBUG_MODE','true')
isDebug = True if isDebug == 'true' else False


class CustomJsonEncoder(JSONDecoder):
    def default(self, obj):
        try:
            if isinstance(obj, date):
                return obj.isoformat()
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self,obj)

app.json_encoder = CustomJsonEncoder

if isDebug == True:
    api = Api(app, version = '0.1', title='krx data crawler API',descripition = 'krx data crawler API')
else:
    api = Api(app, version = '0.1', title='krx data crawler API',descripition = 'krx data crawler API', doc=False)

api.add_namespace(krxApi)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=isDebug)