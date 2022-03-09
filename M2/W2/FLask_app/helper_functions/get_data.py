import redis
import requests
import json

BASE_URL = "https://jsonplaceholder.typicode.com/posts/"


def api_data(url, _id):
    """
    :param url: Base url
    :param _id: id passed from user via url parameter
    :return: if id is in fetched data: json data, if not: False
    """
    try:
        r = requests.get(url + str(_id))
        data = r.json()
        if data:
            return json.dumps(data)
        else:
            return False
    except ValueError:
        return {"message": "Parameter is not valid"}


def redis_base(_id):
    """
    :param _id: id passed from user via url parameter
    :return: if data in redis_task: data, if data not in redis_task: error message
    """

    data = api_data(BASE_URL, _id)

    def check_redis_data():
        """
        :return: if data in redis_task: gets data and returns it, if data not in redis_task: sets data and returns fetched data
        """
        r = redis.Redis()
        res_key = r.get(_id)
        if bool(res_key):
            print("getting from redis_task")
            return res_key
        else:
            print("setting in redis_task")
            r.set(_id, data)
            return data

    try:
        return check_redis_data(), 200
    except:
        return f"Error: data with ID: {_id} doesn't exist", 404
