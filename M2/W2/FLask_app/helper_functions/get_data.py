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
    except ConnectionError as e:
        return f'{e}'

    if data:
        return json.dumps(data)
    else:
        return False


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
        try:
            r = redis.Redis()
        except ConnectionError as e:
            raise e

        res_key = r.get(_id)
        if r.exists(_id):
            return res_key
        else:
            r.set(_id, data)
            return data

    if data:
        return check_redis_data(), 200
    else:
        return f"Data with ID: {_id} doesn't exist. Error: Parameter needs to be a number between 1-100. ", 404
