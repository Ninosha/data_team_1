import requests as rq


def fetch_data():
    """
    :return: fetched data from api as a dictionary
    """

    api_data = rq.get("https://disease.sh/v3/covid-19/countries", verify=False)
    json_data = api_data.json()

    return json_data
