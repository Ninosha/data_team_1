import pandas as pd


def read_config(config_url):
    """
    function parses configuration json file and returns what to parse
    all datasets or customs

    :param config_url: str/config.json file
    :return: tuple of bool & dictionary
    """

    df = pd.read_json(config_url)
    df = df.to_dict()
    data = df["data"]

    to_parse = (True, {}) if \
        data["parse_all_datasets"] is True \
        else (False, data["parse_datasets"])

    return to_parse


