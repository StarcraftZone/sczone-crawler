from configparser import ConfigParser

config = ConfigParser()
config.read("config.ini", "UTF-8")

app = config["app"]
credentials = config["credentials"]
mongo = config["mongo"]
redis = config["redis"]


def getint(section, option):
    return config.getint(section, option)
