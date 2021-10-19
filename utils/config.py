from configparser import ConfigParser

config = ConfigParser()
config.read("config.ini", "UTF-8")

credentials = config["credentials"]
redis = config["redis"]
app = config["app"]


def getint(section, option):
    return config.getint(section, option)
