import os, json, yaml


class UserSpecificSettings:
    """docstring for UserSpecificSettings."""

    def __init__(self, username, email="", cluster=""):
        self.UserInfo = {"username": username, "email": email, "cluster": cluster}
        self.LoadJSON()

    def GetJSONPath(self, username):
        return os.getenv("CLUSTERSUBMISSIONPATH") + "/Settings_" + username + ".json"

    def LoadJSON(self, name=""):
        json_name = name if name != "" else self.GetJSONPath(self.UserInfo["username"])
        if os.path.exists(json_name):
            with open(json_name, "r") as f:
                self.UserInfo = yaml.safe_load(f)

    def SaveJSON(self):
        with open(self.GetJSONPath(self.UserInfo["username"]), "w") as f:
            json.dump(self.UserInfo, f, sort_keys=True, indent=4)

    def Set(self, name, info):
        self.UserInfo.update({name: info})

    def Get(self, name, default=None):
        return self.UserInfo.get(name, default)
