import yaml
from pathlib import Path

CONFIG_FILE = "./config.yaml"


class Config:
    def __init__(self):
        try:
            with open(CONFIG_FILE) as file:
                self.config = yaml.load(file, Loader=yaml.FullLoader)
        except Exception as e:
            raise e

    def get(self, path):
        children = path.split('.')
        i = 0
        x = self.config
        while i < len(children):
            x = x.get(children[i])
            if x is None:
                return None
            i = i + 1
        return x



