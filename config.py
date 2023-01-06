import yaml
import pprint

with open('./config/config.yaml') as config_file:
    config_dictionary = yaml.load(config_file, Loader=yaml.FullLoader)
    pprint.pprint(config_dictionary)
