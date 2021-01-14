import os
import yaml
import logging.config
import logging
import coloredlogs


def setup_logging(default_path='logging.yaml', yamlfile='logging.yaml', default_level=logging.DEBUG, env_key='LOG_CFG'):
    """
    | Logging Setup
    """
    path = default_path
    yamlfile = 'logging.yaml'

    if not os.path.exists(path+'/'+'Logs'):
        os.makedirs(path+'/'+'Logs')
    
    logs_path = os.path.join(path, 'Logs')

    config_path = os.path.join(path,'config', yamlfile)

    value = os.getenv(env_key, None)
    if value:
        config_path = value

    if os.path.exists(config_path):
        with open(config_path, 'rt') as f:
            try:

                config_log = yaml.safe_load(f.read())

                config_log['handlers']['debug_file_handler']['filename'] = logs_path + '/' + config_log['handlers']['debug_file_handler']['filename']

                yaml.dump(config_log)
                logging.config.dictConfig(config_log)
                coloredlogs.install()
            except Exception as e:
                print(e)
                print('Error in Logging Configuration. Using default configs')
                logging.basicConfig(level=default_level)
                coloredlogs.install(level=default_level)
    else:
        logging.basicConfig(level=default_level)
        coloredlogs.install(level=default_level)
        print('Failed to load configuration file. Using default configs')
