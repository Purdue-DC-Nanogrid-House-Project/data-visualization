from configparser import ConfigParser

section_names = 'DATA_DIRECTORIES', 'MEDIA_DIRECTORIES', 'WEATHER_DATA_TYPES',


class Config(object):
    def __init__(self, *file_names):
        parser = ConfigParser()
        parser.optionxform = str
        found = parser.read(file_names)
        if not found:
            raise ValueError('No config file found!')
        for name in section_names:
            self.__dict__.update(parser.items(name))


config = Config(r'C:\Users\jonat\Desktop\Projects\Purdue\data-visualization\config\appconfig.ini')