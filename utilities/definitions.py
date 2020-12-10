import enum


class Directions(enum.Enum):
    N = 0
    NNE = 22.5
    NE = 45
    ENE = 67.5
    E = 90
    ESE = 112.5
    SE = 135
    SSE = 157.5
    S = 180
    SSW = 202.5
    SW = 225
    WSW = 247.5
    W = 270
    WNW = 292.5
    NW = 325
    NNW = 347.5

    def __str__(self):
        return self.name


def convert_integer_to_direction(d):

    dirs = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']
    ix = round(d / (360. / len(dirs)))
    return dirs[ix % len(dirs)]
