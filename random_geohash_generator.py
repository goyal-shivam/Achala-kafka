import random
import pygeohash as pgh
def randomlatlon():
    return (round(random.uniform( -90,  90), 5),
            round(random.uniform(-180, 180), 5))

lat, lon = randomlatlon()
print(pgh.encode(lat, lon))