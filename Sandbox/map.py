import folium
import pandas as pd
from osgeo.osr import SpatialReference, CoordinateTransformation

## Read the information about the location from a .csv file to a pandas dataframe
locationdata = pd.read_csv('/home/WUR/holts009/Downloads/UTREC_F_2015_jun_Locatie.csv')

## Used to convert coordinates from RDnew to WGS84
epsg28992 = SpatialReference()
epsg28992.ImportFromEPSG(28992)

epsg28992.SetTOWGS84(565.237, 50.0087, 465.658, -0.406857, 0.350733, -1.87035, 4.0812)

epsg4326 = SpatialReference()
epsg4326.ImportFromEPSG(4326)

rd2latlon = CoordinateTransformation(epsg28992, epsg4326)
latlon2rd = CoordinateTransformation(epsg4326, epsg28992)

## Set a marker for each location in the dataframe
for each in locationdata.iterrows():
    ## Select all coordinates which are not 'null'
    if str(each[1]['XcoordinaatRD'])[0].isdigit():
        ## Convert coordinates
        X, Y, Z = rd2latlon.TransformPoint(each[1]['XcoordinaatRD'], each[1]['YcoordinaatRD'])
        ## Create a marker for each location
        folium.Marker(
            location=[Y, X],
            popup=each[1]['StraatNaamWegVak'],
            icon=folium.Icon(color='green', icon='road')).add_to(map)

## Set center of map to center of Utrecht
SF_COORDINATES = (52.092876, 5.104480)
map = folium.Map(location=SF_COORDINATES, zoom_start=14)

## Save the map to a .html file
map.save('/tmp/map.html')

## Display in Jupyter Notebook:
# map
