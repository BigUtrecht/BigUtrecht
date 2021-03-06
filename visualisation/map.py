from osgeo.osr import SpatialReference, CoordinateTransformation

import folium

from constants.spark import *
from etl import parquet


def map_dataframe(dataframe, variable_string1, max_color, mid_color, popup_string1, save_string,
                  variable_string2=None, popup_string2=None, variable_string3=None, popup_string3=None,
                  variable_string4=None, popup_string4=None):
    """
    Map dataframe function
    Maps the dataframe given as input to a folium map
    Saves the map in the assigned location
    :return: None
    """
    with Session() as s:
        ## Load the location data in a pandas dataframe
        locationdata = parquet.readLocatie(s).toPandas()

        ## Used to convert coordinates from RDnew to WGS84
        epsg28992 = SpatialReference()
        epsg28992.ImportFromEPSG(28992)

        epsg28992.SetTOWGS84(565.237, 50.0087, 465.658, -0.406857, 0.350733, -1.87035, 4.0812)

        epsg4326 = SpatialReference()
        epsg4326.ImportFromEPSG(4326)

        rd2latlon = CoordinateTransformation(epsg28992, epsg4326)

        ## Set center of map to center of Utrecht and create map
        SF_COORDINATES = (52.092876, 5.104480)
        map = folium.Map(location=SF_COORDINATES, zoom_start=14)

        ## Set a marker for each location in the dataframe
        for each in locationdata.iterrows():
            for line in dataframe.collect():
                if each[1]['MeetpuntRichtingCode'] == line['UniekeMeetpuntRichtingCode'] + '-1' or \
                                each[1]['MeetpuntRichtingCode'] == line['UniekeMeetpuntRichtingCode']:

                    ## Select all coordinates which are not 'null'
                    if each[1]['XRD'] != '':
                        ## Convert coordinates
                        X, Y, Z = rd2latlon.TransformPoint(int(each[1]['XRD']), int(each[1]['YRD']))
                        ## Create a marker for each location
                        if variable_string4 != None:
                            if line[variable_string1] > max_color or line[variable_string2] > max_color or \
                                            line[variable_string3] > max_color or line[variable_string4] > max_color:
                                color = 'red'
                            elif line[variable_string1] > mid_color or line[variable_string2] > mid_color or \
                                            line[variable_string3] > mid_color or line[variable_string4] > mid_color:
                                color = 'orange'
                            else:
                                color = 'green'
                            folium.Marker(
                                location=[Y, X],
                                popup=(folium.Popup(popup_string1 + ': ' + str(int(line[variable_string1])) + ', ' +
                                                    popup_string2 + ': ' + str(int(line[variable_string2])) + ', ' +
                                                    popup_string3 + ': ' + str(int(line[variable_string3])) + ', ' +
                                                    popup_string4 + ': ' + str(int(line[variable_string4]))
                                                    , max_width=200)),
                                icon=folium.Icon(color=color, icon='road')).add_to(map)
                        elif variable_string3 != None:
                            if line[variable_string1] > max_color or line[variable_string3] > max_color or \
                                            line[variable_string4] > max_color:
                                color = 'red'
                            elif line[variable_string1] > mid_color or line[variable_string2] > mid_color or line[variable_string3] > mid_color:
                                color = 'orange'
                            else:
                                color = 'green'
                            folium.Marker(
                                location=[Y, X],
                                popup=(folium.Popup(popup_string1 + ': ' + str(int(line[variable_string1])) + ', ' +
                                                    popup_string2 + ': ' + str(int(line[variable_string2])) + ', ' +
                                                    popup_string3 + ': ' + str(int(line[variable_string3]))
                                                    , max_width=200)),
                                icon=folium.Icon(color=color, icon='road')).add_to(map)
                        elif variable_string2 != None:
                            if line[variable_string1] > max_color or line[variable_string2] > max_color:
                                color = 'red'
                            elif line[variable_string1] > mid_color or line[variable_string2] > mid_color:
                                color = 'orange'
                            else:
                                color = 'green'
                            folium.Marker(
                                location=[Y, X],
                                popup=(folium.Popup(popup_string1 + ': ' + str(int(line[variable_string1])) + ', ' +
                                                    popup_string2 + ': ' + str(int(line[variable_string2])),
                                                    max_width=200)),
                                icon=folium.Icon(color=color, icon='road')).add_to(map)
                        else:
                            if line[variable_string1] > max_color:
                                color = 'red'
                            elif line[variable_string1] > mid_color:
                                color = 'orange'
                            else:
                                color = 'green'
                            folium.Marker(
                                location=[Y, X],
                                popup=(popup_string1 + ': ' + str(int(line[variable_string1]))),
                                icon=folium.Icon(color=color, icon='road')).add_to(map)

        ## Save the map to a .html file
        map.save(save_string)

