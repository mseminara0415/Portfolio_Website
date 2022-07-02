import itertools

def flatten(list_to_flatten:list) -> list:
    '''_summary_
    Flatten a list of lists.

    Parameters
    ----------
    list_to_flatten : list
        _description_

    Returns
    -------
    _type_
        _description_
    '''
    flattened_list = []
    for item in list_to_flatten:

        # If item in list is of type list, if so, extend the list, else append the item
        if isinstance(item, list):
            flattened_list.extend(flatten(item))
        else:
            flattened_list.append(item)
    return flattened_list

def get_all_keys(dictionary_to_search:dict) -> list:
    '''_summary_
    Recursivly go through a dictionary and extract all keys

    Parameters
    ----------
    dictionary_to_search : dict
        _description_

    Returns
    -------
    _type_
        _description_
    '''
    # Final Key List
    key_list = []

    for outer_key, outer_value in dictionary_to_search.items():
        # Append all outer keys
        key_list.append(outer_key)

        # Using recursion, check if the outer value is of type dictionary
        if isinstance(outer_value, dict):
            key_list.append(get_all_keys(outer_value))

    # flatten any potential list of lists
    flattend_key_list = flatten(key_list)

    return flattend_key_list

def find_item(satellite_obj, key) -> bool:
    '''_summary_
    Recursively search dictionary for the provided key, if found, return the value.
    Parameters
    ----------
    satellite_obj : _type_
        _description_
        Dictionary to search through

    key : _type_
        _description_
        Key to search dictionary for

    Returns
    -------
    bool
        _description_
        If the key is found this returns the value, if not, returns False
    '''
    if key in satellite_obj:
        return satellite_obj[key]
    for v in filter(dict.__instancecheck__, satellite_obj.values()):
        if (found := find_item(v, key)) is not None:  
            return found

wiki_to_db_column_map ={
    'name':'name',
    'description':'description',
    'application':'application',
    'operator':'operator',
    'cospar_id':'cospar_id',
    'call_sign':'call_sign',
    'manufacturer':'manufacturer',
    'mission_duration':'mission_duration',
    'mission_type':'mission_type',
    'spacecraft_type':'spacecraft_type',
    'satcat_number':'satellite_id',
    'bus': 'satellite_bus',
    'launch_mass':'launch_mass_lbs',
    'launch_date':'launch_date',
    'launch_site':'launch_site',
    'reference_system':'reference_system',
    'rocket':'rocket',
    'website':'website',
    'wikipedia_link':'wikipedia_link'
}


satellite = {
    "name": "ALOS-2",
    "description": "Advanced Land Observing Satellite-2 (ALOS-2), also called Daichi-2, is a 2,120 kg (4,670 lb) Japanese satellite launched in 2014. Although the predecessor ALOS satellite had featured 2 optical cameras in addition to 1.2 GHz (L-band) radar, ALOS-2 had optical cameras removed to simplify construction and reduce costs. The PALSAR-2 radar is a significant upgrade of the PALSAR radar, allowing higher-resolution (1 x 3 m per pixel) spotlight modes in addition to the 10 m resolution survey mode inherited from the ALOS spacecraft. Also, the SPAISE2 automatic ship identification system and the Compact Infra Red Camera (CIRC) will provide supplementary data about sea-going ships and provide early warnings of missile launches.",
    "wikipedia_link": "https://en.wikipedia.org/wiki/ALOS-2",
    "infobox": {
        "names": "Daichi-2",
        "mission_type": "Remote sensing",
        "operator": "JAXA",
        "cospar_id": "2014-029A ",
        "satcat_number": 39766,
        "website": "www.jaxa.jp/projects/sat/alos2/index_j.html",
        "mission_duration": "8 years, 1 month, 5 days (elapsed)",
        "spacecraft_type": "Advanced Land Observing Satellite",
        "bus": "ALOS",
        "launch_mass": 4670,
        "launch_date": "2014-05-24",
        "rocket": "H-IIA 202",
        "launch_site": "Tanegashima, Yoshinobu 1",
        "contractor": "Mitsubishi Heavy Industries",
        "reference_system": {'stanky_leg':'poopy'},
        "regime": "Sun-synchronous orbit",
        "perigee_altitude": "636 km (395 mi)",
        "apogee_altitude": "639 km (397 mi)",
        "inclination": "97.92\u00b0",
        "period": "97.33 minutes"}
    }


db_columns = [column for column in wiki_to_db_column_map.values()]
db_values = [find_item(satellite, k) for k in wiki_to_db_column_map.keys()]

# test = get_all_keys(example_dict)
# print(test)
# print(db_columns)
# print(db_values)
test = find_item(satellite, 'mission_type')
test_2 = find_item(satellite,'stanky_leg')
print(test_2)
# if test:
#     print("it works!")
# else:
#     print("nope!")