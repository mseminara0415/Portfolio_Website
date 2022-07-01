from bs4 import BeautifulSoup
from dateutil import parser
from datetime import datetime
import boto3
from io import BytesIO
import itertools
import json
import re
import requests
import uuid
import wikipedia

wiki_url = 'https://en.wikipedia.org/wiki/List_of_Earth_observation_satellites'
base_url = 'https://en.wikipedia.org'

def get_active_satellites(wiki_url:str) -> dict:
    '''_summary_
    Gets the name and wiki link for active government satellites
    located at the following url:

    https://en.wikipedia.org/wiki/List_of_Earth_observation_satellites

    Parameters
    ----------
    wiki_url : str
        _description_
        This function works specifically with the wiki_url above.
        Any other wiki url will break.

    Returns
    -------
    dict
        _description_
        dictionary of the form {satellite_name: wiki_link}
    '''

    # Make request to wiki url
    response = requests.get(wiki_url)

    # Parse htmle using BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find first table (active satellites)
    active_satellite_table = soup.find('table',{'class':'sortable wikitable'})

    # Get list of satellite names and associated links
    active_satellite_names = [tag.get('title') for tag in active_satellite_table.select('td:nth-of-type(1) a')]
    active_satellite_links = [base_url + tag.get('href') for tag in active_satellite_table.select('td:nth-of-type(1) a')]

    # Zip together 
    active_satellite_dictionary = dict(zip(active_satellite_names,  active_satellite_links))
    
    # active_satellite_dictionary_filtered = dict(itertools.islice(active_satellite_dictionary.items(), 20)) 

    return active_satellite_dictionary

def get_satellite_infobox_data(satellites:dict) -> list:
    '''_summary_
    Parse through each satellite's wiki link and scrape description
    and infobox data.

    Parameters
    ----------
    satellites : dict
        _description_
        Dictionary of the form {satellite_name: wiki_link} 

    Returns
    -------
    list
        _description_
        returns a list of dictionaries for each of the satellites containing satellite data.
    '''

    # List of all satellites and their associated infobox data
    satellite_details_list = []

    for satellite_name, satellite_link in satellites.items():

        # Use Beautiful Soup to parse infobox
        response = requests.get(satellite_link)
        soup = BeautifulSoup(response.text, 'html.parser')
        infobox = soup.find('table', {'class': 'infobox'})

        # Create dictionary for complete look at all satellite details.
        # This will be appended to our satellite_details list
        complete_satellite_data = {}

        # Add base satellite details (description, name, wiki link, etc..)
        complete_satellite_data['name'] = satellite_name

        # Try and get a description for the satellite, if none is avaiable then set to empty string 
        try:
            satellite_description = wikipedia.summary(satellite_name).replace("\n","")
        except (wikipedia.DisambiguationError, wikipedia.PageError):
            satellite_description = ''

        # Set satellite description and wikipedia link fields
        complete_satellite_data['description'] = satellite_description
        complete_satellite_data['wikipedia_link'] = satellite_link

        # Create dictionary just for infobox data
        satellite_infobox_data = {}

        try:
            for infobox_item in infobox.find_all('tr'):
                if infobox_item.find('th',{'class':'infobox-label'}) is None:
                    pass
                else:

                    # Get infobox title and data
                    infobox_item_title = infobox_item.find('th',{'class':'infobox-label'}).text.replace("\xa0"," ")
                    infobox_item_data = infobox_item.find('td',{'class':'infobox-data'}).text.replace("\xa0"," ")

                    # Add to dictionary
                    satellite_infobox_data[infobox_item_title] = infobox_item_data

            complete_satellite_data['infobox'] = satellite_infobox_data

            satellite_details_list.append(complete_satellite_data)
        except AttributeError:
            continue

    return satellite_details_list

def clean_string(string_to_clean:str) -> str:

    # Lower case string
    string_to_clean = string_to_clean.lower()

    # Replace spaces with underscores
    string_to_clean = string_to_clean.replace(' ', '_')
 
    # Replace abbreviation
    string_to_clean = string_to_clean.replace('no.', 'number')

    # Replace any parenthesis with nothing and replace dashes with underscores
    cleaned_string = string_to_clean.replace('(', '').replace(')', '').replace('-', '_')

    return cleaned_string
    
def find_item(dict_obj:dict, key) -> bool:
    '''_summary_
    Recursivly look through a dictionary object for the provided key

    Parameters
    ----------
    dict_obj : dict
        _description_
        Dictionary you want to search through

    key : _type_
        _description_
        Key to search dictionary for

    Returns
    -------
    bool
        _description_
    '''

    if key in dict_obj:
        return dict_obj[key]
    for k, v in dict_obj.items():
        if isinstance(v,dict):
            return find_item(v, key)

def clean_satellite_details(satellite_details_list:list) -> list:
    '''_summary_
    Clean up (to the best of our ability) some of the main fields (dict keys)
    we want to look at. For now this is limited to the following
    fields:

    - Launch date
    - Launch mass

    Parameters
    ----------
    satellite_details_list : list
        _description_

    Returns
    -------
    list
        _description_
        Returns a list of dictionaries with cleaned
        satellite keys.
    '''

    # ========== Format Dictionary Keys ==========

    satellite_details_formatted = []

    for satellite_object in satellite_details_list:
        
        if find_item(satellite_object, 'SATCAT no.'):
    
            format_outer_dictionary = {}
    
            for outer_key, outer_value in satellite_object.items():
    
                # If an outer value is of type dict
                if isinstance(outer_value, dict):
                    formatted_inner_dictionary = {}
    
                    # Loop through the inner dictionary key/values and clean the keys                
                    for inner_key, inner_value in outer_value.items():
                        formatted_inner_dictionary[clean_string(inner_key)] = inner_value
    
                    # Add this cleaned inner dictionary as a value to our outer dictionary under the key 'infobox'
                    format_outer_dictionary['infobox'] = formatted_inner_dictionary
                else:
                    format_outer_dictionary[clean_string(outer_key)] = outer_value
    
            # Append cleaned satellite data to our cleaned satellite details list
            satellite_details_formatted.append(format_outer_dictionary)
        else:
            continue

    # ========== Clean fields ==========

    for satellite in satellite_details_formatted:
        
        # ========== Try to clean Launch Dates ==========
        
        # Patterns to match dates within parenthesis
        date_pattern_1 = re.compile(pattern='(\d{4}-\d{2}-\d{2})')
        date_pattern_2 = re.compile('(\d{1}\s\w{1,9}\s\d{4}|\d{1,2}:\d{1,2}:\d{1,2})')     

        try:
            try:
                satellite['infobox']['launch_date'] = parser.parse(satellite['infobox']['launch_date'].split('[')[0]).replace(tzinfo=None).strftime('%Y-%m-%d')
            except (parser.ParserError, AttributeError):
                try:
                    satellite['infobox']['launch_date'] = parser.parse(re.search(pattern=date_pattern_1, string=satellite['infobox']['launch_date']).group()).replace(tzinfo=None).strftime('%Y-%m-%d')
                except (parser.ParserError, AttributeError):
                    try:
                        satellite['infobox']['launch_date'] = parser.parse(' '.join(date_pattern_2.findall(string=satellite['infobox']['launch_date']))).replace(tzinfo=None).strftime('%Y-%m-%d')
                    except (parser.ParserError, AttributeError):
                        try:
                            satellite['infobox']['launch_date'] = parser.parse(satellite['infobox']['launch_date'].split('(')[0]).replace(tzinfo=None).strftime('%Y-%m-%d')
                        except (parser.ParserError, AttributeError):
                            satellite['infobox']['launch_date'] = None
        except KeyError:
            satellite['infobox']['launch_date'] = None
        

        # ========== Try to clean Launch Mass ==========

        # Pattern to match lbs from launch mass infobox
        launch_mass_pattern_lbs = re.compile('((?<=\().*?(?=\s))')
        launch_mass_pattern_kg = re.compile('(^\d{1,4})(?=\sk)')   

        try:
            try:
                matching_group_launch_mass_lb = re.search(launch_mass_pattern_lbs, satellite['infobox']['launch_mass']).groups()
                for match in matching_group_launch_mass_lb:
                    if match is not None:
                        lb_conversion = int(float(match.replace(',', '')))
                        satellite['infobox']['launch_mass'] = lb_conversion
            except AttributeError:
                try:
                    matching_group_launch_mass_kg = re.search(launch_mass_pattern_kg, satellite['infobox']['launch_mass']).groups()
                    for match in matching_group_launch_mass_kg:
                        if match is not None:
                            kg_conversion = int(float(match.replace(',', '')))
                            kg_to_lb_conversion = int(kg_conversion * 2.2)
                            satellite['infobox']['launch_mass'] = kg_to_lb_conversion
                except AttributeError:
                    satellite['infobox']['launch_mass'] = None
        except KeyError:
            satellite['infobox']['launch_mass'] = None

        # ========== Try to clean SATCAT ids ==========

        # Pattern to match SATCAT id's
        satcat_id_pattern = re.compile(pattern='(^\d{4,5}|\d{4,5}$)')        

        try:            
            try:
                matching_group_satcat = re.search(pattern=satcat_id_pattern, string=satellite['infobox']['satcat_number'].strip()).groups()
                for match in matching_group_satcat:
                    if match is not None:
                        satcat_id_conversion = int(match)
                        satellite['infobox']['satcat_number'] = satcat_id_conversion
            except AttributeError:
                satellite['infobox']['satcat_number'] = None
        except KeyError:
            satellite['infobox']['satcat_number'] = None
            
    return satellite_details_formatted

def get_satellite_details():
    '''_summary_
    Runs the entire process of scraping satellite names and links,
    looping through each satellite to get it's details, and then
    cleaning said details.

    Returns
    -------
    _type_
        _description_
        List of dictionaries containing cleaned satellite data.
    '''

    # Get dictionary of active government satellites
    active_gov_satellites = get_active_satellites(wiki_url=wiki_url)

    # Scrape each satellite wiki link and pull the description and infobox data
    active_gov_satellites_with_data = get_satellite_infobox_data(satellites=active_gov_satellites)

    # Clean up some of the main infobox fields that we want to use. Mainly 'launch date' and 'launch mass' 
    satellites_with_data_clean = clean_satellite_details(satellite_details_list=active_gov_satellites_with_data)

    return satellites_with_data_clean
    
def upload_to_s3(data:dict, bucket_name:str, key:str):
    '''_summary_
    Upload fileobject to desired s3 bucket.

    Parameters
    ----------
    data : dict
        _description_
        data to be uploaded. In our case this is
        most likely a json API response.
    bucket_name : str
        _description_
        Desired bucket location to put the fileobj
    key : str
        _description_
        path/name of fileobject. Example (path/filename.json)
    '''

    # Create s3 client
    s3 = boto3.client('s3')
    
    # Write data to json object
    data_as_json_object = json.dumps(data).encode('utf-8')   

    # Write to IO buffer
    fileobj = BytesIO(data_as_json_object)

    # Upload file to s3
    s3.upload_fileobj(fileobj, bucket_name, key)
    
def lambda_handler(event, context):

    wiki_unique_uuid = str(uuid.uuid4())
    
    wikipedia_satellite_detail = get_satellite_details()
    
    upload_to_s3(
            data=wikipedia_satellite_detail,
            bucket_name='satellite-tracker',
            key=f'wikipedia/wiki_scrape_{wiki_unique_uuid}.json'
        )
