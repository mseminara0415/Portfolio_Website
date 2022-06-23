import requests

def download_satellite_data(norad_id: int = 25544, is_tle:bool = False) -> dict:

    API_KEY = 'XM7T6G-DBG58A-PHYUD2-4VPH'
    # If we want to return positioning data
    if not is_tle:
        # Get position data from API
        #api_url = f'https://api.n2yo.com/rest/v1/satellite/{norad_id}/&apiKey={API_KEY}'
        api_url = f'https://api.n2yo.com/rest/v1/satellite/positions/38771/41.702/-76.014/0/2/&apiKey={API_KEY}'
        iss_data = requests.get(api_url).json()
        satellite_data_type = 'position'

    # If we want to return orbital data
    elif is_tle:
        # Get tle data from API
        api_url_tle = f'https://api.wheretheiss.at/v1/satellites/{norad_id}/tles'
        iss_data = requests.get(api_url).json()
        satellite_data_type = 'tle'
        
    return iss_data


test = download_satellite_data()
print(test)