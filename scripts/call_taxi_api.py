import os
import requests
import pandas as pd
import datetime
import time

# Helper function to call the LTA API
def call_lta_api(url,
                 headers = {},
                 params = {}):
    """
    Calls the LTA API with simple error handling

    :param url: LTA API endpoint URL
    :param headers: Optional header to pass into the API call (dict)
    :param params: Optional parameters to pass into the API call (dict)
    :return: Values returned from the LTA API
    """

    response = requests.get(url, headers = headers, params = params)
    if response.status_code != 200:
        raise AssertionError("Error in calling API endpoint. Check your parameters or retry again later.")
    else:
        return response.json()['value']

# Wrapper function to call a paginated LTA API
def call_paginated_api(url,
                       headers = {},
                       params = {}):
    '''
    Calls the LTA API and handles paginated outputs

    :param url: LTA API endpoint URL
    :param headers: Optional header to pass into the API call (dict)
    :param params: Optional parameters to pass into the API call (dict)
    :return: Values returned from the LTA API for all pages
    '''

    # Call the API
    output = call_lta_api(url, headers, params = params)

    # If the length of the output is less than 500, then we can return the output
    if len(output) < 500:
        return output

    # If not, then we need to retrieve the next page of results
    else:

        # Initialise a list to hold the final output
        outputs = []
        outputs.extend(output)

        # Create a while loop
        while len(output) == 500:
            output = call_lta_api(url, headers, params = {'$skip': len(outputs)})
            outputs.extend(output)

        # Return all the outputs
        return outputs

def call_taxi_api():

    '''
    Calls the Taxi Availability API and exports a CSV of all taxis and their locations

    :return: Writes out a CSV file of the taxi availability
    '''

    # Securely obtain our API key from the environmental variables set by PyCharm
    api_key = os.getenv('datamall_api_key')

    # Set
    headers = {
        'AccountKey': api_key,
        'accept': 'application/json'
    }

    # Save the timestamp of the API call
    now = datetime.datetime.now().strftime('%Y-%m-%d_%H%MH')
    print(f'Calling the Taxi Availability API at {datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")}')

    # Call the Taxi Availability API
    taxis = call_paginated_api('http://datamall2.mytransport.sg/ltaodataservice/Taxi-Availability',
                               headers)
    print(f'Total number of available taxis: {len(taxis)}')

    # Convert it into a dataframe and add the timestamp as well
    taxis_df = pd.DataFrame(taxis)
    taxis_df['Timestamp'] = now

    # Export the data to a CSV
    taxis_df.to_csv(f'../data/tmp/TaxiAvailability_{now}.csv', index = False)

# Execute the function if the script is called
if __name__ == '__main__':
    call_taxi_api()