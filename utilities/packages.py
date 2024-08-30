import requests
from bs4 import BeautifulSoup
import json, os
import boto3
import pandas as pd
from datetime import datetime as dt
from typing import Tuple
from io import StringIO



# Initializing the S3 client
s3 = boto3.client('s3')

bucket_name = 'placement-trackers-storage'
scrapfly_api_key = os.getenv('scrapfly_api_key') 

def scrapfly_func(api_key, url):
    API_KEY = api_key
    url = url
    scrapfly_endpoint = "https://api.scrapfly.io/scrape"
    params = {'key':API_KEY,
          'url':url}

    response = requests.get(scrapfly_endpoint, params=params)
    if response.status_code == 200:
      output = json.loads(response.text)
      return output['result']['content']
    else:
       raise Exception(f"failed to fetch the page. Status code: {response.status_code}. \
                       Response: {response.text}")
     

def upload_to_s3(df: pd.DataFrame, file_name: str, folder_name: str):
    """Upload DataFrame as a CSV to S3."""
    # Create an in-memory buffer
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    s3.put_object(Bucket=bucket_name, Key=f"{folder_name}/{file_name}.csv", Body=csv_buffer.getvalue().encode('utf-8'))

def retrieve_announcement(url) -> Tuple[pd.DataFrame, pd.DataFrame]:
    
    html_content = scrapfly_func(scrapfly_api_key, url)

    if html_content:
        
        #soup = BeautifulSoup(response.content, "lxml")
        soup = BeautifulSoup(html_content, "html.parser")
        #return soup.prettify()  # Log the entire HTML content

        tables = soup.find_all('table')
        if not tables:
            raise ValueError("No tables found on the webpage")
        table = tables[0]
        
        headers = []
        rows = []

        for th in table.find_all('th'):
            headers.append(th.text.strip())

        for row in table.find_all('tr')[1:]:
            cells = row.find_all('td')
            row_data = []
            for cell in cells:
                link = cell.find('a')
                if link:
                    row_data.append(link['href'])
                    row_data.append(link.get_text().replace('\r', '').replace('\n', '').replace('\t', '').strip())
                else:
                    row_data.append(cell.get_text().replace('\r', '').replace('\n', '').replace('\t', '').strip())
            rows.append(row_data)

        headers = headers + ['Announcement']  # to Account for the text attached to the pdf
        df = pd.DataFrame(rows, columns=headers if headers else None)
        trading_halt = df[df['Announcement'].str.contains("Trading Halt", case=False, na=False)]

        return df, trading_halt

def retrieve_trading_halt(tdh_tickers) -> pd.DataFrame:
    target_df = pd.DataFrame(columns=[
        'id', 'document_release_date', 'document_date', 'url', 'relative_url',
        'header', 'market_sensitive', 'number_of_pages', 'size',
        'legacy_announcement', 'issuer_code', 'issuer_short_name',
        'issuer_full_name'
    ])
    
    for ticker in tdh_tickers['ASX Code']:
        tdh_url = f"https://www.asx.com.au/asx/1/company/{ticker}/announcements?count=20&market_sensitive=false"
        html_content = scrapfly_func(scrapfly_api_key, tdh_url)
        #soup = BeautifulSoup(html_content, "html.parser")
        #site_data = json.loads(soup.find('p').get_text())['data']
        site_data = json.loads(html_content)['data']
        output_df = pd.DataFrame(site_data)
        target_df = pd.concat([target_df, output_df], ignore_index=True)

    return target_df
    
# loop through close monitorings generated daily, if 'Proposed issue of securities' in legacy_announcment, 
# or do something similar to subsetting like data_interested = close_monitorings[close_monitorings['header'].str.contains("Proposed issue of securities", case=False, na=False)] 
# save the above to s3 and download the associated pdfs to a folder

def download_pdfs(file):
    
    file['document_release_date'] = pd.to_datetime(file['document_release_date'])
    
    for index, row in file.iterrows():
        url = row['url']
        issuer_code = row['issuer_code']
        release_date = row['document_release_date'].strftime("%Y_%m_%d")
        ID = row['id']
        file_name = f"{issuer_code}_{release_date}_id_{ID}.pdf"

        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an HTTPError if the HTTP request returned an unsuccessful status code

            # Upload the PDF file content to S3
            s3.put_object(
                Bucket=bucket_name,
                Key=f"PDFS/{file_name}",
                Body=response.content
                )

        except requests.exceptions.RequestException as e:
            print(f"Failed to download {url}. Error: {e}")
        except boto3.exceptions.Boto3Error as e:
            print(f"Failed to upload {file_name} to S3. Error: {e}")


# this would be called on trading_halt df;
# we load the 'monitoring.json' with the load_ticker_data func

#i.e monitoring_json = load_ticker_data()
def load_ticker_data(): 
    bucket_name = 'placement-trackers-storage'
    json_file_key = 'ticker_monitoring.json'

    try:
        obj = self.s3_client.get_object(Bucket=bucket_name, Key=json_file_key)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        return data
    except self.s3_client.exceptions.NoSuchKey:
        # Return an empty dictionary if the file doesn't exist
        return {}

def add_to_json(trading_halt_df, monitoring_json):

    # i.e updated_json = add_to_json(trading_halt_df, monitoring_json)

    ticker_data = monitoring_json
    for _, row in trading_halt_df.iterrows():

            ticker = row['ASX Code']

            if ticker not in ticker_data:
                ticker_data[ticker] = {
                    'added_date': today.isoformat(),
                    'status': 'Active'
                }
    return ticker_data
            




class TickerMonitor:
    # Class variables for S3 bucket and JSON file
    bucket_name = 'placement-trackers-storage'
    json_file_key = 'ticker_monitoring.json'

    def __init__(self):
        self.s3_client = boto3.client('s3')

    def load_ticker_data(self):
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.json_file_key)
            data = json.loads(obj['Body'].read().decode('utf-8'))
            return data
        except self.s3_client.exceptions.NoSuchKey:
            # Return an empty dictionary if the file doesn't exist
            return {}

    def save_ticker_data(self, data):
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.json_file_key,
            Body=json.dumps(data)
        )

    def update_json_with_tickers(self, df, closed_df ):
        """
        df: trading halt df for adding the tickers to be monitored,
        closed_df: is the close_monitoring dfs that has "Proposed issue of securities" in their 'header'
        """
        ticker_data = self.load_ticker_data()
        today = dt.utcnow().date()

        for _, row in df.iterrows():
            ticker = row['ASX Code']
            
            # If ticker is found in the closed DataFrame, update status to 'Closed' and remove it
            if ticker in closed_df['issuer_code'].values:
                ticker_data[ticker] = {
                    'added_date': today.isoformat(),
                    'status': 'Closed'
                }
                continue

            if ticker not in ticker_data:
                ticker_data[ticker] = {
                    'added_date': today.isoformat(),
                    'status': 'Active'
                }

        # Remove 'Closed' tickers
        closed_tickers = [ticker for ticker, info in ticker_data.items() if info['status'] == 'Closed']
        for ticker in closed_tickers:
            del ticker_data[ticker]


        # clean up expired tickers i.e ticker > 10 days
        expired_tickers = []
        for ticker, info in ticker_data.items():
            added_date = datetime.fromisoformat(info['added_date']).date()
            days_since_added = (today - added_date).days

            if days_since_added > 10:
                info['status'] = 'Inactive'
                expired_tickers.append(ticker)

        # Remove expired tickers
        for ticker in expired_tickers:
            del ticker_data[ticker]

        self.save_ticker_data(ticker_data)
        print("Updated ticker data saved to S3.")


    def monitor_tickers(self, df, closed_df):
        # Update tickers from the dataframe and check if they should be marked as closed
        self.update_json_with_tickers(df, closed_df)
