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
