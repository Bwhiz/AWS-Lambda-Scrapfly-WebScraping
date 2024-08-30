import json, os
from datetime import datetime as dt
from utilities import scrapfly_func, upload_to_s3, retrieve_announcement, retrieve_trading_halt, download_pdfs
from utilities import send_email_notification


def lambda_handler(event, context):
    
    start_time = dt.now()
    
    #base_url = "https://www.asx.com.au/asx/v2/statistics/todayAnns.do"
    previousday_url = "https://www.asx.com.au/asx/v2/statistics/prevBusDayAnns.do"
    current_day_str = dt.now().strftime("%Y-%m-%d")

    try:
        
        # Retrieve announcements
        daily_announcements, trading_halt_announcements = retrieve_announcement(previousday_url)
    except Exception as e:
        send_email_notification(f"Error retrieving announcements: {str(e)}")
        return {
            'statusCode': 500,
            'url':previousday_url,
            'body': json.dumps(f"Error retrieving announcements: {str(e)}")
        }    
        
    try:
        # Retrieve close monitoring data
        close_monitorings = retrieve_trading_halt(trading_halt_announcements)
    except Exception as e:
        send_email_notification(f"Error retrieving trading halt data: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error retrieving trading halt data: {str(e)}")
        }
    try:
        # Upload DataFrames to S3
        upload_to_s3(daily_announcements, f'daily_data_{current_day_str}', 'daily_announcements')
        upload_to_s3(trading_halt_announcements, f'trading_halt_data_{current_day_str}', 'tradingHalt_tickers')
        #upload_to_s3(close_monitorings, f'close_monitoring_data_{current_day_str}', 'xCloser_monitorings')
        
    except Exception as e:
        send_email_notification(f"Error uploading files to S3: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error uploading files to S3: {str(e)}")
        }
    try:
        # download pdfs
        # call the function on 'close_monitorings'
        download_pdfs(close_monitorings)
    except Exception as e:
        send_email_notification(f"Error downloading pdfs to S3: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error downloading pdfs to S3: {str(e)}")
        }
        
    # Successful execution
    total_csvs = len(daily_announcements) + len(trading_halt_announcements) + len(close_monitorings)
    total_pdfs = len(close_monitorings)
    
    end_time = dt.now()
    duration = end_time - start_time
    duration_in_seconds = duration.total_seconds()

    # send success email alert :
    success_body = f"""
{total_csvs} total rows of CSV files successfully uploaded to S3 & {total_pdfs} PDFs was uploaded to s3.
    
It took roughly {duration_in_seconds} Seconds to run, with an allocated memory of 128 MB, which is 0.125 GB,
The estimated Usage for this run is :
    {duration_in_seconds * 0.125} GB-seconds.
    
If maintained as an average for a month, the monthly consumption would be : 
    {duration_in_seconds * 0.125 * 23} GB-seconds
            
For Reference, the Monthly Free Tier Consumption Cap for AWS Lambda is : 
    400,000 GB-seconds.
    
"""
    success_subject = 'Automated Success Notification - AWS Lambda' 
    send_email_notification(success_body, success_subject)
    return {
        'statusCode': 200,
        'body': json.dumps(success_body)}
    
