import json
import pandas as pd
import requests as r
import re
import boto3
import logging
from bs4 import BeautifulSoup
from io import StringIO
from datetime import datetime
import consonants as cs  

#  Logger setup (CloudWatch)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#  AWS Clients
s3 = boto3.client('s3')
sns = boto3.client('sns')
ssm = boto3.client('ssm')


#  Helper function to fetch SSM values
def get_ssm_param(param_name):
    return ssm.get_parameter(
        Name=param_name,
        WithDecryption=True
    )["Parameter"]["Value"]


#  SNS Success
def send_sns_success():
    try:
        success_sns_arn = get_ssm_param(cs.SUCCESS_ARN_PARAM)
        env = get_ssm_param(cs.ENVIRONMENT_PARAM)

        sns_message = f"{cs.COMPONENT_NAME} : {cs.SUCCESS_MSG}"

        logger.info(f"Sending SUCCESS SNS: {sns_message}")

        return sns.publish(
            TargetArn=success_sns_arn,
            Message=json.dumps({'default': sns_message}),
            Subject=f"{env} : {cs.COMPONENT_NAME}",
            MessageStructure="json"
        )

    except Exception as e:
        logger.error(f"SNS Success Error: {str(e)}", exc_info=True)


#  SNS Error
def send_error_sns():
    try:
        error_sns_arn = get_ssm_param(cs.ERROR_ARN_PARAM)
        env = get_ssm_param(cs.ENVIRONMENT_PARAM)

        sns_message = f"{cs.COMPONENT_NAME} : {cs.ERROR_MSG}"

        logger.error(f"Sending ERROR SNS: {sns_message}")

        return sns.publish(
            TargetArn=error_sns_arn,
            Message=json.dumps({'default': sns_message}),
            Subject=f"{env} : {cs.COMPONENT_NAME}",
            MessageStructure="json"
        )

    except Exception as e:
        logger.error(f"SNS Error Failed: {str(e)}", exc_info=True)


#  MAIN LAMBDA HANDLER
def lambda_handler(event, context):
    try:
        logger.info("Lambda execution started")

        # Timestamp
        timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')

        #  S3 Config
        bucket_name = "dino-8595"
        file_key = f"dino-output/DinoPocWebScrape_{timestamp}.csv"

        logger.info(f"S3 File Path: {file_key}")

        #  URL
        url = get_ssm_param(cs.URL_API_PARAM)
        headers = {
            "User-Agent": "SaroStockAnalysisBot/1.0"
        }

        #  Fetch page
        response = r.get(url, headers=headers, timeout=60)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'lxml')

        # Extract links
        urls = soup.find_all('a', href=True)
        links_and_names = [(u['href'], u.text) for u in urls]

        dino_data_clean = [
            links_and_names[i]
            for i in range(len(links_and_names))
            if links_and_names[i][0].startswith("/wiki/")
        ]

        dino_data_clean = dino_data_clean[:2370]

        #  DataFrame
        dino_df = pd.DataFrame(dino_data_clean, columns=['url', 'dinosaur'])
        dino_df['dinosaur'] = dino_df['dinosaur'].replace('', None)
        dino_df = dino_df.dropna(subset=['dinosaur'])

        dino_dict = dino_df.set_index('url')['dinosaur'].to_dict()

        dino_data = [
            ('https://en.wikipedia.org' + u, name)
            for u, name in dino_dict.items()
        ]

        dino_data = dino_data[53:]

        dino_urls = [
            ele for pair in dino_data
            for ele in pair
            if ele.startswith("https://en.wikipedia.org")
        ]

        #  Scraping details
        dino_info = []

        for i in range(min(200, len(dino_urls))):
            try:
                res = r.get(dino_urls[i], headers=headers, timeout=60)
                res.raise_for_status()

                soup = BeautifulSoup(res.text, 'lxml')
                para = soup.find_all('p')

                clean_para = [p.text.strip() for p in para][:4]
                dino_info.append(' '.join(clean_para))

            except Exception as inner_error:
                logger.error(f"Error scraping {i}: {str(inner_error)}")
                dino_info.append("Error")

        #  Combine
        dino_df = pd.DataFrame(dino_data[:len(dino_info)], columns=['URL', 'Dinosaur'])
        dino_details = pd.DataFrame(dino_info, columns=['info'])

        dino_df = pd.concat([dino_df, dino_details], axis=1)

        #  Extract height/weight
        heights, weights = [], []

        for ele in dino_df['info']:
            height = re.findall(r'\d+\smeters', str(ele))
            heights.append(height if height else '-')

            weight = re.findall(r'\d+\s(?:tonnes|kilograms)', str(ele))
            weights.append(weight if weight else '-')

        dino_df.drop('info', axis=1, inplace=True)

        dino_df = pd.concat([
            dino_df,
            pd.DataFrame(heights, columns=['Height']),
            pd.DataFrame(weights, columns=['Weight'])
        ], axis=1)

        #  Save to S3
        csv_buffer = StringIO()
        dino_df.to_csv(csv_buffer, index=False)

        s3.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=csv_buffer.getvalue()
        )

        logger.info("File uploaded successfully to S3")

        #  Send SUCCESS SNS
        send_sns_success()

        return {
            "statusCode": 200,
            "body": json.dumps(f"Uploaded to s3://{bucket_name}/{file_key}")
        }

    except Exception as e:
        logger.error(f"Main Error: {str(e)}", exc_info=True)

        #  Send ERROR SNS
        send_error_sns()

        return {
            "statusCode": 500,
            "body": json.dumps(f"Error: {str(e)}")
        }