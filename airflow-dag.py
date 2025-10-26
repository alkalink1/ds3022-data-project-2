<<<<<<< HEAD
from datetime import datetime, timedelta
import requests
import boto3
import time
import logging
from typing import List, Tuple
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS SQS client
sqs = boto3.client('sqs', region_name='us-east-1')

# Default arguments for the DAG
default_args = {
    'owner': 'eju2pk',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'sqs_message_pipeline',
    default_args=default_args,
    description='SQS message processing pipeline',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['sqs', 'message', 'pipeline'],
)

def populate_queue(**context):
    """
    Task 1: Populate the SQS queue by calling the scatter API.
    Returns the SQS queue URL.
    """
    logger = logging.getLogger(__name__)
    
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/eju2pk"
    
    print("TASK 1: Starting queue population...")
    print(f"Calling scatter API: {url}")
    
    try:
        logger.info(f"Calling scatter API: {url}")
        response = requests.post(url)
        response.raise_for_status()
        
        payload = response.json()
        sqs_url = payload['sqs_url']
        
        print(f"Successfully populated queue!")
        print(f"SQS URL: {sqs_url}")
        print(f"Queue should now contain 21 messages with random delays (30-900 seconds)")
        
        logger.info(f"Successfully populated queue. SQS URL: {sqs_url}")
        
        # Store the SQS URL in XCom for the next task
        return sqs_url
        
    except Exception as e:
        print(f"Error populating queue: {e}")
        logger.error(f"Error populating queue: {e}")
        raise

def collect_messages(**context):
    """
    Task 2: Poll the SQS queue continuously and collect all 21 messages.
    Returns a list of tuples (order_no, word) for all collected messages.
    """
    logger = logging.getLogger(__name__)
    
    # Get SQS URL from previous task
    sqs_url = context['task_instance'].xcom_pull(task_ids='populate_queue')
    
    collected_messages = []
    max_attempts = 100  # Prevent infinite loops
    attempt = 0
    
    print("TASK 2: Starting message collection...")
    print("Polling queue every 5 seconds until all 21 messages are collected...")
    
    logger.info("Starting message collection...")
    
    while len(collected_messages) < 21 and attempt < max_attempts:
        attempt += 1
        
        try:
            # Get queue attributes to monitor progress
            attributes = sqs.get_queue_attributes(
                QueueUrl=sqs_url,
                AttributeNames=[
                    'ApproximateNumberOfMessages',
                    'ApproximateNumberOfMessagesNotVisible',
                    'ApproximateNumberOfMessagesDelayed'
                ]
            )
            
            total_messages = (
                int(attributes['Attributes'].get('ApproximateNumberOfMessages', 0)) +
                int(attributes['Attributes'].get('ApproximateNumberOfMessagesNotVisible', 0)) +
                int(attributes['Attributes'].get('ApproximateNumberOfMessagesDelayed', 0))
            )
            
            print(f"Attempt {attempt}: Queue has {total_messages} total messages, collected {len(collected_messages)}/21")
            
            logger.info(f"Attempt {attempt}: Total messages in queue: {total_messages}, Collected: {len(collected_messages)}")
            
            # Try to receive messages
            response = sqs.receive_message(
                QueueUrl=sqs_url,
                MaxNumberOfMessages=10,  # Max allowed by SQS
                MessageAttributeNames=['All'],
                WaitTimeSeconds=5  # Long polling
            )
            
            # Check if we got any messages
            if 'Messages' in response:
                print(f"Found {len(response['Messages'])} messages in this poll")
                for message in response['Messages']:
                    try:
                        # Extract message attributes
                        attributes = message['MessageAttributes']
                        order_no = int(attributes['order_no']['StringValue'])
                        word = attributes['word']['StringValue']
                        
                        # Store the message data
                        collected_messages.append((order_no, word))
                        
                        # Delete the message from the queue
                        sqs.delete_message(
                            QueueUrl=sqs_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        
                        print(f"Collected message {order_no}: '{word}' (Total: {len(collected_messages)}/21)")
                        
                        logger.info(f"Collected message {order_no}: '{word}'")
                        
                    except KeyError as e:
                        print(f"Missing attribute in message: {e}")
                        logger.warning(f"Missing attribute in message: {e}")
                        # Still delete the message to avoid leaving it in queue
                        sqs.delete_message(
                            QueueUrl=sqs_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                    except Exception as e:
                        print(f"Error processing message: {e}")
                        logger.error(f"Error processing message: {e}")
                        # Still delete the message to avoid leaving it in queue
                        sqs.delete_message(
                            QueueUrl=sqs_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
            else:
                print("No messages available in this poll, waiting 5 seconds...")
                logger.info("No messages available in this poll, waiting...")
            
            # Sleep before next poll attempt
            time.sleep(5)
            
        except Exception as e:
            print(f"Error during message collection attempt {attempt}: {e}")
            logger.error(f"Error during message collection attempt {attempt}: {e}")
            time.sleep(10)  # Wait longer on error
    
    if len(collected_messages) < 21:
        print(f"Only collected {len(collected_messages)} out of 21 messages")
        logger.warning(f"Only collected {len(collected_messages)} out of 21 messages")
    else:
        print(f"Successfully collected all {len(collected_messages)} messages!")
        logger.info(f"Successfully collected all {len(collected_messages)} messages")
    
    # Store messages in XCom for the next task
    return collected_messages

def reassemble_phrase(**context):
    """
    Task 3: Reassemble the phrase by sorting messages by order_no and joining words.
    Returns the complete phrase.
    """
    logger = logging.getLogger(__name__)
    
    # Get messages from previous task
    messages = context['task_instance'].xcom_pull(task_ids='collect_messages')
    
    print("TASK 3: Starting phrase reassembly...")
    print(f"Processing {len(messages)} collected messages")
    
    if not messages:
        print("No messages to reassemble")
        logger.error("No messages to reassemble")
        return ""
    
    # Sort messages by order_no
    sorted_messages = sorted(messages, key=lambda x: x[0])
    
    print("Sorting messages by order_no...")
    print("Message order:")
    for order_no, word in sorted_messages:
        print(f"   {order_no}: '{word}'")
    
    # Extract words in order
    words = [word for order_no, word in sorted_messages]
    phrase = " ".join(words)
    
    print(f"Reassembled phrase: '{phrase}'")
    print(f"Total words: {len(words)}")
    
    logger.info(f"Reassembled phrase: '{phrase}'")
    logger.info(f"Message order: {[order_no for order_no, word in sorted_messages]}")
    
    # Store phrase in XCom for the next task
    return phrase

def submit_solution(**context):
    """
    Task 4: Submit the solution to the submission queue.
    """
    logger = logging.getLogger(__name__)
    
    # Get phrase from previous task
    phrase = context['task_instance'].xcom_pull(task_ids='reassemble_phrase')
    
    print("TASK 4: Submitting solution...")
    print(f"Phrase to submit: '{phrase}'")
    print("Submission queue URL: https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit")
    print("Required message attributes:")
    print("   - uvaid: 'eju2pk'")
    print(f"   - phrase: '{phrase}'")
    print("   - platform: 'airflow'")
    
    try:
        # Submit the solution to the submission queue
        response = sqs.send_message(
            QueueUrl="https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit",
            MessageBody="solution",
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': 'eju2pk'
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': 'airflow'
                }
            }
        )
        
        print(f"Successfully submitted solution!")
        print(f"Message ID: {response['MessageId']}")
        print(f"Response: {response}")
        
        logger.info(f"Successfully submitted solution. Message ID: {response['MessageId']}")
        logger.info(f"Response: {response}")
        
        return True
        
    except Exception as e:
        print(f"Error submitting solution: {e}")
        logger.error(f"Error submitting solution: {e}")
        raise

# Define the tasks
populate_queue_task = PythonOperator(
    task_id='populate_queue',
    python_callable=populate_queue,
    dag=dag,
)

collect_messages_task = PythonOperator(
    task_id='collect_messages',
    python_callable=collect_messages,
    dag=dag,
)

reassemble_phrase_task = PythonOperator(
    task_id='reassemble_phrase',
    python_callable=reassemble_phrase,
    dag=dag,
)

submit_solution_task = PythonOperator(
    task_id='submit_solution',
    python_callable=submit_solution,
    dag=dag,
)

# Define task dependencies
populate_queue_task >> collect_messages_task >> reassemble_phrase_task >> submit_solution_task
=======
# airflow DAG goes here
>>>>>>> f46cd7ddda0d4d55e1c4f67350bd42d543603481
