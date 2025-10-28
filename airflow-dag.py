from datetime import datetime, timedelta
import requests
import boto3
import time
from typing import List, Tuple
from airflow import DAG
from airflow.operators.python import PythonOperator

# AWS SQS client 
sqs = boto3.client("sqs", region_name="us-east-1")

# Default arguments for the DAG
default_args = {
    "owner": "eju2pk",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG (manual trigger; no schedule)
dag = DAG(
    "sqs_message_pipeline",
    default_args=default_args,
    description="SQS message processing pipeline",
    schedule_interval=None,
    catchup=False,
    tags=["sqs", "message", "pipeline"],
)

def populate_queue(**context) -> str:
    """
    Task 1: Populate the SQS queue by calling the scatter API (POST).
    Returns the SQS queue URL via XCom.
    """
    ti = context["ti"]
    log = ti.log

    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/eju2pk"
    log.info("TASK 1: Starting queue population via scatter API.")
    log.info("Calling scatter API: %s", url)

    try:
        # POST per assignment spec
        response = requests.post(url)
        response.raise_for_status()

        payload = response.json()
        sqs_url = payload["sqs_url"]

        # Informational logging for visibility in Airflow task logs
        log.info("Successfully populated queue.")
        log.info("SQS URL: %s", sqs_url)
        log.info("Queue should contain 21 messages with random delays (30–900 seconds).")

        # XCom return (Airflow 2: return value is pushed)
        return sqs_url

    except Exception as e:
        log.error("Error populating queue: %s", e)
        raise

def collect_messages(deadline_seconds: int = 1080, **context) -> List[Tuple[int, str]]:
    """
    Task 2: Poll the SQS queue continuously and collect all 21 messages.
    Returns a list of tuples (order_no, word) via XCom.
    """
    ti = context["ti"]
    log = ti.log

    # Get SQS URL from previous task (XCom)
    sqs_url = ti.xcom_pull(task_ids="populate_queue")

    collected: List[Tuple[int, str]] = []
    seen_receipt_handles = set()  # avoid double-counting the same SQS message

    log.info("TASK 2: Starting message collection…")
    log.info("Polling queue every 5 seconds until all 21 messages are collected or the %ss deadline is reached…", deadline_seconds)

    start_time = time.time()
    # Deadline set to 18 minutes (1080s), exceeding the 900s max delay.

    while len(collected) < 21 and (time.time() - start_time) < deadline_seconds:
        try:
            # Get attributes to monitor queue progress
            attrs = sqs.get_queue_attributes(
                QueueUrl=sqs_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "ApproximateNumberOfMessagesDelayed",
                ],
            )
            visible = int(attrs["Attributes"].get("ApproximateNumberOfMessages", 0))
            not_visible = int(attrs["Attributes"].get("ApproximateNumberOfMessagesNotVisible", 0))
            delayed = int(attrs["Attributes"].get("ApproximateNumberOfMessagesDelayed", 0))
            total = visible + not_visible + delayed

            log.info("Queue status — visible=%s, not_visible=%s, delayed=%s (total=%s); collected %s/21",
                     visible, not_visible, delayed, total, len(collected))

            # Long-poll receive up to 10 messages (SQS max)
            resp = sqs.receive_message(
                QueueUrl=sqs_url,
                MaxNumberOfMessages=10,
                MessageAttributeNames=["All"],
                WaitTimeSeconds=5,  # long polling
            )

            if "Messages" not in resp:
                log.info("No messages in this poll; sleeping 5s before retry…")
                time.sleep(5)
                continue

            log.info("Received %s messages in this poll.", len(resp["Messages"]))

            for msg in resp["Messages"]:
                rh = msg["ReceiptHandle"]
                # Dedup: if this receipt handle has already been processed, skip deletion/logging
                if rh in seen_receipt_handles:
                    log.info("Duplicate delivery detected; skipping already-seen message.")
                    continue

                try:
                    mattrs = msg["MessageAttributes"]
                    order_no = int(mattrs["order_no"]["StringValue"])  # required attr
                    word = mattrs["word"]["StringValue"]  # required attr

                    collected.append((order_no, word))
                    seen_receipt_handles.add(rh)

                    # Delete after successful parse to prevent re-delivery
                    sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=rh)
                    log.info("Collected message %s: '%s' (total %s/21)", order_no, word, len(collected))

                except KeyError as e:
                    # Malformed message: delete to avoid poison loop
                    log.warning("Missing attribute in message (%s); deleting.", e)
                    sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=rh)
                except Exception as e:
                    # Any other parse/processing error: delete to avoid build-up
                    log.error("Error processing message: %s; deleting.", e)
                    sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=rh)

            # Be polite between polls
            time.sleep(5)

        except Exception as e:
            # Back off briefly on transient AWS/network errors
            log.error("Error during message collection: %s; backing off for 10s…", e)
            time.sleep(10)

    if len(collected) < 21:
        log.warning("Deadline reached: collected %s / 21 messages.", len(collected))
    else:
        log.info("Successfully collected all 21 messages!")

    # XCom return
    return collected

def reassemble_phrase(**context) -> str:
    """
    Task 3: Reassemble the phrase by sorting messages by order_no and joining words.
    Returns the complete phrase via XCom.
    """
    ti = context["ti"]
    log = ti.log

    messages: List[Tuple[int, str]] = ti.xcom_pull(task_ids="collect_messages") or []

    log.info("TASK 3: Starting phrase reassembly…")
    log.info("Processing %s collected messages.", len(messages))

    if not messages:
        log.error("No messages to reassemble; returning empty string.")
        return ""

    # Sort by order number and join
    sorted_messages = sorted(messages, key=lambda x: x[0])
    order_list = [order_no for order_no, _ in sorted_messages]
    log.info("Sorted order numbers: %s", order_list)

    words = [word for _, word in sorted_messages]
    phrase = " ".join(words)

    log.info("Reassembled phrase: '%s'", phrase)
    return phrase

def submit_solution(**context) -> bool:
    """
    Task 4: Submit the solution to the submission queue.
    """
    ti = context["ti"]
    log = ti.log

    phrase: str = ti.xcom_pull(task_ids="reassemble_phrase") or ""

    submit_queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    log.info("TASK 4: Submitting solution to submission queue…")
    log.info("Submission queue URL: %s", submit_queue_url)
    log.info("Message attributes: uvaid='eju2pk', platform='airflow'")

    try:
        resp = sqs.send_message(
            QueueUrl=submit_queue_url,
            MessageBody="solution",
            MessageAttributes={
                "uvaid": {"DataType": "String", "StringValue": "eju2pk"},
                "phrase": {"DataType": "String", "StringValue": phrase},
                "platform": {"DataType": "String", "StringValue": "airflow"},
            },
        )

        log.info("Successfully submitted solution. MessageId=%s", resp["MessageId"])
        return True

    except Exception as e:
        log.error("Error submitting solution: %s", e)
        raise

# Define the tasks
populate_queue_task = PythonOperator(
    task_id="populate_queue",
    python_callable=populate_queue,
    dag=dag,
)

collect_messages_task = PythonOperator(
    task_id="collect_messages",
    python_callable=collect_messages,
    op_kwargs={"deadline_seconds": 1080},  # 18 minutes (>900s max delay)
    dag=dag,
)

reassemble_phrase_task = PythonOperator(
    task_id="reassemble_phrase",
    python_callable=reassemble_phrase,
    dag=dag,
)

submit_solution_task = PythonOperator(
    task_id="submit_solution",
    python_callable=submit_solution,
    dag=dag,
)

# Define task dependencies
populate_queue_task >> collect_messages_task >> reassemble_phrase_task >> submit_solution_task
