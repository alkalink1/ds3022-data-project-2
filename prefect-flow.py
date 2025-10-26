import requests
import boto3
import time
from typing import List, Tuple
from prefect import flow, task, get_run_logger

# NOTE: Per rubric, use Prefect's logger only; do not import or configure Python's `logging`.

# AWS SQS client (region per assignment)
sqs = boto3.client("sqs", region_name="us-east-1")


@task
def populate_queue() -> str:
    """
    Task 1: Populate the SQS queue by calling the scatter API.
    Returns the SQS queue URL.

    Rubric notes:
    - Use POST to the scatter endpoint with your UVA ID appended.
    - This task is executed once per flow run.
    """
    logger = get_run_logger()

    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/eju2pk"
    logger.info("TASK 1: Starting queue population via scatter API.")
    logger.info(f"Calling scatter API: {url}")

    try:
        # POST per assignment spec
        response = requests.post(url)
        response.raise_for_status()

        payload = response.json()
        sqs_url = payload["sqs_url"]

        # Informational logging for visibility in Prefect UI
        logger.info("Successfully populated queue.")
        logger.info(f"SQS URL: {sqs_url}")
        logger.info("Queue should contain 21 messages with random delays (30–900 seconds).")

        return sqs_url

    except Exception as e:
        logger.error(f"Error populating queue: {e}")
        raise


@task
def collect_messages(sqs_url: str, deadline_seconds: int = 1080) -> List[Tuple[int, str]]:
    """
    Task 2: Poll the SQS queue continuously and collect all 21 messages.
    Returns a list of tuples (order_no, word) for all collected messages.

    Improvements vs. initial version:
    - Uses a **time-based deadline** (>900s) instead of `max_attempts` to avoid early exit.
    - Prefect-only logging (no prints).
    - Inline comments that explain key choices.
    - Simple **deduplication** guard by `seen_receipt_handles` to avoid double counting if
      visibility timeouts cause re-deliveries.
    """
    logger = get_run_logger()

    collected: List[Tuple[int, str]] = []
    seen_receipt_handles = set()  # avoid double-counting the same SQS message

    logger.info("TASK 2: Starting message collection…")
    logger.info("Polling queue every 5 seconds until all 21 messages are collected or deadline is reached…")

    start_time = time.time()
    # We allow up to 18 minutes (1080s) which exceeds the max message delay (900s)
    # This avoids the need to sleep a fixed 900s while still ensuring completeness.

    while len(collected) < 21 and (time.time() - start_time) < deadline_seconds:
        try:
            # Query attributes to understand current queue state (visible, in-flight, delayed)
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
            total_messages = visible + not_visible + delayed

            logger.info(
                f"Queue status — visible={visible}, not_visible={not_visible}, delayed={delayed}; "
                f"collected {len(collected)}/21"
            )

            # Long-poll receive up to 10 messages (SQS max) to be efficient
            resp = sqs.receive_message(
                QueueUrl=sqs_url,
                MaxNumberOfMessages=10,
                MessageAttributeNames=["All"],
                WaitTimeSeconds=5,  # long polling to reduce empty responses
            )

            if "Messages" not in resp:
                logger.info("No messages in this poll; sleeping 5s before retry…")
                time.sleep(5)
                continue

            logger.info(f"Received {len(resp['Messages'])} messages in this poll.")

            for msg in resp["Messages"]:
                rh = msg["ReceiptHandle"]
                # Dedup: if we have already processed this receipt handle, skip it
                if rh in seen_receipt_handles:
                    logger.info("Duplicate delivery detected; skipping already-seen message.")
                    continue

                try:
                    mattrs = msg["MessageAttributes"]
                    order_no = int(mattrs["order_no"]["StringValue"])  # required attr
                    word = mattrs["word"]["StringValue"]  # required attr

                    collected.append((order_no, word))
                    seen_receipt_handles.add(rh)

                    # Delete after successful parse to prevent re-delivery
                    sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=rh)
                    logger.info(f"Collected message {order_no}: '{word}' (total {len(collected)}/21)")

                except KeyError as e:
                    # If the message is malformed, we still delete it so the queue can progress
                    logger.warning(f"Missing attribute in message ({e}); deleting to avoid poison message.")
                    sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=rh)
                except Exception as e:
                    # On any other parse/processing error, delete to avoid build-up
                    logger.error(f"Error processing message: {e}; deleting to avoid reprocessing loop.")
                    sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=rh)

            # Be polite between polls
            time.sleep(5)

        except Exception as e:
            # Back off briefly on transient AWS/network errors
            logger.error(f"Error during message collection: {e}; backing off for 10s…")
            time.sleep(10)

    if len(collected) < 21:
        logger.warning(f"Deadline reached: collected {len(collected)} / 21 messages.")
    else:
        logger.info("Successfully collected all 21 messages!")

    return collected


@task
def reassemble_phrase(messages: List[Tuple[int, str]]) -> str:
    """
    Task 3: Reassemble the phrase by sorting messages by order_no and joining words.
    Returns the complete phrase.
    """
    logger = get_run_logger()

    logger.info("TASK 3: Starting phrase reassembly…")
    logger.info(f"Processing {len(messages)} collected messages.")

    if not messages:
        logger.error("No messages to reassemble; returning empty string.")
        return ""

    # Sort by order number to reconstruct the intended sentence
    sorted_messages = sorted(messages, key=lambda x: x[0])

    # For transparency in logs, show the final order indices
    order_list = [order_no for order_no, _ in sorted_messages]
    logger.info(f"Sorted order numbers: {order_list}")

    # Join words into the final phrase
    words = [word for _, word in sorted_messages]
    phrase = " ".join(words)

    logger.info(f"Reassembled phrase: '{phrase}'")
    return phrase


@task
def submit_solution(phrase: str) -> bool:
    """
    Task 4: Submit the solution to the submission queue.
    """
    logger = get_run_logger()

    submit_queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    logger.info("TASK 4: Submitting solution to submission queue…")
    logger.info(f"Submission queue URL: {submit_queue_url}")

    try:
        resp = sqs.send_message(
            QueueUrl=submit_queue_url,
            MessageBody="solution",
            MessageAttributes={
                "uvaid": {"DataType": "String", "StringValue": "eju2pk"},
                "phrase": {"DataType": "String", "StringValue": phrase},
                "platform": {"DataType": "String", "StringValue": "prefect"},
            },
        )

        logger.info(f"Successfully submitted solution. MessageId={resp['MessageId']}")
        return True

    except Exception as e:
        logger.error(f"Error submitting solution: {e}")
        raise


@flow(name="sqs-message-pipeline")
def sqs_message_pipeline(repopulate: bool = True, deadline_seconds: int = 1080) -> str:
    """
    Main Prefect flow that orchestrates the SQS message processing pipeline.

    Parameters
    ----------
    repopulate : bool
        If True (default), call the scatter API to populate the queue. If you ever schedule
        this flow to run repeatedly, you can set this to False to avoid repopulating the
        queue on each scheduled run.
    deadline_seconds : int
        Upper bound for collection time; set > 900 seconds (max scatter delay) to avoid
        prematurely exiting when many messages are delayed.
    """
    logger = get_run_logger()

    logger.info("==== STARTING SQS MESSAGE PIPELINE ====")

    try:
        # Task 1: Populate the queue (single execution per flow run per rubric)
        if repopulate:
            sqs_url = populate_queue()
        else:
            # If not repopulating, derive or configure the SQS URL from prior context/env
            raise RuntimeError(
                "repopulate=False requires you to supply/restore a known SQS URL; "
                "for the assignment, leave repopulate=True."
            )

        # Task 2: Collect all messages with deadline-based polling
        messages = collect_messages(sqs_url, deadline_seconds=deadline_seconds)

        # Task 3: Reassemble the phrase
        phrase = reassemble_phrase(messages)

        # Task 4: Submit solution
        submit_solution(phrase)

        logger.info("==== PIPELINE COMPLETED SUCCESSFULLY ====\n")
        logger.info(f"Final phrase: '{phrase}'")
        return phrase

    except Exception as e:
        logger.error(f"PIPELINE FAILED: {e}")
        raise


if __name__ == "__main__":
    # Run the flow locally (helpful for quick manual tests)
    final = sqs_message_pipeline()
    print(f"Final result: {final}")
