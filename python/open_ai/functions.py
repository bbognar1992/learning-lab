import json
import os

from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables from .env file
load_dotenv()

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# Load data from JSON files
def load_json(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


# Prepare batch file for OpenAI Batch API
def prepare_batch_files(data_list, instructions):
    file_names = []
    for k, v in data_list.items():
        file_name = f"inputs/batch_input_{k}.jsonl"
        file_names.append(file_name)
        with open(file_name, "w", encoding="utf-8") as batch_file:
            for idx, data in enumerate(v):
                content = f"{instructions}\n\nHere is the data:\n{json.dumps(data, ensure_ascii=False)}"
                batch_request = {
                    "custom_id": f"request-{idx + 1}",
                    "method": "POST",
                    "url": "/v1/chat/completions",
                    "body": {
                        "model": "gpt-4",
                        "messages": [
                            {"role": "system",
                             "content": "You are a data processing assistant."},
                            {"role": "user",
                             "content": content}
                        ],
                        "temperature": 0
                    }
                }
                batch_file.write(
                    json.dumps(batch_request, ensure_ascii=False) + "\n")
    return file_names


# Upload batch file
def upload_batch_file(file_name):
    MAX_REQUESTS = 50000
    MAX_FILE_SIZE_MB = 100
    file_size_mb = os.path.getsize(file_name) / (1024 * 1024)

    # Check file constraints
    if file_size_mb > MAX_FILE_SIZE_MB:
        raise Exception(f"Batch file size exceeds {MAX_FILE_SIZE_MB} MB.")
    with open(file_name, "r") as file:
        lines = file.readlines()
    if len(lines) > MAX_REQUESTS:
        raise Exception(f"Batch file exceeds {MAX_REQUESTS} requests.")

    # Upload file
    batch_file = client.files.create(file=open(file_name, "rb"),
                                     purpose="batch")
    return batch_file


# Create batch job
def create_batch_job(batch_file_id):
    batch_job = client.batches.create(
        input_file_id=batch_file_id,
        endpoint="/v1/chat/completions",
        completion_window="24h"
    )
    return batch_job


# Retrieve batch results
def get_batch_results(batch_job_id):
    batch_job = client.batches.retrieve(batch_job_id)
    if batch_job.status == "completed":
        # Get successful responses
        if batch_job.output_file_id:
            output_content = client.files.content(
                batch_job.output_file_id).content.decode("utf-8")
            with open(f"{batch_job.output_file_id}_output.jsonl", "w",
                      encoding="utf-8") as f:
                f.write(output_content)
        # Get errors if any
        if batch_job.error_file_id:
            error_content = client.files.content(
                batch_job.error_file_id).content.decode("utf-8")
            with open(f"{batch_job.output_file_id}errors.jsonl", "w",
                      encoding="utf-8") as f:
                f.write(error_content)
        return "Batch completed successfully."
    elif batch_job.status == "failed":
        return f"Batch job failed: {batch_job.errors}"
    else:
        return f"Batch job status: {batch_job.status}"
