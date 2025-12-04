# import libraries 
from datetime import datetime, timezone
import os, requests
from urllib.parse import quote
import json
import time
from azure.identity import DefaultAzureCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
from azure.core.credentials import AzureKeyCredential

from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from dataclasses import dataclass
from itertools import tee

####### - SETUP CONNECTIONS - #######

current_directory = os.getcwd()
connection_file = os.path.join(current_directory, "connections.json")
connection_details = {}
with open(connection_file, "r+") as connection_data:
    connection_details = json.load(connection_data)

@dataclass
class ConnectionsConfig:
    """Connection settings loaded from connections.json."""
    di_endpoint: str
    di_key: str
    storage_account_url: str

# set `<your-endpoint>` and `<your-key>` variables with the values from the Document Intelligence  
endpoint = connection_details['DI-Endpoint'] 
key = connection_details['DI-Key']
 
# Options for mode: local, azure
# local: reads all files in directory specified by "directory_path"
# azure: reads all files in blob container in Azure storage account specified by "connection_string" and "container_name"
mode = "local"
 
# If using a local directory, set directory here. Make sure to use forward slashes (/) and not backslashs (\)
directory_path = "./pdf/test-wcvi"
 
# Azure blob container setttings, set `<connection_string>` and `<container_name>` for blob container in storage account
connection_string = connection_details['SA-endpoint']
container_name = "wcvi"

# Azure blob output container setttings, set `<connection_string>` and `<container_name>` for blob container in storage account you want to write a JSON output too
# Script will only attempt to upload to a blob container if output_azure is set to True
output_azure = False
output_connection_string = connection_string
output_container_name = "wcvi"
output_directory_name = "wcvi-sil-2023/update_jsons"

# Determine model to be used here. Check with Document Intelligence Studio for the name of the model used:
extraction_model_id = "wcvi-sil-6"

OK, ERR = "[OK]", "[ERR]"

def _rfc1123_now():
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

# This is a custom serialization function to convert object type BoundingRegion and polygon into a JSON format
# JSON library has no built-in support for serializing arbitrary Python objects such as BoundingRegion
def serialize(obj):
    # If object is BoundingRegion, define page number and polygon
    if hasattr(obj, 'page_number') and hasattr(obj, 'polygon'):   
        return {  
            'page_number': obj.page_number,  
            'polygon': serialize(obj.polygon)  # Serialize the polygon as it is also an arbitrary object 
        }

    # If object is polygon (list of points), define each point contained in the polygon
    elif isinstance(obj, list) and all(hasattr(point, 'x') and hasattr(point, 'y') for point in obj):   
        return obj
    
    # If object is type supported by JSON library return it as is
    elif isinstance(obj, (dict, list, str, int, float, bool, type(None))):  
        return obj
    
    # Catch error in case object is encountered that this code can not serialize 
    else:  
        raise TypeError(f"Object of type '{type(obj).__name__}' is not JSON serializable")

def upload_file_to_blob_via_aad(sa_endpoint: str, container: str, directory: str, local_path: str, timeout: int = 30) -> str:
    """
    Uploads local_path to https://<account>.blob.core.windows.net/<container>/<directory>/<basename(local_path)>
    using AAD (Bearer token). Overwrites if it exists. Returns the blob URL.
    """
    # Acquire token
    token = DefaultAzureCredential().get_token("https://storage.azure.com/.default").token

    # Build target URL
    blob_name = f"{directory.strip('/')}/{os.path.basename(local_path)}"
    url = f"{sa_endpoint.rstrip('/')}/{container}/{quote(blob_name)}"

    # Read file bytes
    with open(local_path, "rb") as f:
        body = f.read()

    headers = {
        "Authorization": f"Bearer {token}",
        "x-ms-version": "2021-10-04",
        "x-ms-date": _rfc1123_now(),
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "application/json; charset=utf-8",
        "Content-Length": str(len(body)),
    }

    r = requests.put(url, headers=headers, data=body, timeout=timeout)
    if r.status_code != 201:
        raise RuntimeError(f"Upload failed {r.status_code}: {r.text[:300]}")
    return url

def chunk_list(lst, size):
    return [lst[i:i+size] for i in range(0, len(lst), size)]

def content_field(value):
    # print(field, value['content'])
    result_content = {}
    result_content["content"] = value['content']
    result_content["confidence"] = value['confidence']
    for region in value.bounding_regions:
        polygon_list = region["polygon"]
        if len(polygon_list) != 8:
            result_content["polygon"] = polygon_list
        else:
            for value in region:
                result_content["polygon"] = chunk_list(polygon_list, 2)
    return result_content

def extract_object_array(field, dict_data):
    # result_list = []
    result_dict = {}
    for data in dict_data:
        holder = {field: data}
        holder_dict = extract_object(holder)
        for holder_field, holder_value in holder_dict.items():
            for field_name, field_value in holder_value.items():
                result_dict[field_name] = field_value
    return result_dict

def extract_object(dict_data):
    result_dict_data = {}
    for field, attributes in dict_data.items():
        # print(field, attributes)
        dict_data_type = attributes['type']
        attribute_check_list = attributes.keys()

        if dict_data_type == 'object':
            # print(field, "is object")
            if 'content' in attribute_check_list or 'valueObject' in attribute_check_list:
                inner_object = attributes['valueObject']
                result_dict_data[field] = extract_object(inner_object)

        elif dict_data_type == 'array':
            # print(field, "is array")
            # print(field)
            # print(attributes.keys(), flush = True)
            if 'content' in attribute_check_list or 'valueArray' in attribute_check_list:
                inner_object = attributes['valueArray']
                result_dict_data[field] = {'item': extract_object_array(field, inner_object)}

        else:
            # print(field, "is string")
            if 'content' in attribute_check_list:
            # print("Attribute pass")
                result_dict_data[field] = content_field(attributes)

    return result_dict_data

# This is how function print_analyzed_contents(DocumentAnalysisClient() result, string file_name) print_analyzed_contents takes variable result - class DocumentAnalysisClient(). Function access attribute `documents` for data. Refer to DI documenntation for detail.
def print_analyzed_contents(result, file_name):
    analyzed_data = {}
    for doc in result['documents']:
        doc_type = doc.doc_type
        page = doc.bounding_regions[0]['pageNumber']
        confidence = doc.confidence
        doc_data = {}
        doc_data['page'] = page
        doc_data ['confidence'] = confidence
        doc_field = extract_object(doc.fields)
        for field, value in doc_field.items():
            doc_data[field] = value
        if doc_type not in analyzed_data:
            analyzed_data[doc_type] = [doc_data]
        else:
            analyzed_data[doc_type].append(doc_data)
    # for doc in result['documents']:
    #     doc_data_analyzed = {}
    #     if doc['fields'] is None:
    #         continue        
    #     doc_data = doc['fields']
    #     for field, value in doc_data.items():
    #         # print('KEY:', field, 'value:', value.content, 'confidence:', value.confidence)
    #         # if value.content is None:
    #         #     continue
    #         # for region in value.bounding_regions:
    #             # print('polygon:', region.polygon)
    #         doc_data_analyzed = extract_object(doc_data)
    #     for field, value in doc_data_analyzed.items():
    #         analyzed_data[field] = value
    # print(analyzed_data)
    
    ### Check container for existing result and redownload the result for no rewrite. Use this if you do not want to overwrite content above. 
    # Else if all runs are to rewrite, comment out the code block
    # if output_azure and container_client.get_blob_client(file_name).exists():
        # downloaded_blob = container_client.download_blob(file_name)
        # analyzed_data = json.loads(downloaded_blob.readall())
    ###
    # Properly naming the given file_name input for writing.
    # Default to write at the same directory as the terminal is running
    current_directory = os.getcwd()
    target_directory = current_directory + '/json/' + output_container_name
    os.makedirs(target_directory, exist_ok = True)
    output_file = os.path.join(target_directory, file_name)


    # Changing analyzed_data to writable json format before writing to output_file
    json_data = json.dumps(analyzed_data, default = serialize, indent=4)

    # Write json_data to file_name. output_file will now exists locally 
    with open(output_file, 'w') as f:
        f.write(json_data)
    # print(f"Analysis data appended to: {output_file}")
    
        # If output_azure is true, then proceed to upload output_file to the target blob container
    # If output_azure is true, upload output_file to Azure Blob
    if output_azure:
        try:
            blob_url = upload_file_to_blob_via_aad(
                sa_endpoint=cfg.storage_account_url,   # e.g. "https://<account>.blob.core.windows.net"
                container=output_container_name,
                directory=output_directory_name,
                local_path=output_file
            )
            print(f"{OK} Analysis data uploaded to Azure Blob: {blob_url}")
        except Exception as e:
            print(f"{ERR} Failed to upload to Azure: {e}")

def printdict(dictdata):
    for key, value in dictdata.items():
        print('KEY:', key)
        if type(value) is dict:
            print('\n\t', end = '')
            printdict(value)
        elif type(value) is list:
            for item in value:
                if type(item) is dict:
                    print('\n\t', end = '')
                    printdict(item)
                else:
                    print('\n\tVALUE:', item)
        else:
            print('\n\tVALUE:', value)
                    

# function analyzed_local_documents()
# function reads local files from a directory, connect to a DI model from DI resource in Azure cloud and runs print_analyzed_contents() on them.
def analyze_local_documents():
    # Connect to DI resource on Azure cloud. Also verifies permission here.
    # document_analysis_client = DocumentIntelligenceClient(endpoint=endpoint, credential=AzureKeyCredential(key))
    document_analysis_client = DocumentIntelligenceClient(
        endpoint=cfg.di_endpoint,
        credential=AzureKeyCredential(cfg.di_key),
    )
    # Going through files the directory_path from above
    for filename in os.listdir(directory_path):
        # Only check for the following file format. These are also only the current formats supported. Check DI blogpost for update when run.
        if filename.endswith((".pdf", ".jpg", ".png", ".bmp", ".tiff")):  
            file_path = os.path.join(directory_path, filename)

            with open(file_path, "rb") as file:
                # Change the model name to fit with the corresponding model. Default is: prebuilt-document
                # currently only read pdf files
                if filename.endswith(".pdf"): 
                    print(".", end='', flush=True)
                    poller = document_analysis_client.begin_analyze_document(extraction_model_id, file)
                # If not, then treat the rest as an image and proceed as such
                else:  
                    poller = document_analysis_client.begin_analyze_document("prebuilt-document", file, content_type="image/jpeg")
                # At this stage, OCR is completed, and current data is available with the attribute `result` of the variable above `poller`
                result = poller.result()
                # Defining an appropriate file_name to be produced.
                # file_name = "local " + filename.replace(' ', '') + " " + "analyzed_data.json"
                file_name = filename[:-4]+".json"
                # for page in result.pages:
                #     printdict(page)
                # print(result.paragraphs)
                print_analyzed_contents(result, file_name)

def poller_list_create(document_analysis_client, container_client, list_of_blob, processed_blob_list):
    poller_list = []
    poller_create_time = datetime.now()
    # index = 0
    for blob in list_of_blob:
        blob_name = blob
        # print(blob_name)
        if blob_name[:-4] not in processed_blob_list and blob_name.endswith(".pdf") and blob_name.startswith("pdf"):
            # if index >= 10:
            #     break
            blob_client = container_client.get_blob_client(blob)
            blob_url = blob_client.url
            # print('Running extraction model on', blob, 'at', blob_url)
            try:
                poller = document_analysis_client.begin_analyze_document(extraction_model_id, AnalyzeDocumentRequest(url_source = blob_url))
                poller_list.append(poller)
            except:
                print("encountered error -- stopping before:", blob_name)
                break
            print('.', end='', flush = True)
            # print(blob_name[5:11], flush = True, end = '|')
            # index += 1
    poller_end_time = datetime.now()
    seconds = (poller_end_time - poller_create_time).total_seconds()
    print(f"PL TIME: {seconds} secs.", flush=True)
    return poller_list

# function analyze_azure_documents()
# function reads files from an azure blob container - available at container level - directory, connect to a DI model from DI resource in Azure cloud and runs print_analyzed_contents() on them.
def analyze_azure_documents():
    # Connect to DI resource on Azure cloud. Also verifies permission here.
    # document_analysis_client = DocumentIntelligenceClient(endpoint=endpoint, credential=AzureKeyCredential(key))
    document_analysis_client = DocumentIntelligenceClient(
        endpoint=cfg.di_endpoint,
        credential=AzureKeyCredential(cfg.di_key),
    )
    # Connect to blob resource on Azure cloud. Connecting to the resource by connection_string, and to the exact container via container_name
    cred = DefaultAzureCredential()
    blob_service = BlobServiceClient(account_url=cfg.storage_account_url, credential=cred)
    container_client = blob_service.get_container_client(container_name)
    reader_start_time = datetime.now()
    
    # Storing all file accesses' via container_client.list_blobs()
    blob_list = container_client.list_blob_names()
    
    original_blob_list = []
    processed_blob_list = []
    blob_list_backup, blob_list_backup_2 = tee(blob_list)

    for blob_name in blob_list_backup:
        if blob_name.startswith('raw_jsons/'):
            processed_blob_list.append(blob_name[10:-5])
        else:
            original_blob_list.append(blob_name[:-4])
    
    unprocess_blob_list = [blob.split('/')[1] for blob in original_blob_list if blob not in processed_blob_list and blob.startswith("pdf")]
    # print(unprocess_blob_list, len(unprocess_blob_list))
    
    print(f'Container: {container_name}\t|\t Model: {extraction_model_id}')
    print('Client connect, blob accessed')
    index = 0
    
    # Proceed to go through the list of files here. Since container is a blob container, iterating variable is called blob
    poller_list = poller_list_create(document_analysis_client, container_client, blob_list_backup_2, processed_blob_list)
    
    tps_limit = 8
    tps = 0
    print("Extracting now", end = '', flush = True)
    for poller in poller_list:
        # start_time = datetime.now()
        if tps >= tps_limit:
            time.sleep(1)
            tps = 0
        result = poller.result()
        # end_time = datetime.now()
        # seconds = (end_time - start_time).total_seconds()
        # print(f"RS TIME: {seconds} secs.", flush=True)
        # Defining an appropriate file_name to be produced, before running print_analyzed_contents()
        print_analyzed_contents(result, unprocess_blob_list[index] + ".json")
        # end_time = datetime.now()
        # seconds = (end_time - start_time).total_seconds()
        # print(f"JC TIME: {seconds} secs.", flush=True)
        index += 1
        tps += 1
        print('.', end='', flush = True)
    print('\n')
    reader_end_time = datetime.now()
    seconds = (reader_end_time - reader_start_time).total_seconds()
    print(f"ALL files Executed in {seconds} secs.", flush=True)

def load_connections_config() -> ConnectionsConfig:
    """
    Load ./connections.json (CWD). Expects:
      - DI-Key
      - DI-Endpoint (or legacy DI_Endpoint)
      - SA-endpoint (account URL, https://<storage>.blob.core.windows.net)
    """
    config_path = os.path.join(os.getcwd(), "connections.json")
    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    di_endpoint = (data.get("DI-Endpoint") or data.get("DI_Endpoint"))
    if not di_endpoint:
        raise ValueError("connections.json missing 'DI-Endpoint' (or 'DI_Endpoint').")
    di_key = data.get("DI-Key")
    if not di_key:
        raise ValueError("connections.json missing 'DI-Key'.")
    storage_account_url = (data.get("SA-endpoint") or data.get("SA-endpoint"))
    if not storage_account_url:
        raise ValueError("connections.json missing 'SA-endpoint'.")

    return ConnectionsConfig(
        di_endpoint=di_endpoint.rstrip("/"),
        di_key=di_key,
        storage_account_url=storage_account_url.rstrip("/"),
    )

if __name__ == "__main__": 
    cfg = load_connections_config()
    match mode:
        # Check if running local file OCR or cloud (azure) file OCR
        case "local":
            print("local")
            analyze_local_documents()
        case "azure":
            print("azure")
            analyze_azure_documents()   
