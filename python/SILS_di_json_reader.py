# import libraries 
from datetime import datetime
import os
import json
import time
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
from azure.core.credentials import AzureKeyCredential

from azure.storage.blob import BlobServiceClient
from itertools import tee

####### - SETUP CONNECTIONS - #######

current_directory = os.getcwd()
connection_file = os.path.join(current_directory, "connections.json")
connection_details = {}
with open(connection_file, "r+") as connection_data:
    connection_details = json.load(connection_data)

# set `<your-endpoint>` and `<your-key>` variables with the values from the Document Intelligence  
endpoint = connection_details['DI-Endpoint'] 
key = connection_details['DI-Key']
 
# Options for mode: local, azure
# local: reads all files in directory specified by "directory_path"
# azure: reads all files in blob container in Azure storage account specified by "connection_string" and "container_name"
mode = "azure"
 
# If using a local directory, set directory here. Make sure to use forward slashes (/) and not backslashs (\)
directory_path = "./tempdata"
 
# Azure blob container setttings, set `<connection_string>` and `<container_name>` for blob container in storage account
connection_string = connection_details['SA-enpoint']
container_name = "test-upload"

# Azure blob output container setttings, set `<connection_string>` and `<container_name>` for blob container in storage account you want to write a JSON output too
# Script will only attempt to upload to a blob container if output_azure is set to True
output_azure = False
output_connection_string = connection_string
output_container_name = "/raw_jsons" 

# Determine model to be used here. Check with Document Intelligence Studio for the name of the model used:
extraction_model_id = ""

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

def content_field(value):
    # print(field, value['content'])
    result_content = [value['content'], value['confidence']]
    return result_content

def extract_object_array(field, dict_data):
    result_list = []
    for data in dict_data:
        holder = {field: data}
        holder_dict = extract_object(holder)
        for holder_field, holder_value in holder_dict.items():
            result_list.append(holder_value)
    return result_list

def extract_object(dict_data):
    result_dict_data = {}
    for field, attributes in dict_data.items():
        dict_data_type = attributes['type']
        attribute_check_list = attributes.keys()

        if dict_data_type == 'object':
            if 'content' in attribute_check_list or 'valueObject' in attribute_check_list:
                inner_object = attributes['valueObject']
                result_dict_data[field] = extract_object(inner_object)

        elif dict_data_type == 'array':
            # print(field)
            # print(attributes.keys(), flush = True)
            if 'content' in attribute_check_list or 'valueArray' in attribute_check_list:
                inner_object = attributes['valueArray']
                result_dict_data[field] = extract_object_array(field, inner_object)

        else:
            if 'content' in attribute_check_list:
            # print("Attribute pass")
                result_dict_data[field] = content_field(attributes)

    return result_dict_data

# This is how function print_analyzed_contents(DocumentAnalysisClient() result, string file_name) print_analyzed_contents takes variable result - class DocumentAnalysisClient(). Function access attribute `documents` for data. Refer to DI documenntation for detail.
def print_analyzed_contents(result, file_name):
    doc_data = result['documents'][0]['fields']
    analyzed_data = extract_object(doc_data)
    # Initiate connection to blob storage - if `output_azure` is set to true - and retrieve the output container connection details
    container_client = None
    if output_azure:
        blob_service_client = BlobServiceClient.from_connection_string(output_connection_string)
        container_client = blob_service_client.get_container_client(output_container_name)
    
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
    if output_azure:
        with open(output_file, "rb") as data:
            container_client.upload_blob(name=file_name, data=data, overwrite=True)
        # print(f"Analysis data uploaded to Azure Blob: {file_name}")


# function analyzed_local_documents()
# function reads local files from a directory, connect to a DI model from DI resource in Azure cloud and runs print_analyzed_contents() on them.
def analyze_local_documents():
    # Connect to DI resource on Azure cloud. Also verifies permission here.
    document_analysis_client = DocumentIntelligenceClient(endpoint=endpoint, credential=AzureKeyCredential(key))

    # Going through files the directory_path from above
    for filename in os.listdir(directory_path):
        # Only check for the following file format. These are also only the current formats supported. Check DI blogpost for update when run.
        if filename.endswith((".pdf", ".jpg", ".png", ".bmp", ".tiff")):  
            file_path = os.path.join(directory_path, filename)

            with open(file_path, "rb") as file:
                # Change the model name to fit with the corresponding model. Default is: prebuilt-document
                # currently only read pdf files
                if filename.endswith(".pdf"): 
                    print("extract pdf")
                    poller = document_analysis_client.begin_analyze_document(extraction_model_id, file)
                # If not, then treat the rest as an image and proceed as such
                else:  
                    poller = document_analysis_client.begin_analyze_document("prebuilt-document", file, content_type="image/jpeg")
                # At this stage, OCR is completed, and current data is available with the attribute `result` of the variable above `poller`
                result = poller.result()
                # Defining an appropriate file_name to be produced.
                file_name = "local " + filename.replace(' ', '') + " " + "analyzed_data.json"
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
    document_analysis_client = DocumentIntelligenceClient(endpoint=endpoint, credential=AzureKeyCredential(key))

    # Connect to blob resource on Azure cloud. Connecting to the resource by connection_string, and to the exact container via container_name
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
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
 
if __name__ == "__main__": 
    match mode:
        # Check if running local file OCR or cloud (azure) file OCR
        case "local":
            print("local")
            analyze_local_documents()
        case "azure":
            print("azure")
            analyze_azure_documents()   
