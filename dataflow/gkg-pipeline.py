# %%
import apache_beam as beam
from datetime import datetime,timedelta
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions,StandardOptions
import requests
import zipfile
import io
import os
from google.cloud import storage
from dotenv import load_dotenv
from common.load_to_bq import load_csv_to_bigquery,schema
load_dotenv()


# %%
class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--start_date',
                                            default=(datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d"),
                                            type=str)
        parser.add_value_provider_argument('--end_date',
                                            default=(datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d"),
                                            type=str)


# %%
if hasattr(__builtins__, '__IPYTHON__'):
    import apache_beam.runners.interactive.interactive_beam as ib
    from apache_beam.runners.interactive.interactive_runner import InteractiveRunner

    ib.todict = lambda pcoll: ib.collect(pcoll).to_dict(orient="records")
    pipeline_options = PipelineOptions()
    user_options = pipeline_options.view_as(UserOptions)
    pipeline_options.view_as(GoogleCloudOptions).temp_location = 'gs://factored-hackaton-2024/tmp'
    pipeline_options.view_as(GoogleCloudOptions).project = 'factored-hackaton-2024'
    p = beam.Pipeline(InteractiveRunner(), options=pipeline_options)
    print("INTERACTIVE")
else:
    print("RUNNING OPTIONS")
    pipeline_options = PipelineOptions()
    p = beam.Pipeline(options=pipeline_options)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    user_options = pipeline_options.view_as(UserOptions)


# %%
def expand(data,start_date,end_date):
    from datetime import datetime,timedelta
    start_date_dt = datetime.strptime(start_date.get(), "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date.get(), "%Y-%m-%d")
    
    range_date_generated = [{"filter_date": (start_date_dt + timedelta(days=x)).strftime("%Y-%m-%d")} for x in range(0, (end_date_dt-start_date_dt).days+1)]
    return range_date_generated


# %%
if hasattr(__builtins__, '__IPYTHON__') and False:
    dates = p | 'Create' >> beam.Create([{"filter_date": "2024-08-21"}]) 
else:
    initial = p | 'Create' >> beam.Create([1]) 
    dates = (initial | "create requests" >> beam.FlatMap(expand,user_options.start_date,
                                    user_options.end_date)
            )


# %%
def download_and_upload_gdelt_gkg(json_data):
    project_id = "factored-hackaton-2024"
    bucket_name = project_id
    filter_date = json_data["filter_date"]
    # Convert filter_date to required format
    date_obj = datetime.strptime(filter_date, '%Y-%m-%d')
    formatted_date = date_obj.strftime('%Y%m%d')
    
    # Construct the URL
    url = f"http://data.gdeltproject.org/gkg/{formatted_date}.gkg.csv.zip"
    print(f"{filter_date} will be uploaded")
    try:
        # Download the file
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        
        # Unzip the content
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            csv_filename = zip_ref.namelist()[0]  # Assume there's only one file in the zip
            csv_content = zip_ref.read(csv_filename)
        
        lines = csv_content.splitlines()
        first_column = ["FILEDATE".encode('ascii')] + [filter_date.encode('ascii')] * (len(lines) - 1)
        zipped_list = list(zip(first_column,lines))
        result = [b'\t'.join(item) for item in zipped_list]
        csv_content = b'\n'.join(result)

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"gdelt_gkg/{csv_filename}")
        blob.upload_from_string(csv_content)
        gcs_uri = f"gs://{bucket_name}/gdelt_gkg/{csv_filename}"
        table_id = f"gdelt.gkg_raw${formatted_date}"
        output = load_csv_to_bigquery(gcs_uri, table_id, project_id=project_id,schema=schema)
        print(f"File {csv_filename} successfully uploaded to {bucket_name}")
        
        return output
    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")
    except zipfile.BadZipFile:
        print("Error: The downloaded file is not a valid zip file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# %%
files = dates | beam.Map(download_and_upload_gdelt_gkg)

# %%
ib.show(files)

# %%

# %%
df = ib.collect(files)

# %%
http://data.gdeltproject.org/gkg/20240822.gkgcounts.csv.zip

# %%
csv_content = df.iloc[0][0]

# %%
lines = csv_content.splitlines()

# %%
lines[0]

# %%
for i,line in enumerator(lines):
    if i ==0:
        
    

# %%
second_list = ["FILEDATE".encode('ascii')] + ["20240801".encode('ascii')] * (len(lines) - 1)
zipped_list = list(zip(second_list,lines))
result = [b'\t'.join(item) for item in zipped_list]
b'\n'.join(result)

# %%
second_list

# %%
zipped_list

# %%
import chardet

def detect_encoding(byte_string):
    result = chardet.detect(byte_string)
    return result['encoding']
detect_encoding(csv_content)

# %%
