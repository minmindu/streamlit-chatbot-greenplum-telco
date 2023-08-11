from minio import Minio
from io import BytesIO
import PyPDF2

# from ingestion import convertpdf
# import covertpdf

minio_host_port="35.189.1.42:9000"
minio_access_key="minioadmin"
minio_secret_key="minioadmin"
minio_bucket_name="telco-bills"
# file_prefix="invoice"
file_prefix="invoice11013202208"

## Note: secure=False is required for HTTP, not HTTPS
client = Minio(minio_host_port, 
               secure=False, 
               access_key=minio_access_key,
               secret_key=minio_secret_key)

print("\n# Checking if " + minio_bucket_name + " exists\n")
if not client.bucket_exists(minio_bucket_name):
    print("# bucket does not exist, creating new bucket\n")
    client.make_bucket(minio_bucket_name, location="")
else:
    print("# bucket exists\n")

# List all object paths in bucket that begin with prefixname.
objects = client.list_objects(minio_bucket_name, prefix=file_prefix,
                              recursive=True)
for obj in objects:
    print(obj.bucket_name, obj.object_name.encode('utf-8'), obj.last_modified,
          obj.etag, obj.size, obj.content_type)
    
    data = client.get_object(minio_bucket_name, obj.object_name)
    print(data)

    data2 = client.fget_object(minio_bucket_name, obj.object_name, obj.object_name)
    print(data2)
    # print(obj) ## this is the object itself. not the urllib3.response.HTTPResponse 
    # pdfReader = PyPDF2.PdfReader(data)
    # number_of_pages = len(pdfReader.pages)
    content =  BytesIO(data.read())
    # convertpdf.convert_one_pdf(content)
    
    print(content)

    ## covert the pdf file to text
    # file1 = open(r'../../data/Mobile Bill_MinminDu_202307.pdf', mode='rb')
    # covertpdf.convert_one_pdf(file1)

