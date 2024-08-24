FROM apache/beam_python3.12_sdk:2.58.1

# add requirements.txt file
COPY requirements.txt .
# Pre-built python dependencies  
RUN pip install -r requirements.txt

COPY common ./common
# Set the entrypoint to the Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]