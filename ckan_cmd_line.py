
### to run this paste the following in command line and run

### python ckan_cmd_line.py http://saturn04.ctg.albany.edu fadd2ba4-9d6a-40d6-98ba-9a984bdc1625 /Users/suniljoshi/wq-exploratory-code/sjoshi/CTG/CKAN/GlobalImportFormat1.csv

### import required modules
import ckanapi
from ckanapi import RemoteCKAN
import unidecode
import re
import pprint
import pandas as pd
import numpy as np
import csv
import json
import argparse , ckanapi



### add the command line arguments using arg-parser module
parser = argparse.ArgumentParser(description = 'uploads file to ckan')
parser.add_argument('API_HOST' , help = 'ckan_url / ex : http://saturn04.ctg.albany.edu')
parser.add_argument('API_KEY' , help = 'ckan_api_key / ex : fadd2ba4-9d6a-40d6-98ba-9a984bdc1625 ')
parser.add_argument('CATALOG_CSV', help = 'file to upload to ckan ex: bulk import csv file from local system')
args = parser.parse_args()


### function to upload csvfile which has both resource and meta data file , api_key and api_host

def bulkupload(CATALOG_CSV , API_HOST, API_KEY):
		# Create DataSet and associated resources. Metadata values are retrieved from csv file, 
	# resource values are retrieved from CSV data file.
	# found similar example: https://github.com/riordan/ckan-import-via-csv/blob/master/CKAN_bulk_load_datasets_and_resources_via_csv.ipynb
	#! pip3 install ckanapi
	
	# Ref: http://giv-oct.uni-muenster.de/dev-corner/data/storage/datastore-create/
	#Ref2 https://github.com/ckan/example-update-datastore/blob/master/datastore_example.py
	# REF : 3  https://github.com/ckan/example-earthquake-datastore
	#API_HOST = 'http://saturn04.ctg.albany.edu'
	#API_KEY = 'fadd2ba4-9d6a-40d6-98ba-9a984bdc1625'
	ua = 'ckanapi/1.0 (+http://example.com/my/website)'
	#CATALOG_CSV = '/Users/suniljoshi/wq-exploratory-code/sjoshi/CTG/CKAN/GlobalImportFormat1.csv'

	#CKAN_URL = 'http://saturn04.ctg.albany.edu/organization'

	## creating a connection to CKAN

	ckan = RemoteCKAN(address=API_HOST, apikey=API_KEY)


	print(ckan)





	### read bulk csv file from local system 
	catalog = pd.read_csv(CATALOG_CSV, skip_blank_lines=True, skiprows=0) # Template CSV, don't Skips first header row.
	catalog = catalog.replace(np.nan, '', regex=True) #Blank text is treated as a Numpy Not a Number (NaN). Here we replace that with blank text again.
	print(catalog)



	        
	### loop through bulk file columns and reading the template to create a new resource
	for ix, record in catalog.iterrows():
	   
	    resource_format = 'CSV'
	    dataset_title = record['dataset_title']
	    simple_dataset_name = record['simple_dataset_name'] #"lmas"
	    simple_resource_name= record['simple_resource_name']
	    simple_resource_description = record['simple_resource_description']
	    simple_resource_format = record['simple_resource_format'] #'CSV'
	    dataset_description = record['dataset_description'] #  'Lake Monitoring and Assessment Section of DEC'
	    owner_org = 'ctgteam'
	    metadatatoload = record['metadata_file']# 'C:/temp/testresults_metadata.csv'
	    #print(metadatatoload)
	    resource_path = record['data_file']# 'C:/temp/testresults_data.csv'
	    #print(resource_path)
	    #ckan = ckanapi.RemoteCKAN(remote, apikey)
	    
	   
	    ckan = ckanapi.RemoteCKAN(API_HOST, apikey=API_KEY, user_agent=ua)
	    #ckan.logic.validators.package_name_exists(value, context)
	    

	    
	    
	        # List the ID and names of resources in this dataset 

	    ## new data set details
	    new_dataset_response = ckan.action.package_show(id=simple_dataset_name)
	    #print(new_dataset_response)

	    print("List out all the  resources in the {datasetname} dataset".format(datasetname=simple_dataset_name ))
	    # Find the resource id for named resource;
	    for key in new_dataset_response['resources']:
	        print(" Resource Name:{0}".format(key['name']))
	        print(" Resouce ID:{0}".format(key['id']))
	        



	    #print("Dataset Package ID:",response['id']) 
	    print("Dataset Package ID:",new_dataset_response['id']) 
	    #print(response['resources']['name']=lake_location )
	    
	    
	        # Create Resource in a dataset, resource holds CSV files etc..
	    print ('Creating resource')
	    

	    # response contains the package_id for the dataset
	    #{'license_title': None, 'maintainer': None, 'relationships_as_object': [], 'private': False, 'maintainer_email': None, 'num_tags': 0, 'id': 'b551763d-06d3-43bf-8a04-7bcf8d477b92',
	    # https://docs.ckan.org/en/2.8/api/index.html#ckan.logic.action.create.resource_create
	    # sample: resource = {"package_id": package['id'],"format": "CSV", "name": "NameExample"}
	    resource_definition = {"package_id": new_dataset_response['id'], "format": resource_format, "name": simple_resource_name }
	    print(resource_definition)


	    

	    ### uploading metadata by creating data template
	    fields= []
	    with open(metadatatoload) as csvfile2:
	        r = csv.DictReader(csvfile2, skipinitialspace=True)
	        #next(r, none)  #skip header
	        for d in r:
	            #print(d)
	            #test_fields = [dict(d) for d in r]
	            #print(d['Field Name']) #Field Name
	            ## CKAN doesn't have an alphanumeric type##

	            ### checking for text and alphanumeric dara in metadata
	            if "alphanumeric" in d['Field Name']:
	                d['Field Name'] = "text"
	            if "alphanumeric" in d['Field Type']:
	                d['Field Type']="text"
	            elif "numeric" in d['Field Type']:
	                if "text" in d['Field Type']:
	                    d['Field Type']="text" 


	            fields.append({"id": d['Field Name'],
	                               "type": "text",
	                               "description" : d['Field Description'],
	                               "info":{
	                                   "label": d['Field Name'],
	                                   "type_override": "text",
	                                   "units": d['Units'],
	                                   "notes": d['Notes']
	                               }
	                              })


	    print(" Metadata fields data structure ready")
	    #print(fields)




	    
	    
	    
	    # Find the resource id for named resource; 
	    # Loop through since a dataset can have multiple resources
	    for key in new_dataset_response['resources']:
	        print(" ", key['name'])
	        print(" ", key['id'])
	        if key['name'] == simple_resource_name:
	            resource_id=key['id']

	    # Create the New Resource in the Dataset, and upload the CSV file
	    # Other file types could be used
	    
	    ### creating new resource , this is where actual upload happens
	    resource_data = ckan.action.resource_create(package_id=new_dataset_response['id'],url='Empty',  # ignored but required by CKAN<2.6
			                                            description = simple_resource_description,name=simple_resource_name,
			                                            format=simple_resource_format,upload=open(resource_path, 'rb'))
	    print("resource id of new data resource",resource_data['id'])
	    data = ckan.action.datastore_create(resource_id=resource_data['id'],
			                                        fields=fields,
			                                        force=True)
	    	
	    
   
	    

### upload csv , api key , host url through command line
if __name__ == '__main__':
	print(bulkupload(args.CATALOG_CSV, args.API_HOST, args.API_KEY))



