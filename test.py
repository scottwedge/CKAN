
### to run this paste the following in command line

### python test.py http://saturn04.ctg.albany.edu fadd2ba4-9d6a-40d6-98ba-9a984bdc1625 /Users/suniljoshi/Desktop/Desk/CTG/LMAS/GlobalImportFormat.csv



import ckanapi
from ckanapi import RemoteCKAN
import unidecode
import re

from collections import OrderedDict


import pprint
import pandas as pd

import numpy as np
import csv
import json
import argparse , ckanapi


parser = argparse.ArgumentParser(description = 'uploads file to ckan')
parser.add_argument('API_HOST' , help = 'ckan_url / ex : http://saturn04.ctg.albany.edu')
parser.add_argument('API_KEY' , help = 'ckan_api_key / ex : fadd2ba4-9d6a-40d6-98ba-9a984bdc1625 ')
parser.add_argument('CATALOG_CSV', help = 'file to upload to ckan ex: bulk import csv file from local system')
args = parser.parse_args()



def bulkupload(CATALOG_CSV , API_HOST, API_KEY):
		# Create DataSet and associated resources. Metadata values are retrieved from csv file, 
	# resource values are retrieved from CSV data file.
	# found similar example: https://github.com/riordan/ckan-import-via-csv/blob/master/CKAN_bulk_load_datasets_and_resources_via_csv.ipynb
	#! pip3 install ckanapi
	
	# Ref: http://giv-oct.uni-muenster.de/dev-corner/data/storage/datastore-create/
	#Ref2 https://github.com/ckan/example-update-datastore/blob/master/datastore_example.py
	# REF : 3  https://github.com/ckan/example-earthquake-datastore
	API_HOST = 'http://saturn04.ctg.albany.edu'
	API_KEY = 'fadd2ba4-9d6a-40d6-98ba-9a984bdc1625'
	ua = 'ckanapi/1.0 (+http://example.com/my/website)'
	CATALOG_CSV = '/Users/suniljoshi/Desktop/Desk/CTG/LMAS/GlobalImportFormat.csv'

	#CKAN_URL = 'http://saturn04.ctg.albany.edu/organization'

	ckan = RemoteCKAN(address=API_HOST, apikey=API_KEY)


	print(ckan)



	   
	# orgs = ckan.action.organization_list(all_fields=True, limit=10000)
	# orgs_nameindex = {}
	# for org in orgs:
	#     orgs_nameindex[org['display_name']] = org
	#     #print(org)
	# def reindex_orgs():
	#     global orgs
	#     global orgs_nameindex
	#     orgs = ckan.action.organization_list(all_fields=True, limit=10000)
	#     for org in orgs:
	#         orgs_nameindex[org['display_name']] = org
	#         print(org)
	        
	# reindex_orgs()        


	catalog = pd.read_csv(CATALOG_CSV, skip_blank_lines=True, skiprows=0) # Template CSV, don't Skips first header row.
	catalog = catalog.replace(np.nan, '', regex=True) #Blank text is treated as a Numpy Not a Number (NaN). Here we replace that with blank text again.
	print(catalog)



	        
	# for ix, row in catalog.iterrows():
	#     try:    
	#         if row['dataset_title'] not in groups_nameindex.keys():
	#             groupname = unidecode.unidecode(row['dataset_title']).lower().replace(' ','_')
	#             groupname = re.sub('[()\.,!@#$]', '', groupname)
	#             print('Missing group:', row['dataset_title'])
	#             print('creating group...')
	#             print(groupname, row['dataset_title'])
	#             ckan.action.group_create(name=groupname, title=row['dataset_title'])
	#             reindex_groups()
	#         else:
	#             pass
	#     except ValueError:
	#         print(row)
	#         raise ValueError



	# for ix, row in catalog.iterrows():
	#     if row['simple_dataset_name'] not in orgs_nameindex.keys():
	#         orgname = unidecode.unidecode(row['simple_dataset_name']).lower().replace(' ','_')
	#         orgname = re.sub('[()\.,!@#$]', '', orgname)
	#         print('Missing organization', row['simple_dataset_name'], orgname)
	#         print('Creating Organization...')
	#         ckan.action.organization_create(name=orgname, title = row['simple_dataset_name'])
	#         reindex_orgs()

	        
	def get_orgID(record):
	    return orgs_nameindex[record['simple_dataset_name']]['id']


	def get_orgSlug(record):
	    return orgs_nameindex[record['simple_dataset_name']]['name']




	#print(catalog)
	#fucntion to create a new dataset

	def create_Dataset(record,name):
	    ## create a dictonary for new dataset with its key value pairs
	    dataset = {}

	    dataset['isopen'] = True
	    dataset['private'] = False
	    dataset['state'] = 'active'
	    dataset['type'] = 'dataset'
	    dataset['resources'] = []
	    dataset['name'] = name
	    #dataset['tags'] = []
	    dataset['title'] = record['simple_dataset_name']

	    return dataset


	all_Datasets = OrderedDict()

	jsonl = ""
	i = 0
	cat_len = len(catalog)

	for ix, record in catalog.iterrows():
	#     print("Resource Description",record['simple_resource_description'])
	#     print("dataset_title",record['dataset_title'])
	#     print("simple_dataset_name",record['simple_dataset_name'])
	#     print("simple_resource_name",record['simple_resource_name'])
	#     print("simple_resource_format",record['simple_resource_format'])
	#     print("dataset_description",record['dataset_description'])
	#     print("metadata_file",record['metadata_file'])
	#     print("data_file",record['data_file'])
	    try:
	        name = unidecode.unidecode(record['simple_dataset_name']).lower().replace(' ','_')
	        name = re.sub('[()\.,!@#$]', '', name)
	        #name = get_orgSlug(record)+'--'+name
	    except Exception as e:
	        #print("skipping record", record, i)
	        pass

	    # Test to see if we've seen this dataset before, and if not create it

	    if not name in all_Datasets:
	        all_Datasets[name] = create_Dataset(record, name)
	    currentDataset = all_Datasets[name]
	    
	    
	    ### add new resource 
	    
	    
	    resource = {}
	    resource['name'] = unidecode.unidecode(record['simple_resource_name'])
	    resource['position'] = len(currentDataset['resources'])
	    resource['description'] = record['simple_resource_description']
	    resource['state'] = 'active'
	    resource['url'] = record['data_file']

	    if resource is not None:
	    	print('resource already exists')
	    else:
	    	currentDataset['resources'].append(resource)

    
	    # for d in all_Datasets:
	    #     currentDataset = all_Datasets[d]
	    #     # Add counts for resources 
	    #     currentDataset['num_resources'] = len(currentDataset['resources'])
	    #     jsonl += json.dumps(currentDataset)+'\n'

	    

	    
	    
	    	    	
	    
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
	    new_dataset_response = ckan.action.package_show(id=simple_dataset_name)

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
	    ##resource = ckan.action.resource_create(resource=resource_definition)
	    #print(resource)
	    
	    
	    
	    
	    
	        # Prepare CSV file and metadata file for uploading to the new resouce
	    #records need to be list of Dictionaries
	    #records = []

	    # LakeID,LocationID,LocationName,County,Y_Coordinate,X_Coordinate,Type,DEC Region,LocationType,Horz_Method,Horz_Datum,Report_LocationName
	    #from csv import DictReader
	    #with open(datatoload) as csvfile1:
	    #    r = DictReader(csvfile1, skipinitialspace=True)
	    #    records_dict = [dict(d) for d in r]   


	    #reader = csv.DictReader(open(datatoload))
	    #for line in reader:
	    #    #print(line)
	    #    records.append(dict(line))



	    #print(type(records))


	    ###########################################################################
	    # Parse metadate file and create the data structure so that it can
	    # be used to set the datatype for the data resource 
	    #    NOTES: fields need to be list of Dictionaries
	    #           Records listed need to be in the same order they are in the data file?
	    ###########################################################################
	    fields= []
	    with open(metadatatoload) as csvfile2:
	        r = csv.DictReader(csvfile2, skipinitialspace=True)
	        #next(r, none)  #skip header
	        for d in r:
	            #print(d)
	            #test_fields = [dict(d) for d in r]
	            #print(d['Field Name']) #Field Name
	            ## CKAN doesn't have an alphanumeric type##
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
	    # Loop through since a dataset can have multiple resouces
	    for key in new_dataset_response['resources']:
	        print(" ", key['name'])
	        print(" ", key['id'])
	        if key['name'] == simple_resource_name:
	            resource_id=key['id']

	    # Create the New Resource in the Dataset, and upload the CSV file
	    # Other file types could be used
	    
	    resource_data = ckan.action.resource_create(package_id=new_dataset_response['id'],url='Empty',  # ignored but required by CKAN<2.6
	                                                    description = simple_resource_description,name=simple_resource_name,
	                                                    format=simple_resource_format,upload=open(resource_path, 'rb'))



	    print("resource id of new data resource",resource_data['id'])

	    data = ckan.action.datastore_create(resource_id=resource_data['id'],
	                                                fields=fields,
	                                                force=True)
	    #print(data['success'])
	    #print(data['error'])
	    #created_package = data['result']
	    #pprint.pprint(created_package)
	 
	    


if __name__ == '__main__':
	print(bulkupload(args.CATALOG_CSV, args.API_HOST, args.API_KEY))



























































































































































































































































































	                
	      
	        
	  