from simple_salesforce import Salesforce
from pandas.io.json import json_normalize
from cryptography.fernet import Fernet
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import time
import csv
import re
import warnings 
warnings.filterwarnings("ignore")
csv.field_size_limit(10000000)

class SalesforceOperator(Salesforce):
    '''
    SalesforceOperator:
        Allows for clean interaction with SFDC via standard REST/SOAP and Bulk API 2.0 
        Functions as a Bulk job creator / harvester / gatherer 
    
        Class is derived from the simple-salesforce connection engine:
            https://pypi.org/project/simple-salesforce/
    
    Standard inherited methods include:
        - query()
        - query_all()
        - query_more()
        - (SFDC-object).get('id')
        
        e.g. SalesforceOperator.query(***INSERT YOUR QUERY HERE***)

    Inherits from the Salesforce object within the simple_salesforce library
        Documentation can be viewed here: 
            https://github.com/simple-salesforce/simple-salesforce/blob/master/simple_salesforce/api.py


    Some Parameters
    ---------------
    (SSO login requires use of session_id from previously authenticated connection)
    session_id : str, required
        The sessionId parameter for a successful SSO 
        authenticated / instance of Salesforce
    instance_url : str, required
        The url instance of Salesforce e.g. domain
    version : str, required
        The version of the Salesforce API 
    sandbox : boolean, default False

    (Non-SSO login requires individual Salesforce credentials shown below)
    username : str, required
        Username/email for SFDC, e.g. 'phil.gilbert@realpage.com'
    password : str, required
        Individual password credentials for SFDC 
    security_token : str, required
        Security token can be reset for an individual user
        under '/Settings/My Personal Information' in Salesforce.com
    instance_url :      
        The url used to login to Salesforce.com, e.g. 'https://na100.salesforce.com/'  

    '''

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

                
    def create_bulk_job(self, soql_query, **kwargs):
        '''
        
        Makes a standard query job against the Bulk API 2.0
        
         -Returns a Python Dictionary with API call details
        
        Documentation:
            https://developer.salesforce.com/docs/atlas.en-us.api_bulk_v2.meta/api_bulk_v2/queries.htm
        
        '''
        url = self.base_url + 'jobs/query'
        method = 'POST'
        
        data = {
            "operation": "query"
        }
        
        data['query'] = soql_query

        headers = self.headers.copy()
        additional_headers = kwargs.pop('headers', dict())
        headers.update(additional_headers)

        request = self.session.request(method, url, headers=headers, json=data, **kwargs)

        try:
            return request.json()

        except:
            pass
        
        
    def check_bulk_job(self, job_id, **kwargs):
        '''
        
        Checks a Bulk API job until it's completed.
        The current state of processing for the job. Possible values are:

            UploadComplete
                —All job data has been uploaded and the job is ready to be processed. Salesforce has put the job in the queue.
            InProgress
                —Salesforce is processing the job.
            Aborted
                —The job has been aborted. See Abort a Query Job.
            JobComplete
                —Salesforce has finished processing the job.
            Failed
                —The job failed.
                
        returns Python dictionary with status 

        '''
        url = self.base_url + 'jobs/query/' + job_id
        method = 'GET'
        
        headers = self.headers.copy()
        additional_headers = kwargs.pop('headers', dict())
        headers.update(additional_headers)
        
        request = self.session.request(method, url, headers=headers, **kwargs)
        state = request.json()['state']

        return request#.json()
    
    def harvest_bulk_job(self, job_id, filename, **kwargs):
        """
        Retrieves the Bulk API job from SFDC.
        
        Parameters: 
        -------------------------
        job_id : str, required 
            The unique Salesforce Id associated with Bulk API job
        filename: str, required
            The filepath for the destination file where the CSV export 
            will be loaded
    
        Returns a Python dictionary of relevant details
    
        """
        
        start_time = datetime.now()
        
        url = self.base_url + 'jobs/query/' + job_id + '/results'
        method = 'GET'

        headers = self.headers.copy()
        additional_headers = kwargs.pop('headers', dict())
        headers.update(additional_headers)
        
        request = self.session.request(method, url, headers=headers, **kwargs)
        
        with open(filename, 'w') as f:
            
            # Find the line break called 'x' and the string called 'y'
            x = request.text.find('\n')
            y = request.text[:x]
            y = y.lower().replace('__c','').replace('__r','')
            y = re.sub('^_c$','', y)
            y = y.replace('.','_')
            
            f.write(y)
            f.write(request.text[x:])
        
        _job_dict = self.check_bulk_job(job_id)

        total_records = _job_dict['numberRecordsProcessed']

        processed_records = int(request.headers['Sforce-NumberOfRecords'])
        
        #print(str(processed_records) + ' out of ' + str(total_records))
        
        while int(processed_records) < int(total_records):

            locator = request.headers['Sforce-Locator']
            
            if processed_records + 100000 > int(total_records):
                pull_size = str(int(total_records) - processed_records)
            
            else:
                pull_size = '100000'
            
            new_url = url + '?locator=' + locator + '&maxRecords=' + pull_size

            request = self.session.request(method, new_url, headers=headers, **kwargs)

            with open(file_name, 'a') as f:
                x = request.text.find("\n")
                f.write(request.text[x + 1:])

            processed_records += int(pull_size)
#             print('\t' + str(processed_records) + ' out of ' + str(total_records) + ' -- ' + 
#                   str(int(processed_records / total_records * 100)))
        
        end_time = datetime.now()
        _duration = end_time - start_time
        seconds = _duration.total_seconds()
        
        return_dict = {}
        return_dict['job_id'] = job_id
        return_dict['sfdc_object'] = _job_dict['object']
        return_dict['job_creator_id'] = _job_dict['createdById']
        return_dict['filename'] = filename
        return_dict['processed_records'] = processed_records
        return_dict['total_records'] = total_records
        return_dict['start_time'] = start_time.strftime('%Y-%m-%d %X')
        return_dict['end_time'] = end_time.strftime('%Y-%m-%d %X')
        return_dict['seconds'] = seconds
        
        #print("Complete!")
        
        return return_dict



