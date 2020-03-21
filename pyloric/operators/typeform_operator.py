from typeform import Typeform

class TypeformOperator(object):
    """
    Simple operator for interaction Typeform API
    
    """
    
    def __init__(self, api_key='',*args, **kwargs):
        self.api_key = api_key
        self.typeform = Typeform(api_key)
        
        self.forms_count = len(self.get_forms_dict()['items'])
        
        self.forms_name_list = self.get_forms_list(by_name=True)
        self.forms_id_list = self.get_forms_list(by_name=False)

    def get_forms_dict(self):
        """
        Returns a Python dictionary of Typeform Forms 
        
        Warning - might fail if response is more than 1 page
        
        """
        
        _typeform = self.typeform
        forms: dict = _typeform.forms.list()
        return forms
    
    def get_forms_list(self, by_name=False):
        """
        Produces a list of all forms by either 'title' or by 'id'
        
        """
        if by_name == False:
            ref = 'id'
        else:
            ref = 'title'
            
        _typeform = self.typeform
        forms_list = [self.get_forms_dict()['items'][form_number][ref] for form_number in range(0, self.forms_count)]
        return forms_list
    
    def get_form_response(self, form_id):
        """
        Returns a Python dictionary of Typeform response for a given form_id
        
        Warning: May break when pages > 1
        
        """
        
        _typeform = self.typeform
        response_dict = _typeform.responses.list(form_id)

        return response_dict

    
    def get_answers_dict(self, form_id, submission_number):
        """
        Method to flatten the 'answers' for a given survey submission into a Python dictionary
        
        Warning: May break when pages > 1
        
        """
        
        _typeform = self.typeform
        _answer_list = _typeform.responses.list(form_id)['items'][submission_number]['answers']
        
        answer_dict = {}
        
        for answer_number in range(0, len(_answer_list)):
            # Uses the response 'ref' name as the dict key

            key = _answer_list[answer_number]['field']['ref']
            data_type = _answer_list[answer_number]['type']

            # "choice" is the data_type for multiple choice question 
            # if more than one selection is allowed then the response will be under 'labels' -- otherwise 'label'
            if data_type == 'choice':
                response = _answer_list[answer_number][data_type]['label']

            elif data_type == 'choices':
                try:
                    response = _answer_list[answer_number][data_type]['labels']
                except:
                    response = _answer_list[answer_number][data_type]['other']

            else:
                response = _answer_list[answer_number][data_type]

            answer_dict[key] = response

        return answer_dict
    
    def get_metadata_dict(self, form_id, submission_number):
        """
        Retrieves metadata information for a survey submission (e.g. browser details, submission_date, etc.)
        
        Note: there is also a field called metadata, but metadata should include everything
        
        """
        
        metadata_dict = self.get_form_response(form_id)['items'][submission_number]
        
        for key in ['answers','variables','calculated','hidden']:
            metadata_dict.pop(key)
        
        return metadata_dict
    
    def get_hidden_dict(self, form_id, submission_number):
        """
        Retrieves hidden field information for a survey submission

        """
        
        hidden_dict = self.get_form_response(form_id)['items'][submission_number]['hidden']
        
        return hidden_dict
    
    def get_submission_data(self, form_id, metadata=True, include_hidden=True):
        """
        Returns a list of submissions (Python dictionaries) for a given Typeform survey (form)
        
        """
        
        _typeform = self.typeform
        
        form_response = self.get_form_response(form_id)
        
        submission_data = []
        
        for submission_number in range(0, form_response['total_items']):
            
            _submission_dict = {}
            
            if metadata == True:
                _submission_dict = {**_submission_dict, **self.get_metadata_dict(form_id, submission_number)}
                
            if include_hidden == True:
                _submission_dict = {**_submission_dict, **self.get_hidden_dict(form_id, submission_number)}
                
            _submission_dict = {**_submission_dict, **self.get_answers_dict(form_id, submission_number)}
            
            submission_data.append(_submission_dict)    
         
        return submission_data