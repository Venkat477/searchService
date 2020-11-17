"""
Created on Mon Nov 16 08:15:31 2020
@author: Venkata N Divi
"""

import json,sys
from flask import Flask,request
from config import ELASTICSEARCH_URL,ELASTICSEARCH_INDEX
from elasticsearch import Elasticsearch

app = Flask(__name__)
indexName = ELASTICSEARCH_INDEX
headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
es = Elasticsearch([ELASTICSEARCH_URL])             # Creating Elastic Search Object 

@app.route('/searchData',methods=['POST'])
def get_record():
    # This method will be used to query the Elastic Search Index and return the matching documents to the user
    #
    # This will return a JSON response, which contains number of matching records found and the records.
    
    try:
        if request.json:
            if 'keyword' in request.json:
                result,searchRes,searchTerm = {},[],request.json['keyword']
                res = es.search(index = indexName, body={"query": {"query_string": {"query":searchTerm,"fields":["keywords"]}}})
                if res:
                    result['No. of Matches'] = res['hits']['total']
                    for hit in res['hits']['hits']:
                        if len(searchRes) <= 15: searchRes.append(hit["_source"])
                        else: break
                    result['Matches'] = searchRes
                    return { 'statusCode': 200, 'body': json.loads(json.dumps(result)) }
                else:
                    return { 'statusCode': 500, 'body': 'Unable to Query ES' }
            else:
                return { 'statusCode': 500, 'body': 'Enter proper search Keyword' }
        else:
            return { 'statusCode': 500, 'body': 'Send the request in Proper JSON Format.' }
    except Exception as e:
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno),Exception, e)
        return { 'statusCode': 500, 'body': 'Unable to process Request' }

if __name__=='__main__':
    app.run(debug=True,host='localhost',port='7006')