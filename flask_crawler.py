# import flast stuff
from flask import Flask
from flask import render_template
from flask import url_for
from flask import redirect
from flask import request as flask_request
from flask import Response
import json # formats python dicts and lists to json
from json.decoder import JSONDecodeError
import requests # python package that makes calling Trimble API easy
import shutil # used to download .jpg file
from file_schemas.jpg_file_schema import jpg_metadata, jpg_index_form  #for metadata extracting funciton

# flask application
app = Flask(__name__)

# Class to represent crawler
class Crawler:

    # Constructor
    # p_head_path: passed head path to the directory that the crawler will start at
    # p_id: passed id of crawler
    def __init__(self, p_head_path='DEFAULT_PATH', p_id=0):
        self.id = p_id
        self.status = 'not setup'
        self.trimble_api = 'DATAOCEAN_HOST'
        self.search_trimble_api = 'SEARCH_HOST'
        self.head_path = p_head_path
        self.current_path = p_head_path
        self.bearer_access_token = ''
        self.dirs_queue = [self.current_path]
    
    # Methods - Interact with Trimble API

    # Sets new bearer token
    def set_auth(self):
        url = "TOKEN_ENDPOINT"
        
        payload='grant_type=client_credentials'
        headers = {
          'Authorization': 'Basic APP_CREDENTIALS',
          'Content-Type': 'application/x-www-form-urlencoded'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        
        if response.status_code == 200:
            self.bearer_access_token = response.json()['access_token']
        else:
            print("Error from set_auth():",response.status_code)
    
    # returns api response of directories in current dir - in json format
    # next_page_token: see data ocean api
    def get_dirs_json(self, next_page_token=''):
        url = self.trimble_api + 'directories/directories' + '?path=' + self.current_path
        if next_page_token != '':
            url += '&next_page_take=' + next_page_token
        headers = {
          'Authorization': 'Bearer ' + self.bearer_access_token
        }

        response = requests.request("GET", url, headers=headers)
    
        if response.status_code == 200:
            return response.json()
        else:
            print("Error from get_dirs_json():",response.status_code, self.current_path)
            return {}
    
    # returns api response of files in current dir - in json format
    # next_page_token: see data ocean api
    def get_files_json(self, next_page_token=''):
        url = self.trimble_api + 'directories/files' + '?path=' + self.current_path
        if next_page_token != '':
            url += '&next_page_take=' + next_page_token
        headers = {
          'Authorization': 'Bearer ' + self.bearer_access_token
        }

        response = requests.request("GET", url, headers=headers)
    
        if response.status_code == 200:
            return response.json()
        else:
            print("Error from get_files_json():",response.status_code, self.current_path)
            return {}
            
    # get a fileset based on path - in json format 
    # path: path of a file
    # next_page_token: see data ocean api
    def get_fileset_json(self, path, next_page_token=''):
        url = self.trimble_api + 'files/manifest' + '?path=' + path
        if next_page_token != '':
            url += '&next_page_take=' + next_page_token
        headers = {
          'Authorization': 'Bearer ' + self.bearer_access_token
        }

        response = requests.request("GET", url, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            print("Error from get_fileset_json():",response.status_code,path)
            return {}
    
    # gets all dirs in current dir in a list - all pages
    def get_dirs_list(self):
        dir_list = []
        npt = ''
        # response will have a next_page_token of none if there are no more pages
        while(npt != None):
            json_res = self.get_dirs_json(next_page_token=npt)
            # if json_res is not empty add dirs onto full list
            if json_res:
                dir_list.extend(json_res['directories'])
                npt = json_res['next_page_token']
            else:
                return dir_list
        return dir_list
    
    # gets all files in current dir in a list - all pages
    def get_files_list(self):
        file_list = []
        npt = ''
        # response will have a next_page_token of none if there are no more pages
        while(npt != None):
            json_res = self.get_files_json(next_page_token=npt)
            # if json_res is not empty add files onto full list
            if json_res:
                file_list.extend(json_res['files'])
                npt = json_res['next_page_token']
            else:
                return file_list
        return file_list
    
    # gets all files in a file set in a list - all pages (really only 1 page)
    # path: path of a file - just passed on as same path to get_fileset_json()
    def get_fileset_list(self, path):
        file_list = []
        npt = ''
        c = 0 # !! file sets are large, so limit to 1 page
        # response will have a next_page_token of none if there are no more pages - c limits pages - page size limit is 1000
        while(npt != None and c < 1):
            c += 1
            json_res = self.get_fileset_json(path, next_page_token=npt)
            # if json_res is not empty add files onto full list
            if json_res:
                file_list.extend(json_res['files'])
                npt = json_res['next_page_token']
            else:
                return file_list
        return file_list
    
    # changes current directory to new one
    # path: path to try to change curent path to
    # returns weather or not the cd was succesful
    def cd_x(self, path):
        # check if dir is good
        url = self.trimble_api + 'directories' + '?path=' + path
        headers = {
          'Authorization': 'Bearer ' + self.bearer_access_token
        }

        response = requests.request("GET", url, headers=headers)
    
        if response.status_code == 200:
            self.current_path = path
            return True
        else:
            print("Error from cd_x():",response.status_code, path)
            return False

    # Changes directory and resets the queue
    def reset(self, path):
        f = self.cd_x(path)
        if(f):
            self.status = 'not started'
            self.head_path = self.current_path
            self.dirs_queue = [self.current_path]
            return True
        else:
            return False

    
    # download a file from a file set - takes a download url from a file set file and downloads it. (only working for .jpg right now)
    # download_url: fileset_file['download']['url'] - get this from a item in get_fileset_list()
    # file_type: need to know what kind of file we are looking at to handle how to download it (maybe)
    # output_name: name of the file downloaded on your computer
    def download_fileset_file(self, download_url, file_type, output_name='output'):
        headers = {
          'Authorization': 'Bearer ' + self.bearer_access_token
        }

        response = requests.request("GET", download_url, headers=headers,stream=True)
        # writes the file to your computer here
        if(file_type == '.jpg'):
            if response.status_code == 200:
                with open(output_name+file_type, 'wb') as f:
                    response.raw.decode_content = True
                    shutil.copyfileobj(response.raw, f)
                    f.close()
                return output_name
            else:
                print(response.status_code)

    #input file download url and get metadata of that file back 
    def get_metadata(self, file_name, download_url, file_type):
        file = self.download_fileset_file(download_url, file_type, file_name)
        metadata = {}
        if(file_type == '.jpg'):
            metadata = jpg_metadata(file)
            # instead of uploading to search
            f = open("search_output.txt", "a")
            f.write(json.dumps(metadata)+"\n")
            f.close()
            self.upload_to_search_index(metadata, '.jpg')
        return metadata
    
    #create template for metadata index
    def create_search_index(self, filetype, next_page_token = ''):
        url = self.search_trimble_api + 'indexes'
        if next_page_token != '':
            url += '&next_page_take=' + next_page_token
        headers = {
          'Authorization': 'Bearer ' + self.bearer_access_token,
          'Content-type':'application/json'
        }

        if(filetype == '.jpg'):
            response = requests.request("POST", url, data=json.dumps(jpg_index_form()), headers=headers)
    
        if response.status_code == 200 or response.status_code == 201:
            return response.json()
        else:
            print(response.status_code)
    
    #return index by id
    def get_index(self, id, next_page_token = ''):
        url = self.search_trimble_api + 'indexes/' + id
        if next_page_token != '':
            url += '&next_page_take=' + next_page_token
        headers = {
          'Authorization': 'Bearer ' + self.bearer_access_token
        }
        response = requests.request("GET", url, headers=headers)

        if response.status_code == 200 or response.status_code == 201:
            return response.json()
        else:
            print(response.status_code)

    #uploads json info to a search index
    def upload_to_search_index(self, metadata, filetype, next_page_token=''):
        #check if jpg index is already created
        url = self.search_trimble_api + 'indexes'
        if next_page_token != '':
            url += '&next_page_take=' + next_page_token
        headers = {
          'Authorization': 'Bearer ' + self.bearer_access_token
        }

        response = requests.request("GET", url, headers=headers)

        if response.status_code == 200 or response.status_code == 201:
            index_id = ''
            not_found = True
            query_table = ''
            if(filetype == '.jpg'):
                #loop through all indexes to find jpg index and get id
                query_table = 'jpg_file'
                for x in response.json()["indexes"]:
                    if(x["name"] == "File Metadata DEMO"):
                        index_id = x["id"]
                        not_found = False
                        break
                        
                #otherwise create a new one
                if(not_found):
                    res = self.create_search_index('.jpg')
                    #get id of index from return
                    if(res == None):
                        return None
                    not_found = False
                    index_id = res["index"]["id"]

                #wait until index is finished creating and is available if it created a new one
                while(self.get_index(index_id)["index"]["status"] != "AVAILABLE"):
                    continue

                #do upload of metadata and necessary query
                url = self.search_trimble_api + 'indexes/' + index_id + '/uploads?table_name=' + query_table + '&type=default'
                if next_page_token != '':
                    url += '&next_page_take=' + next_page_token
                headers = {
                    'Authorization': 'Bearer ' + self.bearer_access_token,
                    'Content-type':'application/json'
                }

                upload = {
                    "upload": []
                }

                upload["upload"].append(metadata)
                
                response = requests.request("POST", url, data=json.dumps(upload), headers=headers)
                if response.status_code == 200 or response.status_code == 201:
                    return response.json()
                else:
                    print(response.status_code)
        else:
            print(response.status_code)
    
    #show all search indexes and uploads of data to those indexes
    def search(self, next_page_token=''):
        url = self.search_trimble_api + 'indexes'
        if next_page_token != '':
            url += '&next_page_take=' + next_page_token
        headers = {
          'Authorization': 'Bearer ' + self.bearer_access_token
        }
        response = requests.request("GET", url, headers=headers)

        if response.status_code != 200 and response.status_code != 201:
            print("Error from get_files_json():",response.status_code, self.current_path)
            return {}
        
        index_list = []
        npt = ''
        # response will have a next_page_token of none if there are no more pages
        #while(npt != None):
        json_res = response.json()
            # if json_res is not empty add files onto full list
        #if json_res:
            #print(json_res)
        index_list.extend(json_res['indexes'])
                #npt = json_res['next_page_token']
        #else:
        return index_list
        #return index_list
    
    #get keys for index using its id
    def indexKeys(self, id, next_page_token=''):
        url = self.search_trimble_api + 'indexes/' + id
        if next_page_token != '':
            url += '&next_page_take=' + next_page_token
        headers = {
          'Authorization': 'Bearer ' + self.bearer_access_token
        }
        response = requests.request("GET", url, headers=headers)

        if response.status_code != 200 and response.status_code != 201:
            print("Error from indexKeys():",response.status_code, self.current_path)
            return {}
        
        index_keys = []
        index_keys.extend(response.json()['index']['tables']['jpg_file']['properties'])
        return index_keys

    #query values in indexes
    def queryValues(self, id, next_page_token=''):
        url = self.search_trimble_api + 'indexes/' + id + '/queries'
        if next_page_token != '':
            url += '&next_page_take=' + next_page_token
        headers = {
          'Authorization': 'Bearer ' + self.bearer_access_token,
          'Content-type':'application/json'
        }
        query = {
            "query":"select * from jpg_file;",
            "format":"JSON"
        }
        response = requests.request("POST", url, data= json.dumps(query), headers=headers)
        if response.status_code != 200 and response.status_code != 201:
            print("Error from queryValues():",response.status_code, self.current_path)
            return {}
        
        values = []
        values.extend(response.json()["results"])
        return values

    # Methods - Crawl actions

    # Adds the current Directory's Directories to the queue to be crawled
    def fill_dirs_queue(self):
        d_list = self.get_dirs_list()
        d_list_paths = []
        for d in d_list:
            d_list_paths.append(d['path'])
        self.dirs_queue.extend(d_list_paths)

    # For each file in working directory - go to the file set of it and download the files in the file set
    def crawl_pwd_files(self):
        limit = 2 # !! file sets are so large the limit of file set files downloaded for testing
        # get all files in working dir
        f_list = self.get_files_list()
        for f in f_list:
            if f['fileset']:
                # gets all files in the file set that coresponds to file f
                fset_list = self.get_fileset_list(f['path'])
                c_ount = 0 # counting to the limit
                # downloads each file set file - limited for testing
                for fs_f in fset_list:
                    # looks confusing but reverses the path string and gets the last characters and the period 
                    # path/name.jpg -> (gpj.)eman/htap
                    # then rereverses it to .jpg
                    fs_f_type = (fs_f['path'][::-1][0:fs_f['path'][::-1].index('.')+1])[::-1]
                    # Same but gets the name
                    # path/name.jpg -> gpj.(eman)/htap
                    # then rereverses it to name
                    fs_f_name = (fs_f['path'][::-1][fs_f['path'][::-1].index('.')+1:fs_f['path'][::-1].index('/')])[::-1]
                    self.get_metadata(fs_f_name, fs_f['download']['url'], fs_f_type)
                    c_ount += 1
                    if c_ount == limit:
                        break

    # Crawl working directory and all directories below - crawl as in download fileset files
    def crawl(self):
        self.status = 'running'
        while(self.dirs_queue):
            self.cd_x(self.dirs_queue[0])
            self.crawl_pwd_files()
            self.fill_dirs_queue()
            self.dirs_queue.pop(0)
        self.status = 'done'      


# create a crawler to use for api
crwlr = Crawler('')
crwlr.set_auth()

# API index - just a string
@app.route('/')
def index():
    return "Trimble Cloud Crawler API"

@app.route('/search')
def show_search_indexes():
    i_list = crwlr.search()
    list_keys = ['id','name','status','type', 'created_at', 'updated_at']
    return render_template('table.html', title='Metadata Indexes',list=i_list, keys=list_keys)

@app.route('/query')
def queryIndex():
    id = flask_request.args.get("id")
    if(id == None or id == ""):
        return Response( json.dumps({'Error':'No id'}), status=404, mimetype='application/json')
    else:
        listkeys = crwlr.indexKeys(id)
        values = crwlr.queryValues(id)
        return render_template('table.html', title='Query Index',list=values, keys=listkeys)

# API to manage crawler

# Returns info about the crawler as json
@app.route('/crawler')
def crawler():
    response_dict = {'id':crwlr.id,'working_dir':crwlr.current_path,'status':crwlr.status}
    return Response( json.dumps(response_dict), status=200, mimetype='application/json')

# Route allows you to set the path to start the crawl at
# https://<localhost>/crawler/setup?path=<path>
# path: is the path of the directory you want to start the crawl at
@app.route('/crawler/setup')
def crawler_setup():
    head_path = flask_request.args.get("path")
    if(head_path == None):
        return Response( json.dumps({'Error':'No path varible'}), status=404, mimetype='application/json')
    elif(crwlr.status == 'running'):
        return Response( json.dumps({'Error':'Crawler is running'}), status=404, mimetype='application/json')
    else:
        if(not crwlr.reset(head_path)):
            return Response( json.dumps({'Error':'Path is invalid'}), status=404, mimetype='application/json')
        else:
            return Response( json.dumps({'id':crwlr.id,'working_dir':crwlr.current_path,'status':crwlr.status}), status=201, mimetype='application/json')

# Route allows you to call crawl() on the crawler
# https://<localhost>/crawler/start
# !! note this must run the whole crawl before it returns a response right now !!
@app.route('/crawler/start')
def crawler_start():
    if(crwlr.status == 'not setup' or crwlr.status == 'running' or crwlr.status == 'done'):
        return Response( json.dumps({'Error':'Crawler is running or already done'}), status=404, mimetype='application/json')
    else:
        crwlr.crawl()
        return Response( json.dumps({'id':crwlr.id,'working_dir':crwlr.current_path,'status':crwlr.status}), status=201, mimetype='application/json')