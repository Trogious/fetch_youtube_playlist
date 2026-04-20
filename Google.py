import pickle
import os
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request


def Create_Service(client_secret_file, pickle_path, api_name, api_version, *scopes):
    #print(client_secret_file, api_name, api_version, scopes, sep='-')
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = [scope for scope in scopes[0]]
    #print(SCOPES)

    cred = None

    pickle_file = f'{pickle_path}/token_{API_SERVICE_NAME}_{API_VERSION}.pickle'
    # print(pickle_file)

    if os.path.exists(pickle_file):
        try:
            with open(pickle_file, 'rb') as token:
                cred = pickle.load(token)
        except (pickle.UnpicklingError, EOFError):
            cred = None

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            flow.redirect_uri = 'http://localhost:1'
            auth_url, _ = flow.authorization_url(access_type='offline', prompt='consent')
            print(f'1. Visit this URL on any device: {auth_url}')
            print('2. After authorizing, your browser will redirect to a URL that won\'t load.')
            print('3. Copy the full URL from your browser\'s address bar and paste it below.')
            redirect_url = input('Paste the redirect URL here: ')
            flow.fetch_token(authorization_response=redirect_url)
            cred = flow.credentials

        with open(pickle_file, 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=cred)
        #print(API_SERVICE_NAME, 'service created successfully')
        return service
    except Exception as e:
        #print(e)
        #print(f'Failed to create service instance for {API_SERVICE_NAME}')
        os.remove(pickle_file)
        raise e
        return None

def convert_to_RFC_datetime(year=1900, month=1, day=1, hour=0, minute=0):
    dt = datetime.datetime(year, month, day, hour, minute, 0).isoformat() + 'Z'
    return dt
