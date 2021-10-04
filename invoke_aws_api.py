import requests
import argparse
import json
from boto3 import Session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

from aws_requests_auth.aws_auth import AWSRequestsAuth # To generate signature
# https://github.com/DavidMuller/aws-requests-auth

from botocore.httpsession import URLLib3Session
from botocore.credentials import Credentials
# https://dev.classmethod.jp/articles/botocore-signed-process/

headers={'Content-Type': 'application/json'}


def get_auth(API_ID):
    auth = AWSRequestsAuth(aws_access_key=AWS_ACCESS_KEY,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        aws_host=API_ID+'.execute-api.'+ AWS_REGION + '.amazonaws.com',
                        aws_region=AWS_REGION,
                        aws_service='execute-api')
    return auth


def invoke_api():
    auth = get_auth(API_ID)
    api_url='https://'+ API_ID + '.execute-api.'+ AWS_REGION +'.amazonaws.com/'+API
    credentials = Credentials(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)

    if METHOD and METHOD=='GET':

        response = requests.get(api_url, auth=auth, headers=headers)
    elif METHOD and METHOD=='POST':
        request=AWSRequest(method=METHOD,url=api_url, data=PAYLOAD.encode('utf-8'), headers=headers)
        
        SigV4Auth(credentials, "execute-api", AWS_REGION).add_auth(request)
        
        response=URLLib3Session().send(request.prepare())
    print('response:',response.content)
    if 'error_message' in str(response.content): quit()
    return response

def main():
    result=invoke_api()
    print('\nCompleted ...')

def get_aws_credentials_from_profile(profile_name):
    return Session(profile_name=profile_name).get_credentials()

if __name__ == '__main__':
    global AWS_ACCESS_KEY
    global AWS_SECRET_ACCESS_KEY
    global API_ID
    global API
    global SCENARIO_ID
    global AWS_PROFILE
    global AWS_REGION
    global METHOD
    global PAYLOAD

    AWS_ACCESS_KEY=""
    AWS_SECRET_ACCESS_KEY=""
    AWS_REGION=""
    METHOD=""
    PAYLOAD=""

    parser = argparse.ArgumentParser(description='Invoke AWS API')
    parser.add_argument('-p',  '--profile',help='AWS profile名')
    parser.add_argument('--awsAccessKey', help='AWS Access Key Id')
    parser.add_argument('--awsSecretAccessKey', help='AWS Secret Access Key')
    parser.add_argument('--awsRegion', help='AWS Region')
    parser.add_argument('--apiId', help='API Id')
    parser.add_argument('--api', help='API')
    parser.add_argument('--method', help='Request Method')
    parser.add_argument('--payload', help='Payload')

    args = parser.parse_args()

    if args.profile:
        credentials = get_aws_credentials_from_profile(args.profile)
        AWS_ACCESS_KEY=credentials.access_key
        AWS_SECRET_ACCESS_KEY=credentials.secret_key
    elif args.awsAccessKey and args.awsSecretAccessKey :
        AWS_ACCESS_KEY=args.awsAccessKey
        AWS_SECRET_ACCESS_KEY=args.awsSecretAccessKey

    if args.apiId :API_ID=args.apiId 
    if args.api :API=args.api 
    if args.awsRegion:AWS_REGION=args.scenarioId
    if args.method: METHOD=args.method
    if args.payload:PAYLOAD=args.payload
    main()
