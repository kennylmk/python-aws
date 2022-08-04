import boto3
import sys
from boto3.session import Session
from boto3.dynamodb.conditions import Key, Attr
import argparse

# def main(argv):
def main():
    # if argv:
    #     profile = "fch-dev"
    #     env_name=argv[0]

    #     session=Session(profile_name=profile)
    #     delete_tables=[]
    #     dynamodb=session.resource('dynamodb')
        # tables = dynamodb.tables

        # for table in dynamodb.tables.all():
        #     if table.name.startswith(env_name + "-"):
        #         if input("Delete {} ? (Y/n)".format(table.name)) == "Y":
        #             table.delete()
        #             print("           {} deleted".format(table.name))
        #         else:
        #             print("            Skip to delete {}".format(table.name))
        #         # delete_tables.append(table.name)

        # # if delete_tables:
        # #     for del_tables in delete_tables:
        # #         print(del_tables)

    if PROFILE_NAME:
        session=Session(profile_name=PROFILE_NAME)
    else:
        session=Session(aws_access_key_id=AWS_ACCESS_KEY,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    delete_tables=[]
    dynamodb=session.resource('dynamodb')

    # ========= Delete All Items =================
    table = dynamodb.Table(TABLE_NAME)
    table2 = dynamodb.Table(TABLE_NAME)

    response = table.scan()
    items = []
    scan=False
 
    # # items.extend(response['Items'])
    # items=response['Items']
    # with table.batch_writer() as batch:
    #     for item in items:
    #         partitionKey= item['partitionKey']
    #         sortKey= item['sortKey']
    #         runNo = str(partitionKey).split('#')[0]
    #         if  '#record' in partitionKey and (int(runNo)==120 or int(runNo)==121):
    #             print(runNo,partitionKey,sortKey)
    #             table2.delete_item(Key={'partitionKey':partitionKey,'sortKey':sortKey})
    #             # batch.delete_item(Key={'partitionKey':partitionKey,'sortKey':sortKey})
    last_evaluated_key = None
    fetch_data = True
    params = {
        "TableName" : TABLE_NAME,
    }

    params['FilterExpression']=Attr('partitionKey').contains('#record') & Attr('partitionKey').begins_with('120#')

    while fetch_data:
        if last_evaluated_key:
            params["ExclusiveStartKey"] = last_evaluated_key

        response = table.scan(**params)
        # response = table.scan( 
        #     ExclusiveStartKey=response['LastEvaluatedKey']
        #     )
        # items.extend(response['Items'])
        items=response['Items']
        
        if items: print(len(items))
        for item in items:
            partitionKey= item['partitionKey']
            runNo = str(partitionKey).split('#')[0]
            sortKey= item['sortKey']
            # if  '#record' in partitionKey and PARTITION_KEY in partitionKey:
            print(runNo,partitionKey,sortKey)
            # if  '#record' in partitionKey and (int(runNo)<120):
            # if  (int(runNo)==120):
            # print("......DELETE ",runNo,partitionKey,sortKey)
            table2.delete_item(Key={'partitionKey':partitionKey,'sortKey':sortKey})
                # batch.delete_item(Key={'partitionKey':partitionKey,'sortKey':sortKey})

        if "LastEvaluatedKey" in response:
            last_evaluated_key = response["LastEvaluatedKey"]
        else:
            fetch_data = False
    # i=1

    # keyName="family_name"
    # keyValue="ヨミコミ"

    # for item in items:
    #     partitionKey= item['partitionKey']
    #     sortKey= item['sortKey']
    #     print(partitionKey,sortKey)
        # if keyName in item:
        #     # print(item['form_id'],item['reservation_incharge'])
        #     data=item[keyName]
        #     condition="ヨミコミ"==item[keyName]

        #     if  condition:
        #         print(" >>>>>>> " ,condition,  i,item['form_id'],item[keyName])
        #         table2.delete_item(Key={'form_id':item['form_id']})
        #         i+=1
        

if __name__ == "__main__":
    # main(sys.argv[1:])

    global PROFILE_NAME
    global AWS_ACCESS_KEY
    global AWS_SECRET_ACCESS_KEY
    global ENV_NAME
    global TABLE_NAME
    global PARTITION_KEY

    current_status=''
    parser = argparse.ArgumentParser("Delete all items from dynamoDB table")
    parser.add_argument('-p','--profile', help='AWS profile name')
    parser.add_argument('-ak','--awsAccessKey', help='AWS Access Key')
    parser.add_argument('-sak','--awsSecretAccessKey', help='AWS Secret Access Key')
    parser.add_argument('-t','--tableName', help='Table name')
    parser.add_argument('-e','--envName', help='Environments name')
    parser.add_argument('-pk','--partitionKey', help='Partition Key')

    args = parser.parse_args()
    
    if args.profile:
        PROFILE_NAME = args.profile
    else:
        PROFILE_NAME=''
        if args.awsAccessKey and args.awsSecretAccessKey:
            AWS_ACCESS_KEY=args.awsAccessKey
            AWS_SECRET_ACCESS_KEY=args.awsSecretAccessKey
        else:
            print('NO AWS CREDENTIAL provided')
            quit()
    ENV_NAME=args.envName
    TABLE_NAME=args.tableName

    if args.partitionKey:
        PARTITION_KEY=args.partitionKey
    else:
        PARTITION_KEY=""
    main()