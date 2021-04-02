#APIKEY、Location,デプロイメントスペースのID、バッチ・デプロイメントのIDを指定する
api_key = 'XXXXXXXXXXXXXXXXXXXX'   #enter your key here – keep the single quotes
location = 'https://us-south.ml.cloud.ibm.com'
space_id = '8929f245xxxxxxxxxxxxxxxx'
deployment_uid="b17c279xxxxxxxxxxxxxxxx"

#入出力ファイル名の定義
input_filename="bank-test-jp-autoai-01.csv"
input_filepath=""+input_filename

import datetime
ts= datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
output_filename="spss-batch-output_"+ ts +".csv"
output_filepath=""+output_filename

#wmlへ接続
# Import the APIClient class
from ibm_watson_machine_learning import APIClient

wml_credentials = {
    "apikey": api_key,
    "url": location
}

client = APIClient(wml_credentials)
#デプロイメントスペースへ接続
client.set.default_space(space_id)
deployment_details = client.deployments.get_details(deployment_uid)


#入力ファイルのアップロード
input_assest_detail=client.data_assets.create(name=input_filename,file_path=input_filepath)

#ジョブ作成と実行
job_payload_ref = {
    client.deployments.ScoringMetaNames.INPUT_DATA_REFERENCES: [{
        "name": input_filename,
        "type": "data_asset",
        "connection": {},
        "location": {
                      "href":  input_assest_detail['metadata']['href']
                    }
    }],
 client.deployments.ScoringMetaNames.OUTPUT_DATA_REFERENCE: {
         "type": "data_asset",
         "connection": {},
         "location": {
             "name": output_filename,
             "description": "spss-batch-output"
         }
     }}
job = client.deployments.create_job(deployment_uid, meta_props=job_payload_ref, platform_job_immediate=True)

job_id = client.deployments.get_job_uid(job)
job_details=client.deployments.get_job_details(job_id)


#非同期実行のためJobの終了を確認
import time
retry_times=12
retry_sleep=3

for i in range(retry_times):
    job_details=client.deployments.get_job_details(job_id)
    jobstatus = job_details['entity']['scoring']['status']['state']
    print(jobstatus)
    if jobstatus in ['completed', 'failed']: 
        break
    time.sleep(retry_sleep)
    if i==retry_times-1:
        print("Time up")


#入出力ファイルのIDを取得
import re
input_asset_href=job_details['entity']['scoring']['input_data_references'][0]['location']['href']
input_asset_uid=re.match('/v2/assets/(.*?)\?space_id=', input_asset_href)[1]
output_asset_href=job_details['entity']['scoring']['output_data_reference']['location']['href']
output_asset_uid=re.match('/v2/assets/(.*?)\?space_id=', output_asset_href)[1]


#出力ファイルのダウンロード
client.data_assets.download(output_asset_uid,output_filepath)

#入出ファイルのDeployment Spaceからの削除
client.data_assets.delete(output_asset_uid)
client.data_assets.delete(input_asset_uid)