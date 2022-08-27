import time
import json
from bottle import route, run, HTTPResponse,response
from database_setup import Connection
from request_url import get_url_for_api

@route("/metadata")
def dcl_data():
    try:
        tablename = "select contractAddress,tokenID,id from nft_tokens_by_contract_address;"
        data_in_table = Connection.selectData(tablename)
        for i in list(data_in_table):
            try:
                contractAddress =i[0]
                tokenId =i[1]
                fk_nft_token_contract_id = i[2]
                url = f'https://api.covalenthq.com/v1/1/tokens/{contractAddress}/nft_metadata/{tokenId}/?key=ckey_07aeca19d4e444148d159ee6126'
                request_api = get_url_for_api(url)
                time.sleep(2)
                contract_address = request_api['data']['items'][0]['contract_address']
                print(contract_address)
                
                meta_data_api = [i for i in request_api['data']['items'][0]['nft_data']]
                
                for i in meta_data_api:
                    token_id = i['token_id']
                    name = i['external_data']['name']
                    image = i['external_data']['image']
                    meta_data = (fk_nft_token_contract_id,name,image)
                    data_insert_into_nft_transaction = Connection.InsertData(
                        "insert ignore into metadata(fk_nft_token_contract_id,name,image)\
                                values('%s','%s','%s');" % (meta_data))
                    
                    for j in i['external_data']['attributes']:
                        type = j['trait_type'] or j['type']
                        value = j['value'] or j['description']
                        
                        meta_data_table = "select id from metadata;"
                        data_in_meta_table = Connection.selectData(meta_data_table)
                        for i in list(data_in_meta_table):
                            fk_metadata = i[0]
                            type_and_value_data = (fk_metadata,type,value)
                        data_insert_into_nft_transaction = Connection.InsertData(
                        "insert ignore into metadata_type_and_values(fk_metadata,type_data,values_data )\
                                values('%s','%s','%s');" % (type_and_value_data))
                    tablename = "select * from metadata;"
            
                    data_in_table = Connection.selectData(tablename)
                    print(data_in_table)
                   
            except Exception as e:
                print(e)
            
    except Exception as e:
        print(e)
    response = {
            "metadata transfers": list(data_in_table),
             "status": "successfully retrieved data"
        }
    return HTTPResponse(json.dumps(response, sort_keys=False, indent=4, separators=(',', ': ')))


run(host='localhost', port=8080, debug=True, reloader=True)
