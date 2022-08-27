
from decimal import Decimal
from lib2to3.pytree import convert
from select import select
import time
import json
import requests
from bottle import route, run, HTTPResponse
from database_setup import Connection
from request_url import get_url_for_api
from requests.structures import CaseInsensitiveDict


def convert_wei_to_eth(amount):
    eth_amount = int(amount) / 10**18
    return eth_amount


@route("/fetch_all_api")
def fetch_all_api():
    bored_ape_yacht_club = '0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D'
    bored_ape_kennel_club = '0xba30E5F9Bb24caa003E9f2f0497Ad287FDF95623'
    crypto_kitties = '0x06012c8cf97BEaD5deAe237070F9587f8E7A266d'
    otherdeed_of_otherside = '0x34d85c9CDeB23FA97cb08333b511ac86E1C4E258'
    genuine_undead = '0x209e639a0EC166Ac7a1A4bA41968fa967dB30221'
    sorare = '0x629A673A8242c2AC4B7B8C5D8735fbeac21A6205'
    moonbirds_oddities = '0x1792a96E5668ad7C167ab804a100ce42395Ce54D'
    potatoz = '0x39ee2c7b3cb80254225884ca001F57118C8f21B6'
    meebits = '0x7Bd29408f11D2bFC23c34f18275bBf23bB716Bc7'
    mutant_ape_yacht_club = '0x60E4d786628Fea6478F785A6d7e704777c86a7c6'
    moonbirds = '0x23581767a106ae21c074b2276D25e5C3e136a68b'
    pudgy_penguins = '0xBd3531dA5CF5857e7CfAA92426877b022e612cf8'

    all_contract_address = [otherdeed_of_otherside, bored_ape_yacht_club, bored_ape_kennel_club, crypto_kitties,
                            genuine_undead, sorare, moonbirds, moonbirds_oddities, potatoz, meebits, mutant_ape_yacht_club, pudgy_penguins]
    for contract_address in all_contract_address:
        for j in range(1, 100):
            request_api = get_url_for_api(
                f"https://api.etherscan.io/api?module=account&action=tokennfttx&contractaddress={contract_address}&page={j}&offset=100&sort=asc&apikey=12NQZXJR4TFFMJHBSTSMCPFR9PCI6UQ6SA")
            try:
                nft_transaction_data = [i for i in request_api['result']]
                for i in nft_transaction_data:
                    timeStampdata = i['timeStamp']
                    tokenID = i['tokenID']
                    blockNumber = i['blockNumber']
                    hash_id = i['hash']
                    contractAddress = i['contractAddress']
                    tokenName = i['tokenName']
                    tokenSymbol = i['tokenSymbol']
                    transactionIndex = i['transactionIndex']
                    blockHash = i['blockHash']
                    from_address = i['from']
                    to_address = i['to']
                    gas_offered = i['gas']
                    gas_spent = i['gasUsed']
                    cumulativeGasUsed = i['cumulativeGasUsed']
                    gas_price = i['gasPrice']
                    tokenId = hex((int(tokenID)))
                    data = (timeStampdata, tokenID, blockNumber, hash_id, contractAddress, tokenName, tokenSymbol, transactionIndex,
                            blockHash, from_address, to_address, cumulativeGasUsed,
                            gas_offered, gas_spent, gas_price)
                    select_from_nft_token = "select contractAddress,tokenID from nft_tokens_by_contract_address;"
                    data_in_nft_tokens_by_contract_address = Connection.selectData(
                        select_from_nft_token)
                    for i in list(data_in_nft_tokens_by_contract_address):
                        check_data_exist = "select contractAddress,tokenID from nft_tokens_by_contract_address where contractAddress= '%s' and tokenID='%s';" % (
                            contractAddress, tokenID)
                        selected_fields_in_table = Connection.selectOneData(
                            check_data_exist)

                        if selected_fields_in_table is None:

                            print("data", data)

                            data_insert_into_nft_tokens_by_contract_address = Connection.InsertData(
                                "insert ignore into  nft_tokens_by_contract_address(timeStampdata,tokenID,blockNumber,hash_id,contractAddress,tokenName,tokenSymbol,transactionIndex,blockHash,from_address,to_address,\
                                gas_offered,gas_spent,cumulativeGasUsed,gas_price) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s') ;" % (data))

                    select_foreign_key = 'select id from nft_tokens_by_contract_address;'
                    get_foreign_key_from_table = Connection.selectData(select_foreign_key)
                    for i in list(get_foreign_key_from_table):
                        fk_nft_token_contract_id = i[0]
                    time.sleep(1)
                    url = f'https://api.rarify.tech/data/tokens/ethereum:{contractAddress}:{tokenId}/insights/all_time'

                    headers = CaseInsensitiveDict()
                    headers["Accept"] = "application/json"
                    headers["Authorization"] = "Bearer {8b986d94-a689-45e3-84b3-3bac9167e859}"
                    resp = requests.get(url, headers=headers)
                    data = resp.text
                    parse_json = json.loads(data)
                    if parse_json:
                        price_id = parse_json['included'][0]['id']
                        avg_price = convert_wei_to_eth(
                            parse_json['included'][0]['attributes']['avg_price'])
                        max_price = convert_wei_to_eth(
                            parse_json['included'][0]['attributes']['max_price'])
                        min_price = convert_wei_to_eth(
                            parse_json['included'][0]['attributes']['min_price'])
                        trades = parse_json['included'][0]['attributes']['trades']
                        volume = convert_wei_to_eth(
                            parse_json['included'][0]['attributes']['volume'])
                        price_history = parse_json['included'][1]['attributes']['history']
                        
                        
                        for j in price_history:
                            history_avg_price = convert_wei_to_eth(
                                j['avg_price'])
                            history_max_price = convert_wei_to_eth(
                                j['max_price'])
                            history_min_price = convert_wei_to_eth(
                                j['min_price'])
                            history_trades = j['trades']
                            history_volume = convert_wei_to_eth(j['volume'])
                            history_time = j['time']
                            price_data = (fk_nft_token_contract_id, price_id, history_avg_price, history_max_price, history_min_price, history_trades, history_volume,
                                          history_time)
                            print(price_data)
                            check_data_in_table = "select fk_nft_token_contract_id,price_id,history_avg_price,history_max_price,history_min_price,history_trades,history_volume,history_time from nft_transactions_history;"
                            data_in_nft_transactions_history = Connection.selectData(check_data_in_table)
                            for i in list(data_in_nft_transactions_history):
                                check_data_exist = "select fk_nft_token_contract_id,price_id,history_avg_price,history_max_price,history_min_price,history_trades,\
                                        history_volume,history_time from nft_transactions_history where fk_nft_token_contract_id= '%s' and price_id = '%s' and history_avg_price= '%s' and  history_max_price= '%s' and history_min_price= '%s' and history_trades= '%s' and \
                                        history_volume= '%s' and history_time = '%s' ;" % (fk_nft_token_contract_id, price_id, history_avg_price, history_max_price, history_min_price, history_trades, history_volume,
                                                                                           history_time)
                                get_data_from_table = Connection.selectOneData(
                                    check_data_exist)

                                if get_data_from_table is None:
                                    insert_into_nft_transaction_history = Connection.InsertData(
                                        "insert ignore into nft_transactions_history(fk_nft_token_contract_id,price_id,history_avg_price,history_max_price,history_min_price,history_trades,\
                                        history_volume,history_time) \
                                    values('%s','%s','%s','%s','%s','%s','%s','%s');" % (price_data))

                        data = (price_id, avg_price, max_price, min_price,
                                trades, volume, fk_nft_token_contract_id)

                        print("data", data)

                        data_insert_into_nft_tokens_by_contract_address = Connection.UpdateData(

                            "UPDATE nft_tokens_by_contract_address SET price_id= '%s',avg_price= '%s',max_price= '%s', min_price= '%s',trades= '%s' ,volume= '%s' WHERE  id = '%s';" % (data))
                    time.sleep(2)
                    tablename = "select * from nft_tokens_by_contract_address;"

                    data_in_table = Connection.selectData(tablename)
                    time.sleep(1)
            except Exception as e:
                print(e)
    response = {
        "nft_tokens_by_contract_address_data": list(data_in_table),
        "status": "successfully retrieved data"
    }
    return HTTPResponse(json.dumps(response, sort_keys=False, indent=4, separators=(',', ': ')))


run(host='localhost', port=8045, debug=True, reloader=True)

