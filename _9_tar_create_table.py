import glob
import json
import os

import requests
import pandas as pd


# Authorization = "Bearer c61440c9c2494b4189fabfe94aa657cf"


def check(source_tableName, Authorization):
    url = "http://172.24.244.28:31950/app/cmcc/data/datacollect/dag/duplication/check"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Authorization": Authorization,
        "Connection": "keep-alive",
        "Cookie": "tenantId=1; userId=1; bdp-auth-ticket-id=8719f81bf0dd4edf808366e0407dbb37; bdp-auth-tenant-id=1; bdp-user-ticket-id=LtJY/C23iLboMXNDrHZDPIy02jmdo1I6F7ycDyNpiOY=",
        "Host": "172.24.244.28:31950",
        "Referer": "http://172.24.244.28:31950/datacollect/TaskManage",
        "TENANT-ID": "1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36"
    }

    params = {"dagName": source_tableName}
    r = requests.get(url=url, headers=headers, params=params)
    return r.json()


def get_src_tar_id(Authorization):
    url = "http://172.24.244.28:31950/app/cmcc/data/datacollect/datasource/databasetype"

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Authorization": Authorization,
        "Connection": "keep-alive",
        "Cookie": "tenantId=1; userId=1; bdp-auth-ticket-id=4bf9bc337abe47a8bad57c25e35601b3; bdp-auth-tenant-id=1; bdp-user-ticket-id=LtJY/C23iLboMXNDrHZDPL3V+xvTl7r9AGanXz5kbeE=",
        "Host": "172.24.244.28:31950",
        # "Referer": "http://172.24.244.28:31950/datacollect/TaskManage/DataFlow?SyncType=structured&TaskName=%E4%BA%BA%E5%A4%A7%E8%B4%A2%E7%BB%8F%E8%81%94%E7%BD%91%E7%9B%91%E7%9D%A3%E7%B3%BB%E7%BB%9F_%E8%B4%A2%E6%94%BF%E9%A2%84%E7%AE%97_%E5%86%B3%E7%AE%97_%E6%8E%A5%E5%85%A5%E8%A1%A8&mode=local&yarnQueue",
        "TENANT-ID": "1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36"
    }

    r = requests.get(url=url, headers=headers)
    # srcID
    postgresql_result = {}
    # tarID
    doris_result = {}
    for v in r.json()['data']['postgresql']:
        postgresql_result[f"{v['name']}"] = v['id']
    for v in r.json()['data']['doris']:
        doris_result[f"{v['name']}"] = v['id']
    return doris_result, postgresql_result


def add_columns(id, tableName, tableSchema, Authorization):
    url = "http://172.24.244.28:31950/app/cmcc/data/datacollect/datasource/addcolumns"

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Authorization": Authorization,
        "Connection": "keep-alive",
        "Content-Length": "98",
        "Content-Type": "application/json",
        "Cookie": "tenantId=1; userId=1; bdp-auth-ticket-id=89c161ebdeba4b238129fb5f79ad799f; bdp-auth-tenant-id=1; bdp-user-ticket-id=LtJY/C23iLboMXNDrHZDPAg1WEBnoLmTnjbGGQ/mp/A=",
        "Host": "172.24.244.28:31950",
        "Origin": "http://172.24.244.28:31950",
        # "Referer": "http://172.24.244.28:31950/datacollect/TaskManage/DataFlow?SyncType=structured&TaskName=%E4%BA%BA%E5%A4%A7%E8%B4%A2%E7%BB%8F%E8%81%94%E7%BD%91%E7%9B%91%E7%9D%A3%E7%B3%BB%E7%BB%9F_%E8%B4%A2%E6%94%BF%E9%A2%84%E7%AE%97_%E5%86%B3%E7%AE%97_%E6%8E%A5%E5%85%A5%E8%A1%A8&mode=local&yarnQueue",
        "TENANT-ID": "1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36"
    }
    params = {
        "id": id,
        "tableName": tableName,
        "tableSchema": tableSchema,
    }

    r = requests.post(url=url, headers=headers, data=json.dumps(params))
    return r.status_code


def get_src_table_info(id, tableName, tableSchema, Authorization):
    url = "http://172.24.244.28:31950/app/cmcc/data/datacollect/datasource/tableinfo"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Authorization": Authorization,
        "Connection": "keep-alive",
        # "Content-Length": "138",
        "Content-Type": "application/json",
        "Cookie": "tenantId=1; userId=1; bdp-auth-tenant-id=1; bdp-auth-ticket-id=25b4678452bd46d286400bcfb65ce723; bdp-user-ticket-id=LtJY/C23iLboMXNDrHZDPKSOUY7+StmGJ9pgZY+2XFs=",
        "Host": "172.24.244.28:31950",
        "Origin": "http://172.24.244.28:31950",
        # "Referer": "http://172.24.244.28:31950/datacollect/TaskManage/DataFlow?SyncType=structured&TaskName=%E4%BA%BA%E5%A4%A7%E8%B4%A2%E7%BB%8F%E8%81%94%E7%BD%91%E7%9B%91%E7%9D%A3%E7%B3%BB%E7%BB%9F_%E8%B4%A2%E6%94%BF%E9%A2%84%E7%AE%97_%E5%86%B3%E7%AE%97_%E6%8E%A5%E5%85%A5%E8%A1%A8&mode=local&yarnQueue",
        "TENANT-ID": "1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36"
    }
    params = {
        "id": id,
        "tableName": tableName,
        "tableNameList": [tableName],
        "tableSchema": tableSchema
    }

    r = requests.post(url=url, headers=headers, data=json.dumps(params))
    response = r.json()
    column_info = response['data'][0]['columnList']
    return column_info


def create_src_table(columns, dis_name, tableComment, tarTableName, Authorization):
    url = "http://172.24.244.28:31950/app/cmcc/data/datacollect/datasource/createTable"

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Authorization": Authorization,
        "Connection": "keep-alive",
        "Content-Length": "15588",
        "Content-Type": "application/json",
        "Cookie": "tenantId=1; userId=1; bdp-auth-tenant-id=1; bdp-auth-ticket-id=17c2d0f9965e473abf45b730a253ac0d; bdp-user-ticket-id=LtJY/C23iLboMXNDrHZDPF3BtUTLVEZerYlh2azNrEM=",
        "Host": "172.24.244.28:31950",
        "Origin": "http://172.24.244.28:31950",
        # "Referer": "http://172.24.244.28:31950/datacollect/TaskManage/DataFlow?SyncType=structured&TaskName=%E4%BA%BA%E5%A4%A7%E8%B4%A2%E7%BB%8F%E8%81%94%E7%BD%91%E7%9B%91%E7%9D%A3%E7%B3%BB%E7%BB%9F_%E8%B4%A2%E6%94%BF%E9%A2%84%E7%AE%97_%E5%86%B3%E7%AE%97_%E6%8E%A5%E5%85%A5%E8%A1%A8&mode=local&yarnQueue",
        "TENANT-ID": "1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36"
    }
    params = {
        "columns": columns,
        "partitionInfo": dis_name,
        "tableComment": tableComment,
        "tarSchema": "ods",
        "tarId": "1ecdd2a14b844e2ab1f1c8f283eb9398",
        "tarTableName": tarTableName,
    }
    r = requests.post(url=url, headers=headers, data=json.dumps(params))
    return r


def search_table(srcId, srcSchema, srcTableName, tarId, fields, Authorization):
    url = "http://172.24.244.28:31950/app/cmcc/data/datacollect/datasource/searchTable"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Authorization": Authorization,
        "Connection": "keep-alive",
        "Content-Length": "1236",
        "Content-Type": "application/json",
        "Cookie": "tenantId=1; userId=1; bdp-auth-tenant-id=1; bdp-auth-ticket-id=25b4678452bd46d286400bcfb65ce723; bdp-user-ticket-id=LtJY/C23iLboMXNDrHZDPKSOUY7+StmGJ9pgZY+2XFs=",
        "Host": "172.24.244.28:31950",
        "Origin": "http://172.24.244.28:31950",
        # "Referer": "http://172.24.244.28:31950/datacollect/TaskManage/DataFlow?SyncType=structured&TaskName=%E4%BA%BA%E5%A4%A7%E8%B4%A2%E7%BB%8F%E8%81%94%E7%BD%91%E7%9B%91%E7%9D%A3%E7%B3%BB%E7%BB%9F_%E8%B4%A2%E6%94%BF%E9%A2%84%E7%AE%97_%E5%86%B3%E7%AE%97_%E6%8E%A5%E5%85%A5%E8%A1%A8&mode=local&yarnQueue",
        "TENANT-ID": "1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36"
    }
    params = {
        "fileds": fields,
        "srcId": srcId,
        "srcSchema": srcSchema,
        "srcTableName": srcTableName,
        "tarId": tarId
    }
    r = requests.post(url=url, headers=headers, data=json.dumps(params))
    response = r.json()
    return response['data']['columnList']
    # return response


def custom_create_table(srcDbType="postgresql", tarDbType="doris", Authorization=""):
    params = {
        "srcDbType": srcDbType,
        "tarDbType": tarDbType
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Authorization": Authorization,
        "Connection": "keep-alive",
        "Cookie": "tenantId=1; userId=1; bdp-auth-ticket-id=f76b851c4a324098bef76a4d6e1db6bb; bdp-auth-tenant-id=1; bdp-user-ticket-id=LtJY/C23iLboMXNDrHZDPA22XCrKzWqDJKYwhllWLec=",
        "Host": "172.24.244.28:31950",
        "Referer": "http://172.24.244.28:31950/datacollect/TaskManage/DataFlow?SyncType=structured&TaskName=tw&mode=local&yarnQueue",
        "TENANT-ID": "1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36"
    }
    url = "http://172.24.244.28:31950/app/cmcc/data/datacollect/datasource/customCreateTable"
    r = requests.get(url=url, headers=headers, params=params)
    return r.json()


def save_dag(src_fieldsMsg, src_selectedRowKeys, FieldMap, tar_fieldsMsg, tar_selectedRowKeys, tableComment,
             source_name, tableName
             , tableSchema, dataBaseMsg, ods_name, ds, tar_db_type,
             sourceName, writeMode, tar_dataBaseMsg, targetCleanSql="", Authorization=""):
    url = "http://172.24.244.28:31950/app/cmcc/data/datacollect/dag/daginfo"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Authorization": Authorization,
        "Connection": "keep-alive",
        "Content-Length": "152492",
        "Content-Type": "application/json",
        "Cookie": "tenantId=1; userId=1; bdp-auth-tenant-id=1; bdp-auth-ticket-id=25b4678452bd46d286400bcfb65ce723; bdp-user-ticket-id=LtJY/C23iLboMXNDrHZDPKSOUY7+StmGJ9pgZY+2XFs=",
        "Host": "172.24.244.28:31950",
        "Origin": "http://172.24.244.28:31950",
        # "Referer": "http://172.24.244.28:31950/datacollect/TaskManage/DataFlow?SyncType=structured&TaskName=%E4%BA%BA%E5%A4%A7%E8%B4%A2%E7%BB%8F%E8%81%94%E7%BD%91%E7%9B%91%E7%9D%A3%E7%B3%BB%E7%BB%9F_%E8%B4%A2%E6%94%BF%E9%A2%84%E7%AE%97_%E5%86%B3%E7%AE%97_%E6%8E%A5%E5%85%A5%E8%A1%A8&mode=local&yarnQueue",
        "TENANT-ID": "1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36"
    }
    params = {
        "dagName": tableComment,
        "dagOwner": "xiexinlai",
        "dagParam": {},
        "dagStatus": "7",
        "metadata": {},
        "mode": "local",
        "remark": "",
        "structured": 1,
        "unStructured": 0,
        "watermark": 0,
        "tasks": [
            {"domId": "jsPlumb_1_1",
             "name": "DB",
             "posLeft": 467.9375,
             "posTop": 393.21875,
             "taskId": "jsPlumb_1_1",
             # "taskName": "人大财经联网监督系统_财政预算_决算_接入表",
             "taskName": tableComment,
             "taskType": "DB",
             "taskParam": {"dataType": "postgresql",
                           "metadata": "",
                           "name": "DB",
                           "partitions": [],
                           "positionType": "source",
                           # "sourceName": "人大财经联网监督系统",
                           "sourceName": source_name,
                           # "tableName": "public.npcs_ods_fac",
                           "tableName": tableName,
                           # "tableSchema": "public",
                           "tableSchema": tableSchema,
                           "dataBaseMsg": dataBaseMsg,
                           "fieldsMsg": [
                               # {"tableName": "public.npcs_ods_fac",
                               {"tableName": tableName,
                                "field": src_fieldsMsg}
                           ],
                           "selectedRowKeys": src_selectedRowKeys,
                           "selectedRows": src_fieldsMsg,
                           "syncRuleConfig": {
                               "metadata": {},
                               "syncType": "0",
                           }
                           }
             },
            {"domId": "jsPlumb_1_10",
             "name": "DB",
             "parentId": "jsPlumb_1_1",
             "posLeft": 812.1771240234375,
             "posTop": 359.84375,
             "taskId": "jsPlumb_1_10",
             # "taskName": "人大财经联网监督系统_财政预算_决算_接入表",
             "taskName": tableComment,
             "taskType": "DB",
             "taskParam": {
                 "dataType": tar_db_type,
                 "fieldRadioValue": "1",
                 "name": "DB",
                 "positionType": "target",
                 "preSQLData": [],
                 "runStatusOfNoData": "1",
                 "sourceName": "ORIS_平台数仓" if tar_db_type == "doris" else sourceName,
                 "tableName": ods_name,
                 "tableSchema": "ods" if tar_db_type == "doris" else tableSchema,
                 "targetCleanSql": targetCleanSql,
                 "writeMode": "overwrite" if tar_db_type == "doris" else writeMode,
                 "FieldMap": FieldMap,
                 "dataBaseMsg": tar_dataBaseMsg,
                 "fieldsMsg": [
                     {
                         # "tableName": "ods.to_m_npcs_ods_fac",
                         # "tableName": "ods." + ods_name,
                         "tableName": ods_name,
                         "field": tar_fieldsMsg
                     }
                 ],
                 "partitions" if tar_db_type == "doris" else None: [{
                     "colName": ds,
                     "colValue": "",
                     "condition": "day",
                     "isDynamic": True,
                     "offsetDate": 0,
                     "referenceTime": "${SCHEDULE_TIME}"
                 }, ] if tar_db_type == "doris" else None,
                 "selectedRowKeys": tar_selectedRowKeys,
                 "selectedRows": tar_fieldsMsg,
                 "truncate" if tar_db_type != "doris" else None: True if tar_db_type != "doris" else None
             }
             }
        ]
    }
    response = requests.post(url=url, headers=headers, data=json.dumps(params))
    return response.json()


def get_src_table(id, table_schema, Authorization):
    url = "http://172.24.244.28:31950/app/cmcc/data/datacollect/datasource/tablenames"
    params = {
        "id": id,
        "tableSchema": table_schema
    }

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Authorization": Authorization,
        "Connection": "keep-alive",
        "Cookie": "tenantId=1; userId=1; bdp-auth-ticket-id=f76b851c4a324098bef76a4d6e1db6bb; bdp-auth-tenant-id=1; bdp-user-ticket-id=LtJY/C23iLboMXNDrHZDPA22XCrKzWqDJKYwhllWLec=",
        "Host": "172.24.244.28:31950",
        "Referer": "http://172.24.244.28:31950/datacollect/TaskManage/DataFlow?SyncType=structured&TaskName=%E4%BA%BA%E5%A4%A7%E8%B4%A2%E7%BB%8F%E8%81%94%E7%BD%91%E7%9B%91%E7%9D%A3%E7%B3%BB%E7%BB%9F_%E8%B4%A2%E6%94%BF%E9%A2%84%E7%AE%97_%E5%86%B3%E7%AE%97_%E6%8E%A5%E5%85%A5%E8%A1%A831&dagId=ef708b7cc15c4e469942563c851113ef&mode=local&shareId&yarnQueue",
        "TENANT-ID": "1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36"
    }
    r = requests.get(headers=headers, url=url, params=params)
    return r


def get_database_type(db, name, Authorization):
    url = "http://172.24.244.28:31950/app/cmcc/data/datacollect/datasource/databasetype"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Authorization": Authorization,
        "Connection": "keep-alive",
        "Cookie": "tenantId=1; userId=1; bdp-auth-ticket-id=f76b851c4a324098bef76a4d6e1db6bb; bdp-auth-tenant-id=1; bdp-user-ticket-id=LtJY/C23iLboMXNDrHZDPA22XCrKzWqDJKYwhllWLec=",
        "Host": "172.24.244.28:31950",
        "Referer": "http://172.24.244.28:31950/datacollect/TaskManage/DataFlow?SyncType=structured&TaskName=aaa&mode=local&yarnQueue",
        "TENANT-ID": "1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36"
    }
    r = requests.get(url=url, headers=headers)
    response = r.json()
    for n in response["data"][f"{db}"]:
        if name == n["name"]:
            return n


def read_csv():
    df_columns = ['数据源名称',
                  '前置库英文名称',
                  '中台表中文名称、中台数据流名称',
                  '中台英文名称',
                  '分区',
                  'table_schema',
                  'dbName',
                  'dbType',
                  'target_db_name',
                  'target_db_type',
                  'target_table_schema']
    path = r"E:/公司/copilotkit/Upload/*.csv"
    file_path = glob.glob(path)
    df = pd.read_csv(file_path[0])
    source_name = df["数据源名称"]
    source_tableName_en = df["前置库英文名称"]
    source_tableName = df["中台表中文名称、中台数据流名称"]
    tar_tableName = df["中台英文名称"]
    dis_name = df["分区"]
    tableSchema = df["table_schema"]
    db = df["dbType"]
    target_db_name = df["target_db_name"]
    target_db_type = df["target_db_type"]
    target_table_schema = df["target_table_schema"]
    assert len(source_name.to_list()) > 0, "Error，数据源名称不能为空！"
    assert len(source_tableName_en.to_list()) > 0, "Error，前置库英文名称不能为空！"
    assert len(source_tableName.to_list()) > 0, "Error，中台表中文名称、中台数据流名称不能为空！"
    assert len(tar_tableName.to_list()) > 0, "Error，中台英文名称名称不能为空！"
    assert len(dis_name.to_list()) > 0, "Error，分区不能为空！"
    assert len(tableSchema.to_list()) > 0, "Error，table_schema不能为空！"
    assert len(db.to_list()) > 0, "Error，dbType不能为空！"
    assert len(target_db_name.to_list()) > 0, "Error，target_db_name不能为空！"
    assert len(target_db_type.to_list()) > 0, "Error，target_db_type不能为空！"
    assert len(target_table_schema.to_list()) > 0, "Error，target_table_schema不能为空！"
    assert df.columns.to_list() == df_columns, f"Error, 文件表名错误！ 应为{df_columns}"
    return df.to_dict(orient="records")


def main(token):
    all_result = []
    df = read_csv()
    doris_result, postgresql_result = get_src_tar_id(Authorization=token)

    for le in range(len(df)):
        src_dataBaseMsg = get_database_type(df[le]["dbType"], df[le]["数据源名称"], Authorization=token)

        src_columns = get_src_table_info(id=src_dataBaseMsg["id"],
                                         tableName=df[le]["table_schema"] + "." + df[le]["前置库英文名称"],
                                         tableSchema=df[le]["table_schema"],
                                         Authorization=token)
        add_src_columns_status = add_columns(id=src_dataBaseMsg["id"],
                                             tableName=df[le]["table_schema"] + "." + df[le]["前置库英文名称"],
                                             tableSchema=df[le]["table_schema"],
                                             Authorization=token)
        fields1 = []
        for i in src_columns:
            fields1.append(i['columnName'])

        if df[le]["target_db_type"].lower() == "doris":
            tar_dataBaseMsg = get_database_type(df[le]["target_db_type"], "DORIS_平台数仓", Authorization=token)
            custom_create_table_status = custom_create_table(Authorization=token)
            assert custom_create_table_status["code"] == 200
            src_table = get_src_table(id="1ecdd2a14b844e2ab1f1c8f283eb9398", table_schema="ods", Authorization=token)

            r1 = search_table(srcId=src_dataBaseMsg["id"],
                              srcSchema=df[le]["table_schema"],
                              srcTableName=df[le]["table_schema"] + "." + df[le]["前置库英文名称"],
                              tarId="1ecdd2a14b844e2ab1f1c8f283eb9398",
                              fields=fields1,
                              Authorization=token)

            create_src_table_status = create_src_table(r1,
                                                       dis_name=df[le]["分区"],
                                                       tableComment=df[le]["中台表中文名称、中台数据流名称"],
                                                       tarTableName=df[le]["target_db_name"],
                                                       Authorization=token)

            tar_columns = get_src_table_info(id="1ecdd2a14b844e2ab1f1c8f283eb9398",
                                             tableName="ods." + df[le]["target_db_name"],
                                             tableSchema="ods",
                                             Authorization=token)
            fields = []
            for i in tar_columns:
                fields.append(i['columnName'])
            # r = search_table(srcId=df[le]["数据源名称"],
            #                  srcSchema=df[le]["table_schema"],
            #                  srcTableName=df[le]["table_schema"] + "." + df[le]["前置库英文名称"],
            #                  tarId="1ecdd2a14b844e2ab1f1c8f283eb9398",
            #                  fields=fields)

            src_selectedRowKeys = []
            tar_selectedRowKeys = []
            for k, v in enumerate(src_columns):
                v["src_key"] = src_dataBaseMsg["id"] + "-" + df[le]["table_schema"] + "." + df[
                    le]["前置库英文名称"] + "-" + v["columnName"]
                # v["src_key"] = "ff90525f66c24b3a9844bd80765cbb2d-public.npcs_ods_fac-" + v["columnName"]
                v["src_parent"] = src_dataBaseMsg["id"] + "-" + df[le]["table_schema"] + "." + df[le]["前置库英文名称"]
                # v["src_parent"] = "ff90525f66c24b3a9844bd80765cbb2d-public.npcs_ods_fac"
                v["src_position"] = f"{k}"
                v["tar_key"] = "1ecdd2a14b844e2ab1f1c8f283eb9398" + "-" + "ods." + df[le]["中台英文名称"] + "-" + v[
                    "columnName"]
                v["tar_parent"] = "1ecdd2a14b844e2ab1f1c8f283eb9398-ods." + df[le]["中台英文名称"]
                v["tar_position"] = f"{k}"
                src_selectedRowKeys.append(v["src_key"])
                tar_selectedRowKeys.append(v["tar_key"])
            src_fieldsMsg = []
            tar_fieldsMsg = []
            for j in src_columns:
                temp = {}
                temp["isAdd"] = j["isAdd"]
                temp["isLeaf"] = True
                temp["isPrimaryKey"] = j["isPrimaryKey"]
                temp["key"] = j["src_key"]
                temp["name"] = j["columnName"]
                temp["parent"] = j["src_parent"]
                temp["position"] = str(int(j["src_position"]) + 1)
                temp["type"] = j["columnType"]
                src_fieldsMsg.append(temp)
            for jj in zip(src_columns, r1):
                temp = {}
                temp["isAdd"] = jj[0]["isAdd"]
                temp["isLeaf"] = True
                temp["isPrimaryKey"] = jj[0]["isPrimaryKey"]
                temp["key"] = jj[0]["tar_key"]
                temp["name"] = jj[0]["columnName"]
                temp["parent"] = jj[0]["tar_parent"]
                temp["position"] = str(int(jj[0]["tar_position"]) + 1)
                temp["type"] = jj[1]["columnType"]
                tar_fieldsMsg.append(temp)
            FieldMap = []
            for m, n in enumerate(zip(src_columns, r1)):
                t1 = {}
                t2 = {}
                t3 = {}
                t2["constantValue"] = ""
                t2["iconShow"] = "inherit"
                t2["isConstant"] = ""
                t2["isPartitionField"] = ""
                t2["isPrimaryKey"] = n[0]["isPrimaryKey"]
                t2["key"] = f"field{m + 1}"
                t2["name"] = n[0]["columnName"]
                t2["path"] = ""
                t2["type"] = n[0]["columnType"]
                t2["position"] = f"{m + 1}"
                t2["x"] = 337
                t2["y"] = 53 + m * 36
                t3["iconShow"] = "hidden"
                t3["isPrimaryKey"] = n[1]["isPrimaryKey"]
                t3["key"] = f"field{m + 1}"
                t3["name"] = n[1]["columnName"]
                t3["position"] = f"{m + 1}"
                t3["type"] = n[1]["columnType"]
                t1["source"] = t2
                t1["target"] = t3
                FieldMap.append(t1)
            # tar_selectedRowKeys, tar_fieldsMsg
            result = save_dag(src_fieldsMsg=src_fieldsMsg,
                              src_selectedRowKeys=src_selectedRowKeys,
                              FieldMap=FieldMap,
                              tar_fieldsMsg=tar_fieldsMsg,
                              tar_selectedRowKeys=tar_selectedRowKeys,
                              tableComment=df[le]["中台表中文名称、中台数据流名称"],
                              source_name=df[le]["数据源名称"],
                              tableName=df[le]["table_schema"] + "." + df[le]["前置库英文名称"],
                              tableSchema=df[le]["table_schema"],
                              # dataBaseMsg=all_dataBaseMsg[f"{dagName}"],
                              dataBaseMsg=src_dataBaseMsg,
                              ods_name="ods." + df[le]["中台英文名称"],
                              ds=df[le]["分区"],
                              tar_db_type=df[le]["target_db_type"],
                              sourceName="DORIS_平台数仓",
                              writeMode="insert",
                              tar_dataBaseMsg=tar_dataBaseMsg,
                              Authorization=token
                              )
            all_result.append({"执行结果": result["message"]+f"数据流名称为：{df[le]['中台表中文名称、中台数据流名称']}"})

        else:
            tar_dataBaseMsg = get_database_type(df[le]["dbType"], df[le]["数据源名称"], Authorization=token)
            custom_create_table_status = custom_create_table(tarDbType="postgresql", Authorization=token)
            assert custom_create_table_status["code"] == 200
            src_fieldsMsg = []
            tar_fieldsMsg = []
            FieldMap = []
            src_selectedRowKeys = []
            for k, v in enumerate(src_columns):
                v["src_key"] = src_dataBaseMsg["id"] + "-" + df[le]["table_schema"] + "." + df[
                    le]["前置库英文名称"] + "-" + v["columnName"]
                # v["src_key"] = "ff90525f66c24b3a9844bd80765cbb2d-public.npcs_ods_fac-" + v["columnName"]
                v["src_parent"] = src_dataBaseMsg["id"] + "-" + df[le]["table_schema"] + "." + df[le]["前置库英文名称"]
                # v["src_parent"] = "ff90525f66c24b3a9844bd80765cbb2d-public.npcs_ods_fac"
                v["src_position"] = f"{k}"
                v["tar_key"] = "1ecdd2a14b844e2ab1f1c8f283eb9398" + "-" + "ods." + df[le]["中台英文名称"] + "-" + v[
                    "columnName"]
                v["tar_parent"] = "1ecdd2a14b844e2ab1f1c8f283eb9398-ods." + df[le]["中台英文名称"]
                v["tar_position"] = f"{k}"
                src_selectedRowKeys.append(v["src_key"])
            for j in src_columns:
                temp = {}
                temp["isAdd"] = j["isAdd"]
                temp["isLeaf"] = True
                temp["isPrimaryKey"] = j["isPrimaryKey"]
                temp["key"] = j["src_key"]
                temp["name"] = j["columnName"]
                temp["parent"] = j["src_parent"]
                temp["position"] = str(int(j["src_position"]) + 1)
                temp["type"] = j["columnType"]
                src_fieldsMsg.append(temp)
            for m, n in enumerate(zip(src_columns, src_columns)):
                t1 = {}
                t2 = {}
                t3 = {}
                t2["constantValue"] = ""
                t2["iconShow"] = "inherit"
                t2["isConstant"] = ""
                t2["isPartitionField"] = ""
                t2["isPrimaryKey"] = n[0]["isPrimaryKey"]
                t2["key"] = f"field{m + 1}"
                t2["name"] = n[0]["columnName"]
                t2["path"] = ""
                t2["type"] = n[0]["columnType"]
                t2["position"] = f"{m + 1}"
                t2["x"] = 337
                t2["y"] = 53 + m * 36
                t3["iconShow"] = "inherit"
                t3["isPrimaryKey"] = n[1]["isPrimaryKey"]
                t3["key"] = f"field{m + 1}"
                t3["name"] = n[1]["columnName"]
                t3["position"] = f"{m + 1}"
                t3["type"] = n[1]["columnType"]
                t1["source"] = t2
                t1["target"] = t3
                FieldMap.append(t1)

            result = save_dag(src_fieldsMsg=src_fieldsMsg,
                              src_selectedRowKeys=src_selectedRowKeys,
                              FieldMap=FieldMap,
                              tar_fieldsMsg=src_fieldsMsg,
                              tar_selectedRowKeys=src_selectedRowKeys,
                              tableComment=df[le]["中台表中文名称、中台数据流名称"],
                              source_name=df[le]["数据源名称"],
                              tableName=df[le]["table_schema"] + "." + df[le]["前置库英文名称"],
                              tableSchema=df[le]["table_schema"],
                              # dataBaseMsg=all_dataBaseMsg[f"{dagName}"],
                              dataBaseMsg=src_dataBaseMsg,
                              ods_name=df[le]["target_table_schema"] + "." + df[le]["target_db_name"],
                              ds=df[le]["分区"],
                              tar_db_type=df[le]["target_db_type"],
                              sourceName=df[le]["数据源名称"],
                              writeMode="insert",
                              tar_dataBaseMsg=src_dataBaseMsg,
                              targetCleanSql="truncate table " + df[le]["table_schema"] + "." + df[le]["target_db_name"],
                              Authorization=token
                              )
            all_result.append({"执行结果": result["message"]+f"数据流名称为：{df[le]['中台表中文名称、中台数据流名称']}"})

    return all_result