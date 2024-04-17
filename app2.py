from aliyun.log.logclient import LogClient
from datetime import datetime
import time
from aliyun.log import PutLogsRequest, LogItem, GetLogsRequest, IndexConfig, LogGroup
import os

# 配置你的阿里云访问密钥和端点
accessKeyId = ''
accessKey = ''
endpoint = 'cn-huhehaote.log.aliyuncs.com'  # 使用内蒙古（呼和浩特）地区的endpoint

# 创建日志服务Client
client = LogClient(endpoint, accessKeyId, accessKey)

# 配置项目和日志库名称
project_name = 'userbehavioranalysis'
logstore_name = 'textinputlogs'

query = "*| select createtime,content from " + logstore_name


# 索引配置
logstore_index = {
    'line': {
        'token': [',', ' ', "'", '"', ';', '=', '(', ')', '[', ']', '{', '}', '?', '@', '&', '<', '>', '/', ':', '\n', '\t', '\r'],
        'caseSensitive': False, 'chn': False
    },
    'keys': {
        'createtime': {
            'type': 'text',
            'token': [',', ' ', "'", '"', ';', '=', '(', ')', '[', ']', '{', '}', '?', '@', '&', '<', '>', '/', ':', '\n', '\t', '\r'],
            'caseSensitive': False, 'alias': '', 'doc_value': True, 'chn': False
        },
        'content': {
            'type': 'text',
            'token': [',', ' ', "'", '"', ';', '=', '(', ')', '[', ']', '{', '}', '?', '@', '&', '<', '>', '/', ':', '\n', '\t', '\r'],
            'caseSensitive': False, 'alias': '', 'doc_value': True, 'chn': False
        }
    },
    'log_reduce': False,
    'max_text_len': 2048
}

# 时间范围
from_time = int(time.time()) - 3600
to_time = int(time.time()) + 3600

def create_index():
    print("Ready to create index for %s" % logstore_name)
    index_config = IndexConfig()
    index_config.from_json(logstore_index)
    try:
        client.create_index(project_name, logstore_name, index_config)
        print("Create index for %s success" % logstore_name)
    except Exception as e:
        print("Failed to create index: %s" % str(e))

def put_logs():
    print("ready to put logs for %s" % logstore_name)
    log_group = []
    for i in range(0, 100):
        log_item = LogItem()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 当前时间的字符串格式
        contents = [
            ('createtime', now),                                  # 添加时间戳字段
            ('content', 'content %d' % i)              # 添加内容字段，包含i值的示例文本
        ]
        log_item.set_contents(contents)
        log_group.append(log_item)
    request = PutLogsRequest(project_name, logstore_name, "", "", log_group, compress=False)
    response = client.put_logs(request)  # 确保捕获并处理函数调用的响应
    print("Put logs for %s success, request ID: %s" % (logstore_name, response.get_request_id()))


# 通过SQL查询日志。
def get_logs():
    print("ready to query logs from logstore %s" % logstore_name)
    request = GetLogsRequest(project_name, logstore_name, from_time, to_time, query=query)
    response = client.get_logs(request)
    for log in response.get_logs():
        for k, v in log.contents.items():
            print("%s : %s" % (k, v))
        print("*********************")

if __name__ == '__main__':
    create_index()
    put_logs()
    get_logs()
