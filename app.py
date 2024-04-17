from aliyun.log.logclient import LogClient
import time
from datetime import datetime  # 正确导入datetime类
from aliyun.log import LogClient, PutLogsRequest, LogItem, GetLogsRequest, IndexConfig

# 配置你的阿里云访问密钥和端点
accessKeyId = ''
accessKey = ''
endpoint = 'cn-huhehaote.log.aliyuncs.com'  # 使用内蒙古（呼和浩特）地区的endpoint


# 创建日志服务Client。 
client = LogClient(endpoint, accessKeyId, accessKey)

# 配置你的项目和日志库名称
project_name = 'userbehavioranalysis'
logstore_name = 'textinputlogs'

# 查询语句。
query = "*| select createtime,content from " + logstore_name
# 索引。
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


# from_time和to_time表示查询日志的时间范围，Unix时间戳格式。
from_time = int(time.time()) - 3600
to_time = time.time() + 3600

# 创建Project。
def create_project():
    print("ready to create project %s" % project_name)
    client.create_project(project_name, project_des="")
    print("create project %s success " % project_name)
    # time.sleep(60)

# 创建Logstore。
def create_logstore():
    print("ready to create logstore %s" % logstore_name)
    client.create_logstore(project_name, logstore_name, ttl=3, shard_count=2)
    print("create logstore %s success " % project_name)
    # time.sleep(30)

# 创建索引。
def create_index():
    print("ready to create index for %s" % logstore_name)
    index_config = IndexConfig()
    index_config.from_json(logstore_index)
    client.create_index(project_name, logstore_name, index_config)
    print("create index for %s success " % logstore_name)
    # time.sleep(60 * 2)

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
    # 创建Project。
    create_project()
    # 创建Logstore。
    create_logstore()
    # 创建索引。
    create_index()
    # 向Logstore写入数据。
    put_logs()
    # 通过SQL查询日志。
    get_logs()