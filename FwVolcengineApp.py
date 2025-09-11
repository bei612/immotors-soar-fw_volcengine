from __future__ import print_function
import volcenginesdkcore
import volcenginesdkfwcenter
from volcenginesdkcore.rest import ApiException
from loguru import logger  # 导入日记库，没有请先安装 pip install loguru
import os
import ipaddress
import random
import string
import ast

class FwVolcengineApp:
    def __init__(self, ak, sk, endpoint, region, proxies):
        self.ak = ak
        self.sk = sk
        self.endpoint = endpoint
        self.region = region
        self.proxies = proxies

        # 获取配置
        configuration = volcenginesdkcore.Configuration()
        configuration.ak = self.ak
        configuration.sk = self.sk
        configuration.region = self.region
        configuration.client_side_validation = True
        configuration.host = self.endpoint
        configuration.proxy = self.proxies
        # 设置配置
        volcenginesdkcore.Configuration.set_default(configuration)
        # 获取防火墙 api 句柄
        self.api_instance = volcenginesdkfwcenter.FWCENTERApi()
        pass

    def check_address_type(self, addresslist):
        try:
            ipaddress.IPv4Address(addresslist)
            return "ipv4"
        except ipaddress.AddressValueError:
            pass
        try:
            ipaddress.IPv6Address(addresslist)
            return "ipv6"
        except ipaddress.AddressValueError:
            pass
        try:
            ipaddress.IPv4Network(addresslist)
            return "network"
        except (ipaddress.AddressValueError, ipaddress.NetmaskValueError):
            pass
        return "domain"

    def check_os_type(self):
        sysname = os.uname().sysname
        logs_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__))) + "/logs"
        file_name = os.path.splitext(os.path.basename(__file__))[0].lower()
        logger.remove()
        if sysname == "Darwin":
            logger.add(f'{logs_path}/{file_name}_run.log', rotation='00:00', encoding='utf-8', enqueue=True,
                       retention="30 days")
        elif sysname == "Linux":
            logger.add(f'{os.path.dirname(__file__)}/run.log', rotation='500MB')

    def add_address_book(self, groupname, grouptype, description, addresslist):
        random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=6))
        if type(addresslist) != list:
            addresslist = ast.literal_eval(addresslist)
        else:
            addresslist = addresslist
        request = volcenginesdkfwcenter.AddAddressBookRequest(
            description = f"{description}-{random_string}",
            group_name = f"{groupname}-{random_string}",
            group_type = grouptype,
            address_list = addresslist,
        )
        data = dict()
        # 定义日志配置
        self.check_os_type()
        try:
            # 发送请求
            #resp = self.api_instance.add_address_book(request).to_dict()
            resp, status, headers = self.api_instance.add_address_book_with_http_info(request, _return_http_data_only=False)
            data = resp.to_dict()
            data["status_code"] = status
            data["msg"] = "创建成功"
            logger.info(f'{data}')
            msg = {
                "groupname": f"{groupname}-{random_string}",
                "description": f"{description}-{random_string}",
                "groupuuid": data['group_uuid'],
                "grouptype": f"{grouptype}",
                "desc": "创建成功"
            }
            return msg
        except ApiException as e:
            data["status_code"] = e.status
            data["err_msg"] = e.__str__()
            logger.error(f'{data}')
            msg = {
                "groupname": f"{groupname}-{random_string}",
                "description": f"{description}-{random_string}",
                "grouptype": f"{grouptype}",
                "desc": "创建失败"
            }
            return msg

    def delete_address_book(self, groupuuid):
        request = volcenginesdkfwcenter.DeleteAddressBookRequest(
            group_uuid = groupuuid,
        )
        data = dict()
        # 定义日志配置
        self.check_os_type()
        try:
            # 发送请求
            resp, status, headers = self.api_instance.delete_address_book_with_http_info(request, _return_http_data_only=False)
            data["status_code"] = status
            data["msg"] = "删除成功"
            logger.info(f'{data}')
            return data
        except ApiException as e:
            data["status_code"] = e.status
            data["err_msg"] = e.__str__()
            logger.error(f'{data}')
            return data

    def describe_address_book(self, query, grouptype):
        request = volcenginesdkfwcenter.DescribeAddressBookRequest(
            group_type = grouptype,
            query = query,
            page_number = 1,
            page_size = 200,
        )
        data = dict()
        # 定义日志配置
        self.check_os_type()
        try:
            # 发送请求
            resp, status, headers = self.api_instance.describe_address_book_with_http_info(request, _return_http_data_only=False)
            data = resp.to_dict()
            data["status_code"] = status
            data["msg"] = "查询成功"
            logger.info(f'{data}')
            return data
        except ApiException as e:
            data["status_code"] = e.status
            data["err_msg"] = e.__str__()
            logger.error(f'{data}')
            return data

    def modify_address_book(self, groupname, groupuuid, description, addresslist):
        if type(addresslist) != list:
            addresslist = ast.literal_eval(addresslist)
        else:
            addresslist = addresslist
        request = volcenginesdkfwcenter.ModifyAddressBookRequest(
            address_list = addresslist,
            description = description,
            group_name = groupname,
            group_uuid = groupuuid,
        )
        data = dict()
        # 定义日志配置
        self.check_os_type()
        try:
            # 发送请求
            resp, status, headers = self.api_instance.modify_address_book_with_http_info(request, _return_http_data_only=False)
            data["status_code"] = status
            data["msg"] = "修改成功"
            logger.info(f'{data}')
            return data
        except ApiException as e:
            data["status_code"] = e.status
            data["err_msg"] = e.__str__()
            logger.error(f'{data}')
            return data

    def add_control_policy(self, prio, direction, sourcetype, source, sourcetypegrouptype, destinationtype, destination, destinationgrouptype, proto, action, description):
        prio = int(prio)
        request = volcenginesdkfwcenter.AddControlPolicyRequest(
            prio = prio,
            direction = direction,
            source_type = sourcetype,
            source = source,
            source_group_type = sourcetypegrouptype,
            destination_type = destinationtype,
            destination = destination,
            destination_group_type = destinationgrouptype,
            proto = proto,
            action = action,
            description = description,
            status = True,
        )
        data = dict()
        # 定义日志配置
        self.check_os_type()
        try:
            # 发送请求
            resp, status, headers = self.api_instance.add_control_policy_with_http_info(request, _return_http_data_only=False)
            data = resp.to_dict()
            data["status_code"] = status
            data["msg"] = "创建成功"
            logger.info(f'{data}')
            msg = {
                "groupname": f"{description}",
                "description": f"{description}",
                "ruleid": data['rule_id'],
                "desc": "创建成功"
            }
            return msg
        except ApiException as e:
            data["status_code"] = e.status
            data["err_msg"] = e.__str__()
            logger.error(f'{data}')
            msg = {
                "groupname": f"{description}",
                "description": f"{description}",
                "desc": "创建失败"
            }
            return msg

    def delete_control_policy(self, ruleid, direction):
        request = volcenginesdkfwcenter.DeleteControlPolicyRequest(
            rule_id = ruleid,
            direction = direction,
        )
        data = dict()
        # 定义日志配置
        self.check_os_type()
        try:
            # 发送请求
            resp, status, headers = self.api_instance.delete_control_policy_with_http_info(request, _return_http_data_only=False)
            data["status_code"] = status
            data["msg"] = "删除成功"
            logger.info(f'{data}')
            return data
        except ApiException as e:
            data["status_code"] = e.status
            data["err_msg"] = e.__str__()
            logger.error(f'{data}')
            return data

    def describe_control_policy(self, direction):
        request = volcenginesdkfwcenter.DescribeControlPolicyRequest(
            direction = direction,
        )
        data = dict()
        # 定义日志配置
        self.check_os_type()
        try:
            # 发送请求
            resp, status, headers = self.api_instance.describe_control_policy_with_http_info(request, _return_http_data_only=False)
            data = resp.to_dict()
            data["status_code"] = status
            data["msg"] = "查询成功"
            logger.info(f'{data}')
            return data
        except ApiException as e:
            data["status_code"] = e.status
            data["err_msg"] = e.__str__()
            logger.error(f'{data}')
            return data

    def modify_control_policy(self, ruleid, direction, sourcetype, source, sourcetypegrouptype, destinationtype, destination, destinationgrouptype, proto, action, description):
        request = volcenginesdkfwcenter.ModifyControlPolicyRequest(
            rule_id = ruleid,
            direction = direction,
            source_type = sourcetype,
            source = source,
            source_group_type = sourcetypegrouptype,
            destination_type = destinationtype,
            destination = destination,
            destination_group_type = destinationgrouptype,
            proto = proto,
            action = action,
            description = description,
        )
        data = dict()
        # 定义日志配置
        self.check_os_type()
        try:
            # 发送请求
            resp, status, headers = self.api_instance.modify_control_policy_with_http_info(request, _return_http_data_only=False)
            data["status_code"] = status
            data["msg"] = "修改成功"
            logger.info(f'{data}')
            return data
        except ApiException as e:
            data["status_code"] = e.status
            data["err_msg"] = e.__str__()
            logger.error(f'{data}')
            return data

    def single_block_address(self, addr, direction=None):
        # 定义日志配置
        self.check_os_type()
        # 判断需要解封的地址资源属于ip类型或net类型或domain类型
        if self.check_address_type(addr) == "ipv4":
            addr_obj = f"{addr}/32"
            describe_address_book_grouptype = "ip"
        elif self.check_address_type(addr) == "network":
            addr_obj = f"{addr}"
            describe_address_book_grouptype = "ip"
        elif self.check_address_type(addr) == "domain":
            addr_obj = f"{addr}"
            describe_address_book_grouptype = "domain"
        elif self.check_address_type(addr) == "ipv6":
            msg = {
                "addr": f"{addr}",
                "desc": "无需封禁"
            }
            logger.info(f'{msg}')
            return msg
        # 查询所有地址资源组
        res_describe_address_book = self.describe_address_book(
            query = "P-Deny-Secops-Blacklist",
            grouptype = describe_address_book_grouptype
        )
        # 判断需要封禁的地址资源是否已存在于现有封禁策略中，如存在则found赋值为True，如不存在则found赋值为False
        if direction == 'in':
            found = False
            for list_res_describe_address_book in res_describe_address_book['data']:
                if "P-Deny-Secops-Blacklist-In" in list_res_describe_address_book['group_name']:
                    match_list = list_res_describe_address_book['address_list']
                    if addr_obj in match_list:
                        found = True
                        break
        elif direction == 'out':
            found = False
            for list_res_describe_address_book in res_describe_address_book['data']:
                if "P-Deny-Secops-Blacklist-Out" in list_res_describe_address_book['group_name']:
                    match_list = list_res_describe_address_book['address_list']
                    if addr_obj in match_list:
                        found = True
                        break
        # 判断需要封禁的地址资源是否执行封禁任务，如found值为True，返回msg无需封禁
        if found == True:
            msg = {
                "addr": f"{addr}",
                "desc": "无需封禁"
            }
            logger.info(f'{msg}')
            return msg
        # 判断需要封禁的地址资源是否执行封禁任务，如found值为False，执行封禁任务序列
        elif found == False:
            if direction == 'in':
                # 将包含P-Deny-Secops-Blacklist-In的地址资源组汇总成addrgrps地址资源组列表
                addrgrps = [data_describe_address_book for data_describe_address_book in res_describe_address_book['data']
                            if 'P-Deny-Secops-Blacklist-In' in data_describe_address_book['group_name']]
                for addrgrp in addrgrps:
                    # 将汇总的addrgrps中的addrgrp内的address_list从字符串转换成列表，并计算长度
                    len_addrgrp = len(ast.literal_eval(addrgrp['address_list']))
                    data_len_addrgrp = len_addrgrp + 1
                    # 判断需要封禁的地址资源组内的地址资源是否超过2000最大上限，如不超过2000地址资源，将需要封禁的地址资源更新至地址资源组
                    if len_addrgrp < 2000:
                    # if len_addrgrp < 1:
                        addrgrp_groupname = addrgrp['group_name']
                        addrgrp_groupuuid = addrgrp['group_uuid']
                        addrgrp_description = addrgrp['description']
                        list_addrgrp_addresslist = ast.literal_eval(addrgrp['address_list'])
                        list_addrgrp_addresslist.append(addr_obj)
                        # data_addrgrp_addresslist = ','.join(list_addrgrp_addresslist)
                        # print(data_addrgrp_addresslist, flush=True)
                        # 将需要封禁的地址资源更新至地址资源组
                        res_modify_address_book = self.modify_address_book(
                            groupname = addrgrp_groupname,
                            groupuuid = addrgrp_groupuuid,
                            description = addrgrp_description,
                            addresslist = list_addrgrp_addresslist
                        )
                        if res_modify_address_book['status_code'] == 200:
                            msg = {
                                "addr": f"{addr}",
                                "groupname": f"{addrgrp_groupname}",
                                "groupuuid": f"{addrgrp_groupuuid}",
                                "description": f"{addrgrp_description}",
                                "grouplen": f"{data_len_addrgrp}",
                                "desc": "封禁成功"
                            }
                            logger.info(f'{msg}')
                            return msg
                        else:
                            msg = {
                                "addr": f"{addr}",
                                "groupname": f"{addrgrp_groupname}",
                                "groupuuid": f"{addrgrp_groupuuid}",
                                "description": f"{addrgrp_description}",
                                "grouplen": f"{data_len_addrgrp}",
                                "desc": "封禁失败"
                            }
                            logger.info(f'{msg}')
                            return msg
                    # 判断需要封禁的地址资源组内的地址资源是否超过2000最大上限，如超过2000地址资源，新建控制策略组，新建地址资源组，将需要封禁的地址资源更新至新建的地址资源组
                    else:
                        # 将地址资源对象str转换成list
                        list_addr_obj = []
                        list_addr_obj.append(addr_obj)
                        # 创建地址资源组，将需要封禁的地址资源更新至新至新建的地址资源组
                        res_add_address_book = self.add_address_book(
                            groupname = "P-Deny-Secops-Blacklist-In",
                            grouptype = describe_address_book_grouptype,
                            description = "P-Deny-Secops-Blacklist-In",
                            addresslist = list_addr_obj
                        )
                        res_add_address_book_groupname = res_add_address_book['groupname']
                        res_add_address_book_description = res_add_address_book['description']
                        res_add_address_book_groupuuid = res_add_address_book['groupuuid']
                        # 创建控制策略组，将需要封禁的地址资源组更新至新建的控制策略组
                        res_add_control_policy = self.add_control_policy(
                            prio = 2,
                            direction = direction,
                            sourcetype = "group",
                            source = res_add_address_book_groupuuid,
                            sourcetypegrouptype = describe_address_book_grouptype,
                            destinationtype = "net",
                            destination = "0.0.0.0/0",
                            destinationgrouptype = None,
                            proto = "ANY",
                            action = "deny",
                            description = "P-Deny-Secops-Blacklist-In"
                        )
                        if res_add_address_book['desc'] == "创建成功" and res_add_control_policy['desc'] == "创建成功":
                            msg = {
                                "addr": f"{addr}",
                                "groupname": f"{res_add_address_book_groupname}",
                                "groupuuid": f"{res_add_address_book_groupuuid}",
                                "description": f"{res_add_address_book_description}",
                                "grouplen": 1,
                                "desc": "封禁成功"
                            }
                            logger.info(f'{msg}')
                            return msg
                        else:
                            msg = {
                                "addr": f"{addr}",
                                "groupname": f"{res_add_address_book_groupname}",
                                "groupuuid": f"{res_add_address_book_groupuuid}",
                                "description": f"{res_add_address_book_description}",
                                "grouplen": 1,
                                "desc": "封禁失败"
                            }
                            logger.info(f'{msg}')
                            return msg
            elif direction == 'out':
                # 将包含P-Deny-Secops-Blacklist-Out的地址资源组汇总成addrgrps地址资源组列表
                addrgrps = [data_describe_address_book for data_describe_address_book in res_describe_address_book['data']
                            if 'P-Deny-Secops-Blacklist-Out' in data_describe_address_book['group_name']]
                for addrgrp in addrgrps:
                    # 将汇总的addrgrps中的addrgrp内的address_list从字符串转换成列表，并计算长度
                    len_addrgrp = len(ast.literal_eval(addrgrp['address_list']))
                    data_len_addrgrp = len_addrgrp + 1
                    # 判断需要封禁的地址资源组内的地址资源是否超过2000最大上限，如不超过2000地址资源，将需要封禁的地址资源更新至地址资源组
                    if len_addrgrp < 2000:
                    # if len_addrgrp < 1:
                        addrgrp_groupname = addrgrp['group_name']
                        addrgrp_groupuuid = addrgrp['group_uuid']
                        addrgrp_description = addrgrp['description']
                        list_addrgrp_addresslist = ast.literal_eval(addrgrp['address_list'])
                        list_addrgrp_addresslist.append(addr_obj)
                        # 将需要封禁的地址资源更新至地址资源组
                        res_modify_address_book = self.modify_address_book(
                            groupname=addrgrp_groupname,
                            groupuuid=addrgrp_groupuuid,
                            description=addrgrp_description,
                            addresslist=list_addrgrp_addresslist
                        )
                        if res_modify_address_book['status_code'] == 200:
                            msg = {
                                "addr": f"{addr}",
                                "groupname": f"{addrgrp_groupname}",
                                "groupuuid": f"{addrgrp_groupuuid}",
                                "description": f"{addrgrp_description}",
                                "grouplen": f"{data_len_addrgrp}",
                                "desc": "封禁成功"
                            }
                            logger.info(f'{msg}')
                            return msg
                        else:
                            msg = {
                                "addr": f"{addr}",
                                "groupname": f"{addrgrp_groupname}",
                                "groupuuid": f"{addrgrp_groupuuid}",
                                "description": f"{addrgrp_description}",
                                "grouplen": f"{data_len_addrgrp}",
                                "desc": "封禁失败"
                            }
                            logger.info(f'{msg}')
                            return msg
                    # 判断需要封禁的地址资源组内的地址资源是否超过2000最大上限，如超过2000地址资源，新建控制策略组，新建地址资源组，将需要封禁的地址资源更新至新建的地址资源组
                    else:
                        # 将地址资源对象str转换成list
                        list_addr_obj = []
                        list_addr_obj.append(addr_obj)
                        # 创建地址资源组，将需要封禁的地址资源更新至新至新建的地址资源组
                        res_add_address_book = self.add_address_book(
                            groupname="P-Deny-Secops-Blacklist-Out",
                            grouptype=describe_address_book_grouptype,
                            description="P-Deny-Secops-Blacklist-Out",
                            addresslist=list_addr_obj
                        )
                        res_add_address_book_groupname = res_add_address_book['groupname']
                        res_add_address_book_description = res_add_address_book['description']
                        res_add_address_book_groupuuid = res_add_address_book['groupuuid']
                        # 创建控制策略组，将需要封禁的地址资源组更新至新建的控制策略组
                        res_add_control_policy = self.add_control_policy(
                            prio=2,
                            direction=direction,
                            sourcetype="net",
                            source="0.0.0.0/0",
                            sourcetypegrouptype=None,
                            destinationtype="group",
                            destination=res_add_address_book_groupuuid,
                            destinationgrouptype=describe_address_book_grouptype,
                            proto="ANY",
                            action="deny",
                            description="P-Deny-Secops-Blacklist-Out",
                        )
                        if res_add_address_book['desc'] == "创建成功" and res_add_control_policy['desc'] == "创建成功":
                            msg = {
                                "addr": f"{addr}",
                                "groupname": f"{res_add_address_book_groupname}",
                                "groupuuid": f"{res_add_address_book_groupuuid}",
                                "description": f"{res_add_address_book_description}",
                                "grouplen": 1,
                                "desc": "封禁成功"
                            }
                            logger.info(f'{msg}')
                            return msg
                        else:
                            msg = {
                                "addr": f"{addr}",
                                "groupname": f"{res_add_address_book_groupname}",
                                "groupuuid": f"{res_add_address_book_groupuuid}",
                                "description": f"{res_add_address_book_description}",
                                "grouplen": 1,
                                "desc": "封禁失败"
                            }
                            logger.info(f'{msg}')
                            return msg

    def single_unblock_address(self, addr, direction=None):
        # 定义日志配置
        self.check_os_type()
        # 判断需要解封的地址资源属于ip类型或net类型或domain类型
        if self.check_address_type(addr) == "ipv4":
            addr_obj = f"{addr}/32"
            describe_address_book_grouptype = "ip"
        elif self.check_address_type(addr) == "network":
            addr_obj = f"{addr}"
            describe_address_book_grouptype = "ip"
        elif self.check_address_type(addr) == "domain":
            addr_obj = f"{addr}"
            describe_address_book_grouptype = "domain"
        elif self.check_address_type(addr) == "ipv6":
            msg = {
                "addr": f"{addr}",
                "desc": "无需解封"
            }
            logger.info(f'{msg}')
            return msg
        # 查询所有地址资源组
        res_describe_address_book = self.describe_address_book(
            query = "P-Deny-Secops-Blacklist",
            grouptype = describe_address_book_grouptype
        )
        # 判断需要解封的地址资源是否已存在于现有封禁策略中，如存在则found赋值为True，如不存在则found赋值为False
        if direction == 'in':
            found = False
            for list_res_describe_address_book in res_describe_address_book['data']:
                if "P-Deny-Secops-Blacklist-In" in list_res_describe_address_book['group_name']:
                    match_list = list_res_describe_address_book['address_list']
                    if addr_obj in match_list:
                        found = True
                        break
        elif direction == 'out':
            found = False
            for list_res_describe_address_book in res_describe_address_book['data']:
                if "P-Deny-Secops-Blacklist-Out" in list_res_describe_address_book['group_name']:
                    match_list = list_res_describe_address_book['address_list']
                    if addr_obj in match_list:
                        found = True
                        break
        # 判断需要解封的地址资源是否执行解封任务，如found值为True，执行解封任务序列
        if found == True:
            if direction == 'in':
                # 将包含P-Deny-Secops-Blacklist-In且包含需解封地址对象的地址资源组汇总成addrgrps地址资源组列表
                addrgrps = [data_describe_address_book for data_describe_address_book in
                            res_describe_address_book['data']
                            if 'P-Deny-Secops-Blacklist-In' in data_describe_address_book['group_name'] and addr_obj in
                            data_describe_address_book['address_list']]
                # 构建解封return msg列表
                list_msg = []
                for addrgrp in addrgrps:
                    # 将汇总的addrgrps中的addrgrp内的address_list从字符串转换成列表，并计算长度
                    len_addrgrp = len(ast.literal_eval(addrgrp['address_list']))
                    data_len_addrgrp = len_addrgrp - 1
                    # 判断需要解封的地址资源组内的地址资源是否少于1个最小下限，如解封前大于1个地址资源，将包含解封地址资源的资源地址组从封禁策略中去除，删除地址资源
                    if len_addrgrp > 1:
                    # if len_addrgrp < 1:
                        addrgrp_groupname = addrgrp['group_name']
                        addrgrp_groupuuid = addrgrp['group_uuid']
                        addrgrp_description = addrgrp['description']
                        list_addrgrp_addresslist = ast.literal_eval(addrgrp['address_list'])
                        list_addrgrp_addresslist.remove(addr_obj)
                        # 将需要封禁的地址资源更新至地址资源组
                        res_modify_address_book = self.modify_address_book(
                            groupname = addrgrp_groupname,
                            groupuuid = addrgrp_groupuuid,
                            description = addrgrp_description,
                            addresslist = list_addrgrp_addresslist
                        )
                        if res_modify_address_book['status_code'] == 200:
                            msg = {
                                "addr": f"{addr}",
                                "groupname": f"{addrgrp_groupname}",
                                "groupuuid": f"{addrgrp_groupuuid}",
                                "description": f"{addrgrp_description}",
                                "grouplen": f"{data_len_addrgrp}",
                                "desc": "解封成功"
                            }
                            logger.info(f'{msg}')
                        else:
                            msg = {
                                "addr": f"{addr}",
                                "groupname": f"{addrgrp_groupname}",
                                "groupuuid": f"{addrgrp_groupuuid}",
                                "description": f"{addrgrp_description}",
                                "grouplen": f"{data_len_addrgrp}",
                                "desc": "解封失败"
                            }
                            logger.info(f'{msg}')
                        # 将需要解封的地址资源msg汇总至return msg列表
                        list_msg.append(msg)
                    # 判断需要解封的地址资源组内的地址资源是否少于1个最小下限，如解封前等于1个地址资源，将包含解封地址资源的控制策略组及资源地址组删除
                    else:
                        addrgrp_groupname = addrgrp['group_name']
                        addrgrp_groupuuid = addrgrp['group_uuid']
                        addrgrp_description = addrgrp['description']
                        # 查询所有控制策略组
                        res_describe_control_policy = self.describe_control_policy(
                            direction = direction
                        )
                        # 将包含P-Deny-Secops-Blacklist-In且包含需解封地址对象的策略控制组汇总成policygrp控制策略组列表，dict.values()为精确匹配
                        policygrps = [data_describe_control_policy for data_describe_control_policy in
                                      res_describe_control_policy['data'] if
                                      addrgrp_groupuuid in data_describe_control_policy.values()]
                        # 遍历控制策略列表中的控制策略
                        for policygrp in policygrps:
                            policygrp_ruleid = policygrp['rule_id']
                            # 删除包含需解封地址资源的策略控制组
                            res_delete_control_policy = self.delete_control_policy(
                                ruleid = policygrp_ruleid,
                                direction = direction
                            )
                            # 删除包含需解封地址资源的地址资源组
                            res_delete_address_book = self.delete_address_book(
                                groupuuid = addrgrp_groupuuid
                            )
                            if res_delete_control_policy['status_code'] == 200 and res_delete_address_book['status_code'] == 200:
                                msg = {
                                    "addr": f"{addr}",
                                    "groupname": f"{addrgrp_groupname}",
                                    "groupuuid": f"{addrgrp_groupuuid}",
                                    "description": f"{addrgrp_description}",
                                    "ruleid": f"{policygrp_ruleid}",
                                    "desc": "解封成功"
                                }
                                logger.info(f'{msg}')
                            else:
                                msg = {
                                    "addr": f"{addr}",
                                    "groupname": f"{addrgrp_groupname}",
                                    "groupuuid": f"{addrgrp_groupuuid}",
                                    "description": f"{addrgrp_description}",
                                    "ruleid": f"{policygrp_ruleid}",
                                    "desc": "解封失败"
                                }
                                logger.info(f'{msg}')
                            # 将需要解封的地址资源msg汇总至return msg列表
                            list_msg.append(msg)
                logger.info(f'{list_msg}')
                return list_msg
            elif direction == 'out':
                # 将包含P-Deny-Secops-Blacklist-Out且包含需解封地址对象的地址资源组汇总成addrgrps地址资源组列表
                addrgrps = [data_describe_address_book for data_describe_address_book in
                            res_describe_address_book['data']
                            if 'P-Deny-Secops-Blacklist-Out' in data_describe_address_book['group_name'] and addr_obj in
                            data_describe_address_book['address_list']]
                # 构建解封return msg列表
                list_msg = []
                for addrgrp in addrgrps:
                    # 将汇总的addrgrps中的addrgrp内的address_list从字符串转换成列表，并计算长度
                    len_addrgrp = len(ast.literal_eval(addrgrp['address_list']))
                    data_len_addrgrp = len_addrgrp - 1
                    # 判断需要解封的地址资源组内的地址资源是否少于1个最小下限，如解封前大于1个地址资源，将包含解封地址资源的资源地址组从封禁策略中去除，删除地址资源
                    if len_addrgrp > 1:
                    # if len_addrgrp < 1:
                        addrgrp_groupname = addrgrp['group_name']
                        addrgrp_groupuuid = addrgrp['group_uuid']
                        addrgrp_description = addrgrp['description']
                        list_addrgrp_addresslist = ast.literal_eval(addrgrp['address_list'])
                        list_addrgrp_addresslist.remove(addr_obj)
                        # 将需要封禁的地址资源更新至地址资源组
                        res_modify_address_book = self.modify_address_book(
                            groupname=addrgrp_groupname,
                            groupuuid=addrgrp_groupuuid,
                            description=addrgrp_description,
                            addresslist=list_addrgrp_addresslist
                        )
                        if res_modify_address_book['status_code'] == 200:
                            msg = {
                                "addr": f"{addr}",
                                "groupname": f"{addrgrp_groupname}",
                                "groupuuid": f"{addrgrp_groupuuid}",
                                "description": f"{addrgrp_description}",
                                "grouplen": f"{data_len_addrgrp}",
                                "desc": "解封成功"
                            }
                            logger.info(f'{msg}')
                        else:
                            msg = {
                                "addr": f"{addr}",
                                "groupname": f"{addrgrp_groupname}",
                                "groupuuid": f"{addrgrp_groupuuid}",
                                "description": f"{addrgrp_description}",
                                "grouplen": f"{data_len_addrgrp}",
                                "desc": "解封失败"
                            }
                            logger.info(f'{msg}')
                        # 将需要解封的地址资源msg汇总至return msg列表
                        list_msg.append(msg)
                    # 判断需要解封的地址资源组内的地址资源是否少于1个最小下限，如解封前等于1个地址资源，将包含解封地址资源的控制策略组及资源地址组删除
                    else:
                        addrgrp_groupname = addrgrp['group_name']
                        addrgrp_groupuuid = addrgrp['group_uuid']
                        addrgrp_description = addrgrp['description']
                        # 查询所有控制策略组
                        res_describe_control_policy = self.describe_control_policy(
                            direction=direction
                        )
                        # 将包含P-Deny-Secops-Blacklist-In且包含需解封地址对象的策略控制组汇总成policygrp控制策略组列表，dict.values()为精确匹配
                        policygrps = [data_describe_control_policy for data_describe_control_policy in
                                      res_describe_control_policy['data'] if
                                      addrgrp_groupuuid in data_describe_control_policy.values()]
                        # 遍历控制策略列表中的控制策略
                        for policygrp in policygrps:
                            policygrp_ruleid = policygrp['rule_id']
                            # 删除包含需解封地址资源的策略控制组
                            res_delete_control_policy = self.delete_control_policy(
                                ruleid=policygrp_ruleid,
                                direction=direction
                            )
                            # 删除包含需解封地址资源的地址资源组
                            res_delete_address_book = self.delete_address_book(
                                groupuuid=addrgrp_groupuuid
                            )
                            if res_delete_control_policy['status_code'] == 200 and res_delete_address_book[
                                'status_code'] == 200:
                                msg = {
                                    "addr": f"{addr}",
                                    "groupname": f"{addrgrp_groupname}",
                                    "groupuuid": f"{addrgrp_groupuuid}",
                                    "description": f"{addrgrp_description}",
                                    "ruleid": f"{policygrp_ruleid}",
                                    "desc": "解封成功"
                                }
                                logger.info(f'{msg}')
                            else:
                                msg = {
                                    "addr": f"{addr}",
                                    "groupname": f"{addrgrp_groupname}",
                                    "groupuuid": f"{addrgrp_groupuuid}",
                                    "description": f"{addrgrp_description}",
                                    "ruleid": f"{policygrp_ruleid}",
                                    "desc": "解封失败"
                                }
                                logger.info(f'{msg}')
                            # 将需要解封的地址资源msg汇总至return msg列表
                            list_msg.append(msg)
                logger.info(f'{list_msg}')
                return list_msg
        # 判断需要解封的地址资源是否执行解封任务，如found值为False，返回msg无需解封
        elif found == False:
            msg = {
                "addr": f"{addr}",
                "desc": "无需解封"
            }
            return msg