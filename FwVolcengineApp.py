# -*- coding: utf-8 -*-
from __future__ import print_function
import volcenginesdkcore
import volcenginesdkfwcenter
from volcenginesdkcore.rest import ApiException
from loguru import logger
from apps.fw_volcengine import utils
import ipaddress
import random
import string

class FwVolcengineApp:
    def __init__(self, ak, sk, endpoint, region, proxies=None):
        self.ak = ak
        self.sk = sk
        self.endpoint = endpoint
        self.region = region
        self.proxies = proxies

    def create_client(self):
        """创建火山云防火墙客户端"""
        configuration = volcenginesdkcore.Configuration()
        configuration.ak = self.ak
        configuration.sk = self.sk
        configuration.region = self.region # 这个参数必须有
        configuration.client_side_validation = True
        configuration.host = self.endpoint
        if self.proxies:
            configuration.proxy = self.proxies
        volcenginesdkcore.Configuration.set_default(configuration)
        return volcenginesdkfwcenter.FWCENTERApi()

    # 火山云云防火墙原子方法

    def add_address_book(self, groupname, grouptype, description, addresslist):
        client = self.create_client()
        # 处理addresslist格式 - 确保是列表
        if isinstance(addresslist, str):
            addresslist = [addr.strip() for addr in addresslist.split(',') if addr.strip()]
        elif not isinstance(addresslist, list):
            addresslist = []
        
        request = volcenginesdkfwcenter.AddAddressBookRequest(
            group_name=groupname,
            group_type=grouptype,
            description=description,
            address_list=addresslist
        )
        # 获取实例名（如果在CBT框架中运行）
        instance_name = getattr(self, 'asset', None) and getattr(self.asset, 'name', None)
        utils.check_os_type(instance_name)
        try:
            resp, status, headers = client.add_address_book_with_http_info(request, _return_http_data_only=False)
            data = resp.to_dict()
            logger.info(f'add_address_book响应: status={status}, data={data}')
            
            # 检查是否真正创建成功：状态码200且有group_uuid
            if status == 200 and data.get('group_uuid'):
                # 再次验证地址组是否真的存在
                import time
                time.sleep(1)  # 等待1秒
                verify_result = self.describe_address_book(query=groupname, grouptype=grouptype)
                logger.info(f'验证地址组创建结果: {verify_result}')
                
                if (isinstance(verify_result, dict) and 
                    verify_result.get('statusCode') == 200 and 
                    verify_result.get('body', {}).get('Acls')):
                    msg = {
                        "desc": "创建成功",
                        "groupname": groupname,
                        "groupuuid": data.get('group_uuid', ''),
                        "description": description
                    }
                    return msg
                else:
                    logger.error(f'地址组创建后验证失败: {verify_result}')
                    msg = {
                        "desc": "创建失败"
                    }
                    return msg
            else:
                logger.error(f'地址组创建失败: status={status}, data={data}')
                msg = {
                    "desc": "创建失败"
                }
                return msg
        except Exception as e:
            logger.error(f'地址组创建异常: {e}')
            return str(e)

    def delete_address_book(self, groupuuid):
        client = self.create_client()
        request = volcenginesdkfwcenter.DeleteAddressBookRequest(
            group_uuid=groupuuid
        )
        # 获取实例名（如果在CBT框架中运行）
        instance_name = getattr(self, 'asset', None) and getattr(self.asset, 'name', None)
        utils.check_os_type(instance_name)
        try:
            resp, status, headers = client.delete_address_book_with_http_info(request, _return_http_data_only=False)
            logger.info(f'删除地址簿成功: {groupuuid}')
            # 完全模拟阿里云的返回格式
            res = {
                "statusCode": status
            }
            return res
        except Exception as e:
            logger.error(f'{e}')
            return str(e)

    def describe_address_book(self, query, grouptype):
        client = self.create_client()
        request = volcenginesdkfwcenter.DescribeAddressBookRequest(
            query=query,
            group_type=grouptype,
            page_number=utils.get_config('describe_address_book.page_number', 1),
            page_size=utils.get_config('describe_address_book.page_size', 500)
        )
        # 获取实例名（如果在CBT框架中运行）
        instance_name = getattr(self, 'asset', None) and getattr(self.asset, 'name', None)
        utils.check_os_type(instance_name)
        try:
            resp, status, headers = client.describe_address_book_with_http_info(request, _return_http_data_only=False)
            data = resp.to_dict()
            logger.info(f'{data}')
            # 完全模拟阿里云的返回格式
            # 火山云SDK返回的是小写下划线格式，需要转换为阿里云的大写驼峰格式
            # 注意：火山云SDK在没有数据时返回data=None，需要转换为空数组
            raw_data = data.get('data') or []
            acls_data = []
            for item in raw_data:
                converted_item = {
                    "GroupName": item.get('group_name', ''),
                    "GroupUuid": item.get('group_uuid', ''),
                    "Description": item.get('description', ''),
                    "AddressList": item.get('address_list', []),
                    "GroupType": item.get('group_type', ''),
                    "RefCnt": item.get('ref_cnt', 0)
                }
                acls_data.append(converted_item)
            
            res = {
                "statusCode": status,
                "body": {
                    "Acls": acls_data
                }
            }
            return res
        except Exception as e:
            logger.error(f'{e}')
            return str(e)

    def modify_address_book(self, groupname, groupuuid, description, addresslist):
        client = self.create_client()
        # 处理addresslist格式
        if isinstance(addresslist, str):
            addresslist = [addr.strip() for addr in addresslist.split(',') if addr.strip()]
        elif not isinstance(addresslist, list):
            addresslist = []
        
        request = volcenginesdkfwcenter.ModifyAddressBookRequest(
            group_name=groupname,
            group_uuid=groupuuid,
            description=description,
            address_list=addresslist
        )
        # 获取实例名（如果在CBT框架中运行）
        instance_name = getattr(self, 'asset', None) and getattr(self.asset, 'name', None)
        utils.check_os_type(instance_name)
        try:
            resp, status, headers = client.modify_address_book_with_http_info(request, _return_http_data_only=False)
            logger.info(f'{resp}')
            # 完全模拟阿里云的返回格式
            res = {
                "statusCode": status
            }
            return res
        except Exception as e:
            logger.error(f'{e}')
            return str(e)

    def describe_control_policy(self, direction, description=None):
        client = self.create_client()
        # 构建请求参数
        request_params = {
            'direction': direction,
            'page_size': utils.get_config('describe_control_policy.page_size', 100)
        }
        # 如果提供了description，添加到请求参数中进行过滤
        if description:
            request_params['description'] = description
            
        request = volcenginesdkfwcenter.DescribeControlPolicyRequest(**request_params)
        # 获取实例名（如果在CBT框架中运行）
        instance_name = getattr(self, 'asset', None) and getattr(self.asset, 'name', None)
        utils.check_os_type(instance_name)
        try:
            resp, status, headers = client.describe_control_policy_with_http_info(request, _return_http_data_only=False)
            data = resp.to_dict()
            logger.info(f'{data}')
            # 完全模拟阿里云的返回格式
            # 火山云SDK返回的是小写下划线格式，需要转换为阿里云的大写驼峰格式
            # 注意：火山云SDK在没有数据时返回data=None，需要转换为空数组
            raw_data = data.get('data') or []
            policys_data = []
            for item in raw_data:
                converted_item = {
                    "AclUuid": item.get('rule_id', ''),
                    "Action": item.get('action', ''),
                    "Description": item.get('description', ''),
                    "Destination": item.get('destination', ''),
                    "DestinationType": item.get('destination_type', ''),
                    "Direction": item.get('direction', ''),
                    "Proto": item.get('proto', ''),
                    "Source": item.get('source', ''),
                    "SourceType": item.get('source_type', ''),
                    "Status": item.get('status', True),
                    "Prio": item.get('prio', 0)
                }
                policys_data.append(converted_item)
            
            res = {
                "statusCode": status,
                "body": {
                    "TotalCount": len(policys_data),
                    "Policys": policys_data
                }
            }
            return res
        except Exception as e:
            logger.error(f'{e}')
            return str(e)

    def add_control_policy(self, aclaction, description, destination, destinationtype, direction, proto, source,
                          sourcetype, neworder, applicationname=None, applicationnamelist=None, domainresolvetype=None):
        client = self.create_client()
        
        # 调试：打印控制策略参数
        logger.info(f"创建控制策略参数: source={source}, sourcetype={sourcetype}, destination={destination}, destinationtype={destinationtype}")
        
        request = volcenginesdkfwcenter.AddControlPolicyRequest(
            prio=int(neworder),
            direction=direction,
            source_type=sourcetype,
            source=source,
            destination_type=destinationtype,
            destination=destination,
            dest_port=utils.get_config('add_control_policy.dest_port', 'ANY'),
            dest_port_type=utils.get_config('add_control_policy.dest_port_type', 'port'),
            proto=proto,
            action=aclaction,
            description=description,
            status=utils.get_config('add_control_policy.status', True)
        )
        # 获取实例名（如果在CBT框架中运行）
        instance_name = getattr(self, 'asset', None) and getattr(self.asset, 'name', None)
        utils.check_os_type(instance_name)
        try:
            resp, status, headers = client.add_control_policy_with_http_info(request, _return_http_data_only=False)
            data = resp.to_dict()
            logger.info(f"{data}")
            # 转换为阿里云兼容格式 - 业务逻辑层面
            if status == 200:
                msg = {
                    "desc": "创建成功",
                    "acluuid": data.get('rule_id', '')  # 修正：火山云SDK返回小写下划线格式
                }
                return msg
            else:
                msg = {
                    "desc": "创建失败"
                }
                return msg
        except Exception as e:
            logger.error(f'{e}')
            return str(e)

    def delete_control_policy(self, acluuid, direction):
        client = self.create_client()
        request = volcenginesdkfwcenter.DeleteControlPolicyRequest(
            rule_id=acluuid,
            direction=direction
        )
        # 获取实例名（如果在CBT框架中运行）
        instance_name = getattr(self, 'asset', None) and getattr(self.asset, 'name', None)
        utils.check_os_type(instance_name)
        try:
            resp, status, headers = client.delete_control_policy_with_http_info(request, _return_http_data_only=False)
            logger.info(f'{resp}')
            # 完全模拟阿里云的返回格式
            res = {
                "statusCode": status
            }
            return res
        except Exception as e:
            logger.error(f'{e}')
            return str(e)


    def auto_block_task(self, addr, direction=None):
        # 加载日志配置（支持实例名）
        instance_name = getattr(self, 'asset', None) and getattr(self.asset, 'name', None)
        utils.check_os_type(instance_name)
        
        # 验证direction参数
        if direction not in ['in', 'out']:
            msg = {
                "addr": f"{addr}",
                "desc": "direction参数必须为'in'或'out'"
            }
            logger.error(f'{msg}')
            return msg
        
        # 验证SOAR入参IP（单个IP） or 手动入参IPS（多个IP）
        addrs = utils.parse_ip_list(addr)
        if not addrs:
            return {
                "statusCode": 400,
                "error": "没有有效的IP地址",
                "body": {
                    "total_ips": 0,
                    "success_count": 0,
                    "failed_count": 0,
                    "existed_count": 0,
                    "results": []
                }
            }
        
        """ 初始化处理状态变量定义及初始化 """
        # 其中任何一个需要封禁的ip同一时间值可以位于4个列表 list_remain_addrs list_success_addrs list_failed_addrs list_existed_addrs 中的一个
        # list_remain_addrs 为消费前的任务ip地址列表，list_remain_addrs list_success_addrs list_failed_addrs list_existed_addrs 则是经过处理后ip应该处于的结果列表
        # list_success_addrs 为封禁成功的ip地址字典的列表
        # list_failed_addrs 为封禁失败的ip地址字典的列表
        # list_existed_addrs 为已存在于现有封禁策略中的ip地址字典的列表
        list_success_addrs = []
        list_failed_addrs = []
        list_existed_addrs = []
        list_remain_addrs = list(addrs)

        # 连续处理失败次数计数器（防止while由于某些原因导致死循环）
        consecutive_failures = 0

        # 设置查询前缀和配置
        query_prefix = f"{utils.get_config('add_address_book.group_name_prefix', '')}-{direction.title()}"
        max_addresses_per_group = utils.get_config('modify_address_book.max_addresses_per_group')
        
        # 主处理循环
        # list_remain_addrs 列表为待处理地址列表，会在执行过程中被消费指导列表为空，退出while循环
        # 一共 3个独立的 步骤：
        # 步骤1：判断需要封禁的地址资源是否已存在于现有封禁策略中
        # 步骤2：遍历现有指定名称前缀的所有地址簿，检查是否存地址簿有空位可用
        # 步骤3：如果还有剩余的待封禁的IP，则创建新的地址簿 和 同名称的控制策略组

        while list_remain_addrs:
            len_list_remain_addrs = len(list_remain_addrs)
            msg = {
                "addr": f"{list_remain_addrs}",
                "desc": f"len({len_list_remain_addrs})"
            }
            logger.info(f'{msg}')
            # 按地址类型分组处理
            # 将ip类型和domain类型进行分组，分别存储到addrs_groups列表中
            addrs_groups = {
                "ip" : [],
                "domain": []
            }
            # 遍历list_remain_addrs列表，将ip类型和domain类型进行分组，分别存储到addrs_groups列表中
            for remain_addr in list_remain_addrs[:]:
                try:
                    # 如果ip地址是ipv4地址，则将ip类型存储到addrs_groups列表中
                    ipaddress.IPv4Address(remain_addr.split('/')[0])
                    describe_address_book_grouptype = "ip"
                except ipaddress.AddressValueError:
                    try:
                        # 如果ip地址是ipv6地址，则将ip类型存储到addrs_groups列表中
                        ipaddress.IPv6Address(remain_addr.split('/')[0])
                        # IPV6 无需封禁
                        list_existed_addrs.append({
                            "addr": f"{remain_addr}",
                            "desc": "无需封禁"
                        })
                        if remain_addr in list_remain_addrs:
                            list_remain_addrs.remove(remain_addr)
                        continue
                    except ipaddress.AddressValueError:
                        # 如果ip地址是domain地址，则将domain类型存储到addrs_groups列表中
                        describe_address_book_grouptype = "domain"
                addrs_groups[describe_address_book_grouptype].append(remain_addr)
            

            ########################################################
            #                       步骤1                           #
            ########################################################
            # 对addrs_groups列表中每一个的ip进行判断
            # 如果存在则加入到  list_existed_addrs 列表，同时移除list_remain_addrs列表中对应的地址（消费掉），保证下一次循环中不再处理该地址
            for describe_address_book_grouptype, list_addrs_groups in addrs_groups.items():
                if not list_addrs_groups:
                    continue
                # 获取该类型的现有地址组
                res_describe_address_book = self.describe_address_book(
                    query=query_prefix,
                    grouptype=describe_address_book_grouptype
                )
                
                # 检查API调用是否成功
                if (isinstance(res_describe_address_book, str) or 
                    res_describe_address_book is None or 
                    res_describe_address_book.get('statusCode') != 200 or
                    res_describe_address_book.get('body') is None or
                    res_describe_address_book.get('body', {}).get('Acls') is None):
                    logger.error(f"查询地址组失败: {res_describe_address_book}")
                    continue
                
                # 检查已存在的IP
                for addr in list_addrs_groups[:]:
                    found = False
                    for list_res_describe_address_book in res_describe_address_book['body']['Acls']:
                        if query_prefix in list_res_describe_address_book['GroupName']:
                            if addr in list_res_describe_address_book['AddressList']:
                                found = True
                                msg = {
                                    "addr": f"{addr}",
                                    "groupname": list_res_describe_address_book['GroupName'],
                                    "desc": "无需封禁"
                                }
                                list_existed_addrs.append(msg)
                                logger.info(f'{msg}')
                                break
                    if found == True:
                        if addr in list_addrs_groups:
                            list_addrs_groups.remove(addr)
                        if addr in list_remain_addrs:
                            list_remain_addrs.remove(addr)
                

                ########################################################
                #                       步骤2                           #
                ########################################################
                # 遍历现有指定名称前缀的所有地址簿，检查是否存地址簿有空位可用
                # 针对每一个可用的地址簿，计算空位数量，假设为N个
                # 1. 如果空位数N大于当前需要封禁的IP数量M，则一次请求，将M个待封禁的IP添加到该地址簿，此时消费完成全部待封禁的IP
                # 2. 如果空位数N小于等于当前需要封禁的IP数量M，则一次请求，将N个待封禁的IP添加到该地址簿，此时还剩余M-N个待封禁的IP
                # 不停遍历每一个地址簿，直到 全部的地址簿空位被填满（N<M） 或 消费完成全部待封禁的IP（N>=M）
                addrgrps = [data_describe_address_book for data_describe_address_book in
                            res_describe_address_book['body']['Acls']
                            if query_prefix in data_describe_address_book['GroupName']]
                for addrgrp in addrgrps:
                    if not list_addrs_groups:
                        break
                    addrgrp_groupname = addrgrp['GroupName']
                    addrgrp_groupuuid = addrgrp['GroupUuid']
                    addrgrp_description = addrgrp['Description']
                    len_addrgrp = len(addrgrp['AddressList'])
                    # 如果当前地址组内的地址数量小于地址组最大容量（这个地址簿还没有满）
                    if len_addrgrp < max_addresses_per_group:
                        # 查询当前地址组同名的控制策略组
                        res_describe_control_policy = self.describe_control_policy(
                            direction=direction,
                            description=addrgrp_groupname
                        )
                        # 如果当前地址组同名的控制策略组不存在，则跳过当前地址组
                        if isinstance(res_describe_control_policy, str) or res_describe_control_policy['body']['TotalCount'] == 0:
                            continue
                        # 如果待消费的ip数量这个地址簿剩余容量（ip数量多到这个地址簿放不下），填完坑还有ip剩余
                        if len(list_addrs_groups[:]) > max_addresses_per_group - len_addrgrp:
                            list_addrgrp_addresslist = list_addrs_groups[:max_addresses_per_group - len_addrgrp]
                            data_addrgrp_addresslist = addrgrp['AddressList'] + list_addrs_groups[:max_addresses_per_group - len_addrgrp]
                        # 如果待消费的ip数量小于等于这个地址簿剩余容量（这个地址簿可以一次性消费掉全部ip），填完坑没有ip剩余
                        else:
                            list_addrgrp_addresslist = list_addrs_groups[:]
                            data_addrgrp_addresslist = addrgrp['AddressList'] + list_addrs_groups[:]
                        # 将需要封禁的地址资源更新至地址资源组
                        res_modify_address_book = self.modify_address_book(
                            groupname=addrgrp_groupname,
                            groupuuid=addrgrp_groupuuid,
                            description=addrgrp_description,
                            addresslist=data_addrgrp_addresslist
                        )
                        # 如果更新地址资源组成功，则将成功信息加入到list_success_addrs列表
                        if isinstance(res_modify_address_book, dict) and res_modify_address_book.get('statusCode') == 200:
                            msg = {
                                "addr": f"{','.join(list_addrgrp_addresslist)}",
                                "groupname": f"{addrgrp_groupname}",
                                "groupuuid": f"{addrgrp_groupuuid}",
                                "grouplen": len(data_addrgrp_addresslist),
                                "desc": "封禁成功"
                            }
                            logger.info(f'{msg}')
                            list_success_addrs.append(msg)
                        # 如果更新地址资源组失败，则将失败信息加入到list_failed_addrs列表
                        else:
                            msg = {
                                "addr": f"{','.join(list_addrgrp_addresslist)}",
                                "groupname": f"{addrgrp_groupname}",
                                "groupuuid": f"{addrgrp_groupuuid}",
                                "desc": "封禁失败"
                            }
                            logger.info(f'{msg}')
                            list_failed_addrs.append(msg)
                        # 将已消费的地址资源从list_addrs_groups列表中移除
                        list_addrs_groups = [x for x in list_addrs_groups if x not in list_addrgrp_addresslist]
                        # 将已消费的地址资源从list_remain_addrs列表中移除
                        list_remain_addrs = [x for x in list_remain_addrs if x not in list_addrgrp_addresslist]



                ########################################################
                #                       步骤3                           #
                ########################################################
                # 如果还有剩余的待封禁的IP，则创建新的地址簿 和 同名称的控制策略组
                # 仅创建新的地址簿 和 同名称的控制策略组，本轮循环内不再处理剩余的待封禁的IP
                # 消费剩余的待封禁的IP 不是在这里处理的，而是交给下一次 while 循环的"步骤2"天填坑来处理的
                if list_addrs_groups:
                    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=6))
                    # 创建地址资源组
                    res_add_address_book = self.add_address_book(
                        groupname=f"{query_prefix}-{random_string}",
                        grouptype=describe_address_book_grouptype,
                        description=f"{query_prefix}-{random_string}",
                        addresslist=utils.get_config('add_address_book.addresslist', '1.1.1.1/32')
                    )
                    # 如果创建地址资源组成功，则创建同名的控制策略组
                    if isinstance(res_add_address_book, dict) and res_add_address_book.get('desc') == "创建成功":
                        # 等待地址组创建完成，火山云API需要时间同步
                        import time
                        time.sleep(5)  # 增加延迟时间
                        
                        # 调试：打印地址组信息
                        logger.info(f"准备创建控制策略，地址组信息: {res_add_address_book}")
                        
                        # 验证地址组是否真的创建成功
                        verify_result = self.describe_address_book(query=res_add_address_book['groupname'], grouptype=describe_address_book_grouptype)
                        logger.info(f"验证地址组是否存在: {verify_result}")
                        
                        if direction == 'in':
                            res_add_control_policy = self.add_control_policy(
                                aclaction=utils.get_config('add_control_policy.action', 'deny'),
                                description=res_add_address_book['description'],
                                destination=utils.get_config('add_control_policy.destination_any', '0.0.0.0/0'),
                                destinationtype='net',
                                direction=direction,
                                proto=utils.get_config('add_control_policy.proto', 'ANY'),
                                source=res_add_address_book['groupuuid'],  # 修改：使用UUID而不是名称
                                sourcetype='group',
                                neworder=utils.get_config('add_control_policy.prio', 2)
                            )
                        elif direction == 'out':
                            if describe_address_book_grouptype == 'ip':
                                res_add_control_policy = self.add_control_policy(
                                    aclaction=utils.get_config('add_control_policy.action', 'deny'),
                                    description=res_add_address_book['description'],
                                    destination=res_add_address_book['groupuuid'],  # 修改：使用UUID而不是名称
                                    destinationtype='group',
                                    direction=direction,
                                    proto=utils.get_config('add_control_policy.proto', 'ANY'),
                                    source=utils.get_config('add_control_policy.source_any', '0.0.0.0/0'),
                                    sourcetype='net',
                                    neworder=utils.get_config('add_control_policy.prio', 2)
                                )
                            elif describe_address_book_grouptype == 'domain':
                                res_add_control_policy = self.add_control_policy(
                                    aclaction=utils.get_config('add_control_policy.action', 'deny'),
                                    description=res_add_address_book['description'],
                                    destination=res_add_address_book['groupuuid'],  # 修改：使用UUID而不是名称
                                    destinationtype='group',
                                    direction=direction,
                                    proto=utils.get_config('add_control_policy.proto', 'TCP'),
                                    source=utils.get_config('add_control_policy.source_any', '0.0.0.0/0'),
                                    sourcetype='net',
                                    neworder=utils.get_config('add_control_policy.prio', 2)
                                )
                        # 如果创建控制策略组成功，则将成功信息加入到list_success_addrs列表
                        if isinstance(res_add_control_policy, dict) and res_add_control_policy.get('desc') == "创建成功":
                            msg = {
                                "groupname": res_add_address_book['groupname'],
                                "groupuuid": res_add_address_book['groupuuid'],
                                "description": res_add_address_book['description'],
                                "acluuid": res_add_control_policy['acluuid'],
                                "desc": "创建成功"
                            }
                            logger.info(f'{msg}')
                        # 如果创建控制策略组失败，则删除地址资源组
                        else:
                            self.delete_address_book(
                                groupuuid=res_add_address_book['groupuuid']
                            )
                            msg = {
                                "groupname": res_add_address_book['groupname'],
                                "groupuuid": res_add_address_book['groupuuid'],
                                "description": res_add_address_book['description'],
                                "acluuid": res_add_control_policy.get('acluuid', '') if isinstance(res_add_control_policy, dict) else '',
                                "desc": "创建失败"
                            }
                            logger.info(f'{msg}')
                    # 如果创建地址资源组失败，记录错误信息
                    else:
                        logger.error(f"创建地址资源组失败: {res_add_address_book}")
                        msg = {
                            "groupname": f"{query_prefix}-{random_string}",
                            "desc": "创建地址资源组失败"
                        }
                        logger.info(f'{msg}')
            # while主循环保护机制
            # 如果2次循环后，list_remain_addrs列表中的地址数量没有变化，则认为封禁失败加入list_failed_addrs，并退出循环
            if len(list_remain_addrs) == len_list_remain_addrs:
                consecutive_failures += 1
                # 如果连续处理失败次数大于等于2，则认为封禁失败加入list_failed_addrs，并退出循环
                if consecutive_failures >= 2:
                    msg = {
                        "addr": f"{list_remain_addrs}",
                        "desc": "封禁失败"
                    }
                    logger.info(f"{msg}")
                    list_failed_addrs.append(msg)
                    break
            else:
                # 如果本轮循环处理了IP，则将连续处理失败次数归零
                consecutive_failures = 0
        # 组装统一的返回结果格式
        all_results = list_success_addrs + list_failed_addrs + list_existed_addrs
        
        if list_success_addrs:
            return {
                "statusCode": 200,
                "message": f"成功将 {len(list_success_addrs)} 个IP添加到火山云防火墙封禁列表",
                "body": {
                    "total_ips": len(addrs),
                    "success_count": len(list_success_addrs),
                    "failed_count": len(list_failed_addrs),
                    "existed_count": len(list_existed_addrs),
                    "results": all_results
                }
            }
        else:
            return {
                "statusCode": 400,
                "error": "所有IP都无法添加到火山云防火墙封禁列表",
                "body": {
                    "total_ips": len(addrs),
                    "success_count": len(list_success_addrs),
                    "failed_count": len(list_failed_addrs),
                    "existed_count": len(list_existed_addrs),
                    "results": all_results
                }
            }

    def auto_unblock_task(self, addr, direction=None):
        # 加载日志配置（支持实例名）
        instance_name = getattr(self, 'asset', None) and getattr(self.asset, 'name', None)
        utils.check_os_type(instance_name)
        
        # 验证direction参数
        if direction not in ['in', 'out']:
            msg = {
                "addr": f"{addr}",
                "desc": "direction参数必须为'in'或'out'"
            }
            logger.error(f'{msg}')
            return msg
        
        # 验证SOAR入参IP（单个IP） or 手动入参IPS（多个IP）
        addrs = utils.parse_ip_list(addr)
        if not addrs:
            return {
                "statusCode": 400,
                "error": "没有有效的IP地址",
                "body": {
                    "total_ips": 0,
                    "success_count": 0,
                    "failed_count": 0,
                    "results": []
                }
            }
        
        """ 初始化处理状态变量定义及初始化 """
        list_success_addrs = []
        list_failed_addrs = []
        list_remain_addrs = list(addrs)

        # 连续处理失败次数计数器（防止while由于某些原因导致死循环）
        consecutive_failures = 0

        # 设置查询前缀
        query_prefix = f"{utils.get_config('add_address_book.group_name_prefix', '')}-{direction.title()}"
        
        # 主处理循环
        while list_remain_addrs:
            len_list_remain_addrs = len(list_remain_addrs)
            msg = {
                "addr": f"{list_remain_addrs}",
                "desc": f"len({len_list_remain_addrs})"
            }
            logger.info(f'{msg}')
            
            # 按地址类型分组处理
            addrs_groups = {
                "ip" : [],
                "domain": []
            }
            
            # 遍历list_remain_addrs列表，将ip类型和domain类型进行分组
            for remain_addr in list_remain_addrs[:]:
                try:
                    ipaddress.IPv4Address(remain_addr.split('/')[0])
                    describe_address_book_grouptype = "ip"
                except ipaddress.AddressValueError:
                    try:
                        ipaddress.IPv6Address(remain_addr.split('/')[0])
                        # IPV6 直接跳过
                        if remain_addr in list_remain_addrs:
                            list_remain_addrs.remove(remain_addr)
                        continue
                    except ipaddress.AddressValueError:
                        describe_address_book_grouptype = "domain"
                addrs_groups[describe_address_book_grouptype].append(remain_addr)

            # 对addrs_groups列表中每一个类型的地址进行处理
            for describe_address_book_grouptype, list_addrs_groups in addrs_groups.items():
                if not list_addrs_groups:
                    continue
                    
                # 获取该类型的现有地址组
                res_describe_address_book = self.describe_address_book(
                    query=query_prefix,
                    grouptype=describe_address_book_grouptype
                )
                
                # 检查API调用是否成功
                if (isinstance(res_describe_address_book, str) or 
                    res_describe_address_book is None or 
                    res_describe_address_book.get('statusCode') != 200 or
                    res_describe_address_book.get('body') is None or
                    res_describe_address_book.get('body', {}).get('Acls') is None):
                    logger.error(f"查询地址组失败: {res_describe_address_book}")
                    continue
                
                # 遍历每个地址组，查找匹配的IP并移除
                for addrgrp in res_describe_address_book['body']['Acls']:
                    if not list_addrs_groups:
                        break
                    if query_prefix not in addrgrp['GroupName']:
                        continue
                        
                    addrgrp_groupname = addrgrp['GroupName']
                    addrgrp_groupuuid = addrgrp['GroupUuid']
                    addrgrp_description = addrgrp['Description']
                    addrgrp_addresslist = addrgrp['AddressList']
                    
                    # 查找匹配的IP并构建新的地址列表
                    matched_addrs = []
                    new_address_list = []
                    
                    for addr in addrgrp_addresslist:
                        found_match = False
                        for remain_addr in list_addrs_groups[:]:
                            if utils.ip_matches(addr, remain_addr):
                                matched_addrs.append(remain_addr)
                                found_match = True
                                break
                        if not found_match:
                            new_address_list.append(addr)
                    
                    # 如果找到匹配的IP，则进行处理
                    if matched_addrs:
                        if len(new_address_list) == 0:
                            # 地址组为空，删除地址组和相关策略
                            # 先查找并删除相关的控制策略（按描述过滤）
                            res_describe_control_policy = self.describe_control_policy(direction, description=addrgrp_groupname)
                            if isinstance(res_describe_control_policy, dict) and res_describe_control_policy.get('statusCode') == 200:
                                policies = res_describe_control_policy['body']['Policys']
                                for policy in policies:
                                    policy_source = policy.get('Source', '')
                                    policy_destination = policy.get('Destination', '')
                                    if ((direction == 'in' and policy_source == addrgrp_groupname) or
                                        (direction == 'out' and policy_destination == addrgrp_groupname)):
                                        policy_id = policy.get('AclUuid')
                                        if policy_id:
                                            res_delete_policy = self.delete_control_policy(policy_id, direction)
                                            if isinstance(res_delete_policy, dict) and res_delete_policy.get('statusCode') == 200:
                                                logger.info(f"成功删除控制策略: {policy_id}")
                                            else:
                                                logger.error(f"删除控制策略失败: {policy_id}")
                            
                            # 删除地址组
                            res_delete_address_book = self.delete_address_book(addrgrp_groupuuid)
                            if isinstance(res_delete_address_book, dict) and res_delete_address_book.get('statusCode') == 200:
                                for matched_addr in matched_addrs:
                                    msg = {
                                        "addr": f"{matched_addr}",
                                        "groupname": addrgrp_groupname,
                                        "groupuuid": addrgrp_groupuuid,
                                        "desc": "解封成功"
                                    }
                                    logger.info(f'{msg}')
                                    list_success_addrs.append(msg)
                                    if matched_addr in list_addrs_groups:
                                        list_addrs_groups.remove(matched_addr)
                                    if matched_addr in list_remain_addrs:
                                        list_remain_addrs.remove(matched_addr)
                            else:
                                for matched_addr in matched_addrs:
                                    msg = {
                                        "addr": f"{matched_addr}",
                                        "desc": "解封失败"
                                    }
                                    logger.info(f'{msg}')
                                    list_failed_addrs.append(msg)
                                    if matched_addr in list_addrs_groups:
                                        list_addrs_groups.remove(matched_addr)
                                    if matched_addr in list_remain_addrs:
                                        list_remain_addrs.remove(matched_addr)
                        else:
                            # 修改地址组，移除匹配的IP
                            res_modify_address_book = self.modify_address_book(
                                groupname=addrgrp_groupname,
                                groupuuid=addrgrp_groupuuid,
                                description=addrgrp_description,
                                addresslist=new_address_list
                            )
                            
                            if isinstance(res_modify_address_book, dict) and res_modify_address_book.get('statusCode') == 200:
                                for matched_addr in matched_addrs:
                                    msg = {
                                        "addr": f"{matched_addr}",
                                        "groupname": addrgrp_groupname,
                                        "groupuuid": addrgrp_groupuuid,
                                        "grouplen": len(new_address_list),
                                        "desc": "解封成功"
                                    }
                                    logger.info(f'{msg}')
                                    list_success_addrs.append(msg)
                                    if matched_addr in list_addrs_groups:
                                        list_addrs_groups.remove(matched_addr)
                                    if matched_addr in list_remain_addrs:
                                        list_remain_addrs.remove(matched_addr)
                            else:
                                for matched_addr in matched_addrs:
                                    msg = {
                                        "addr": f"{matched_addr}",
                                        "desc": "解封失败"
                                    }
                                    logger.info(f'{msg}')
                                    list_failed_addrs.append(msg)
                                    if matched_addr in list_addrs_groups:
                                        list_addrs_groups.remove(matched_addr)
                                    if matched_addr in list_remain_addrs:
                                        list_remain_addrs.remove(matched_addr)
                        
                        # 找到匹配后跳出当前地址簿循环，继续处理下一个地址簿
                        break
                
            # while主循环保护机制
            if len(list_remain_addrs) == len_list_remain_addrs:
                consecutive_failures += 1
                if consecutive_failures >= 2:
                    # 处理剩余未找到的IP
                    for addr in list_remain_addrs:
                        msg = {
                            "addr": f"{addr}",
                            "desc": "未找到该IP"
                        }
                        logger.info(f'{msg}')
                        list_failed_addrs.append(msg)
                    break
            else:
                consecutive_failures = 0
        
        # 组装统一的返回结果格式
        all_results = list_success_addrs + list_failed_addrs
        
        if list_success_addrs:
            return {
                "statusCode": 200,
                "message": f"成功从火山云防火墙封禁列表中移除 {len(list_success_addrs)} 个IP",
                "body": {
                    "total_ips": len(addrs),
                    "success_count": len(list_success_addrs),
                    "failed_count": len(list_failed_addrs),
                    "results": all_results
                }
            }
        else:
            return {
                "statusCode": 400,
                "error": "没有找到需要移除的IP",
                "body": {
                    "total_ips": len(addrs),
                    "success_count": len(list_success_addrs),
                    "failed_count": len(list_failed_addrs),
                    "results": all_results
                }
            }