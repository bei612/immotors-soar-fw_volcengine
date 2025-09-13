# -*- coding: utf-8 -*-
# @Autor : immotors

from cbt.base_app import BaseApp
from cbt.action_result import ActionResult
import cbt.status as cbt_status
from apps.fw_volcengine.FwVolcengineApp import FwVolcengineApp
from loguru import logger
import os
import ipaddress
import re

class App(BaseApp):

    def initialize(self, asset_config):
        self.ak = asset_config.get('ak')
        self.sk = asset_config.get('sk')
        self.endpoint = asset_config.get('endpoint')
        self.region = asset_config.get('region')
        self.proxy_url = asset_config.get('proxy_url', '')
        self.proxy_user = asset_config.get('proxy_user', '')
        self.proxy_pass = asset_config.get('proxy_pass', '')
        if re.match(r'(http|https|socks5)://[\w\.]+:\d+', self.proxy_url):
            if self.proxy_user == '' or self.proxy_pass == '':
                self.proxies = self.proxy_url
            else:
                scheme = self.proxy_url.split('://')
                self.proxies = f"{scheme[0]}://{self.proxy_user}:{self.proxy_pass}@{scheme[1]}"
        else:
            self.proxies = None
        return cbt_status.APP_SUCCESS

    def unload(self):
        pass

    def handle_action(self, action_id, params, action_context):
        # action_id 动作ID
        # params 动作参数
        # action_context 动作上下文
        result = ActionResult()
        # 脚本调用的接口
        if hasattr(App, action_id):
            func = getattr(App, action_id)
            # 连通性测试
            if func.__code__.co_name == "test_connectivity":
                if func(self):
                    result.status = cbt_status.ACTION_STATUS_SUCCESS
                    result.message = "连通性测试成功"
                else:
                    result.status = cbt_status.ACTION_STATUS_FAILURE
                    result.message = "连通性测试失败"
                return result
            else:
                data = func(self, params)
                result.data = data
        return result
        
    def test_connectivity(self):
        # 连通性测试
        # return: True or False
        try:
            app = FwVolcengineApp(ak=self.ak, sk=self.sk, endpoint=self.endpoint, region=self.region, proxies=self.proxies)
            # 查询地址簿验证连通性
            app.describe_address_book("", "ip")
            return True
        except Exception as e:
            logger.error(f"连通性测试失败: {e}")
            return False

    def AddAddressBook(self, params):
        groupname = params.get("groupname")
        grouptype = params.get("grouptype")
        description = params.get("description")
        addresslist = params.get("addresslist")
        addaddressbookobj = FwVolcengineApp(ak=self.ak, sk=self.sk, endpoint=self.endpoint, region=self.region, proxies=self.proxies)
        res = addaddressbookobj.add_address_book(groupname, grouptype, description, addresslist)
        return res

    def DeleteAddressBook(self, params):
        groupuuid =  params.get("groupuuid")
        deleteaddressbookobj = FwVolcengineApp(ak=self.ak, sk=self.sk, endpoint=self.endpoint, region=self.region, proxies=self.proxies)
        res = deleteaddressbookobj.delete_address_book(groupuuid)
        return res

    def DescribeAddressBook(self, params):
        query = params.get("query")
        grouptype = params.get("grouptype")
        describeaddressbookobj = FwVolcengineApp(ak=self.ak, sk=self.sk, endpoint=self.endpoint, region=self.region, proxies=self.proxies)
        res = describeaddressbookobj.describe_address_book(query, grouptype)
        return res

    def ModifyAddressBook(self, params):
        groupname = params.get("groupname")
        groupuuid = params.get("groupuuid")
        description = params.get("description")
        addresslist = params.get("addresslist")
        modifyaddressbookobj = FwVolcengineApp(ak=self.ak, sk=self.sk, endpoint=self.endpoint, region=self.region, proxies=self.proxies)
        res = modifyaddressbookobj.modify_address_book(groupname, groupuuid, description, addresslist)
        return res

    def DescribeControlPolicy(self, params):
        direction = params.get("direction")
        describecontrolpolicyobj = FwVolcengineApp(ak=self.ak, sk=self.sk, endpoint=self.endpoint, region=self.region, proxies=self.proxies)
        res = describecontrolpolicyobj.describe_control_policy(direction)
        return res

    def AddControlPolicy(self, params):
        aclaction = params.get("aclaction")
        description = params.get("description")
        destination = params.get("destination")
        destinationtype = params.get("destinationtype")
        direction = params.get("direction")
        proto = params.get("proto")
        source = params.get("source")
        sourcetype = params.get("sourcetype")
        neworder = params.get("neworder")
        applicationname = params.get("applicationname")
        applicationnamelist = params.get("applicationnamelist")
        domainresolvetype = params.get("domainresolvetype")
        addcontrolpolicyobj = FwVolcengineApp(ak=self.ak, sk=self.sk, endpoint=self.endpoint, region=self.region, proxies=self.proxies)
        res = addcontrolpolicyobj.add_control_policy(aclaction, description, destination, destinationtype, direction, proto, source, sourcetype, neworder, applicationname, applicationnamelist, domainresolvetype)
        return res

    def DeleteControlPolicy(self, params):
        acluuid = params.get("acluuid")
        direction = params.get("direction")
        deletecontrolpolicyobj = FwVolcengineApp(ak=self.ak, sk=self.sk, endpoint=self.endpoint, region=self.region, proxies=self.proxies)
        res = deletecontrolpolicyobj.delete_control_policy(acluuid, direction)
        return res


    def AutoBlockTask(self, params):
        """批量封禁IP地址"""
        addr = params.get("addr")
        direction = params.get("direction")
        autoblockobj = FwVolcengineApp(ak=self.ak, sk=self.sk, endpoint=self.endpoint, region=self.region, proxies=self.proxies)
        res = autoblockobj.auto_block_task(addr, direction)
        return res

    def AutoUnblockTask(self, params):
        """批量解封IP地址"""
        addr = params.get("addr")
        direction = params.get("direction")
        autounblockobj = FwVolcengineApp(ak=self.ak, sk=self.sk, endpoint=self.endpoint, region=self.region, proxies=self.proxies)
        res = autounblockobj.auto_unblock_task(addr, direction)
        return res