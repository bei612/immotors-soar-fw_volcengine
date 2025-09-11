# -*- coding: utf-8 -*-
# @Autor : immotors

from cbt.base_app import BaseApp
from cbt.action_result import ActionResult
import cbt.status as cbt_status
from apps.fw_volcengine.FwVolcengineApp import FwVolcengineApp

class App(BaseApp):
    def initialize(self, asset_config):
        self.ak = asset_config.get('ak')
        self.sk = asset_config.get('sk')
        self.endpoint = asset_config.get('endpoint')
        self.region = asset_config.get('region')
        is_proxy = asset_config.get("is_proxy")
        proxy_type = asset_config.get("proxy_type")
        proxy_host = asset_config.get("proxy_host")
        proxy_port = asset_config.get("proxy_port")
        proxy = "{0}://{1}:{2}".format(proxy_type, proxy_host, proxy_port)
        if is_proxy == "Y":
            if proxy_type == "http":
                self.proxies = proxy
            elif proxy_type == "https":
                self.proxies = proxy
        else:
            self.proxies = None
        return cbt_status.APP_SUCCESS

    def unload(self):
        pass

    def handle_action(self, action_id, params, action_context):
        # action_id 动作ip，数据类型string
        # params 动作参数，数据类型dict
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
        # 连通性测
        # return: True or False
        # TODO 自己实现联通性测试的逻辑
        return True

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

    def AddControlPolicy(self, params):
        prio = params.get("prio")
        direction = params.get("direction")
        sourcetype = params.get("sourcetype")
        source = params.get("source")
        sourcetypegrouptype = params.get("sourcetypegrouptype")
        destinationtype = params.get("destinationtype")
        destination = params.get("destination")
        destinationgrouptype = params.get("destinationgrouptype")
        proto = params.get("proto")
        action = params.get("action")
        description = params.get("description")
        addcontrolpolicyobj = FwVolcengineApp(ak=self.ak, sk=self.sk, endpoint=self.endpoint, region=self.region, proxies=self.proxies)
        res = addcontrolpolicyobj.add_control_policy(prio, direction, sourcetype, source, sourcetypegrouptype, destinationtype, destination, destinationgrouptype, proto, action, description)
        return res

    def DeleteControlPolicy(self, params):
        ruleid = params.get("ruleid")
        direction = params.get("direction")
        deletecontrolpolicyobj = FwVolcengineApp(ak=self.ak, sk=self.sk, endpoint=self.endpoint, region=self.region, proxies=self.proxies)
        res = deletecontrolpolicyobj.delete_control_policy(ruleid, direction)
        return res

    def DescribeControlPolicy(self, params):
        direction = params.get("direction")
        describecontrolpolicyobj = FwVolcengineApp(ak=self.ak, sk=self.sk, endpoint=self.endpoint, region=self.region, proxies=self.proxies)
        res = describecontrolpolicyobj.describe_control_policy(direction)
        return res

    def ModifyControlPolicy(self, params):
        ruleid = params.get("ruleid")
        direction = params.get("direction")
        sourcetype = params.get("sourcetype")
        source = params.get("source")
        sourcetypegrouptype = params.get("sourcetypegrouptype")
        destinationtype = params.get("destinationtype")
        destination = params.get("destination")
        destinationgrouptype = params.get("destinationgrouptype")
        proto = params.get("proto")
        action = params.get("action")
        description = params.get("description")
        modifycontrolpolicyobj = FwVolcengineApp(ak=self.ak, sk=self.sk, endpoint=self.endpoint, region=self.region, proxies=self.proxies)
        res = modifycontrolpolicyobj.modify_control_policy(ruleid, direction, sourcetype, source, sourcetypegrouptype, destinationtype, destination, destinationgrouptype, proto, action, description)
        return res

    def SingleBlockAddress(self, params):
        addr = params.get("addr")
        direction = params.get("direction")
        singleblockaddressobj = FwVolcengineApp(ak=self.ak, sk=self.sk, endpoint=self.endpoint, region=self.region, proxies=self.proxies)
        res = singleblockaddressobj.single_block_address(addr, direction)
        return res

    def SingleUnblockAddress(self, params):
        addr = params.get("addr")
        direction = params.get("direction")
        singleunblockaddressobj = FwVolcengineApp(ak=self.ak, sk=self.sk, endpoint=self.endpoint, region=self.region, proxies=self.proxies)
        res = singleunblockaddressobj.single_unblock_address(addr, direction)
        return res