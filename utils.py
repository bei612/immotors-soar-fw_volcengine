# -*- coding: utf-8 -*-
"""
火山云云防火墙工具类
"""

import os, ipaddress, yaml, platform
from loguru import logger

def check_os_type(instance_name=None):
    """检查系统类型并配置日志
    
    Args:
        instance_name: 应用实例名称，用于区分不同实例的日志文件
    """
    sysname = platform.system().lower()
    logs_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__))) + "/logs"
    
    # 获取应用ID（从文件路径中提取）
    app_id = os.path.basename(os.path.dirname(__file__))
    
    # 构建日志文件名：app_id_实例名.log
    if instance_name:
        # 清理实例名：转换为小写，替换特殊字符为下划线
        clean_instance_name = _clean_instance_name(instance_name)
        log_filename = f"{app_id}_{clean_instance_name}.log"
    else:
        # 兼容旧版本：如果没有实例名，使用app_id.log
        log_filename = f"{app_id}.log"
    
    logger.remove()
    if sysname == 'linux':
        logger.add(f'{logs_path}/{log_filename}', rotation='500MB')
    elif sysname == 'darwin':
        logger.add(f'{logs_path}/{log_filename}', rotation='500MB')
    elif sysname == 'windows':
        logger.add(f'{logs_path}/{log_filename}', rotation='500MB')


def _clean_instance_name(instance_name):
    """清理实例名称，转换为适合文件名的格式
    
    Args:
        instance_name: 原始实例名称
        
    Returns:
        清理后的实例名称（小写，特殊字符转为下划线）
    """
    import re
    # 转换为小写
    cleaned = instance_name.lower()
    # 替换中文和特殊字符为下划线
    cleaned = re.sub(r'[^\w\-_.]', '_', cleaned)
    # 合并多个连续的下划线
    cleaned = re.sub(r'_+', '_', cleaned)
    # 去除首尾下划线
    cleaned = cleaned.strip('_')
    return cleaned or 'default'


_config_cache = {}  # 配置缓存


def get_config(key_path, default=None, config_path=None):
    """获取配置值"""
    # 内联配置文件加载逻辑
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
    
    # 使用缓存避免重复读取
    if config_path not in _config_cache:
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                _config_cache[config_path] = config
                logger.info(f"成功加载配置文件: {config_path}")
        except FileNotFoundError:
            logger.error(f"配置文件不存在: {config_path}")
            _config_cache[config_path] = {}
        except yaml.YAMLError as e:
            logger.error(f"配置文件格式错误: {e}")
            _config_cache[config_path] = {}
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            _config_cache[config_path] = {}
    
    config = _config_cache[config_path]
    
    if not key_path:
        return config if config else default
    
    # 分割路径
    keys = key_path.split('.')
    if len(keys) < 2:
        return default
    
    # 遍历路径获取值
    current_data = config
    for i, key in enumerate(keys):
        if not isinstance(current_data, dict) or key not in current_data:
            # 尝试从common获取
            if i == 0 and key != 'common' and 'common' in config:
                # 从common节获取
                remaining_path = '.'.join(keys[1:])
                common_config = config['common']
                for remaining_key in keys[1:]:
                    if not isinstance(common_config, dict) or remaining_key not in common_config:
                        return default
                    common_config = common_config[remaining_key]
                return common_config
            else:
                return default
        current_data = current_data[key]
    
    return current_data


def normalize_ip(ip):
    """标准化IP格式为CIDR"""
    try:
        ip = ip.strip()
        if not ip:
            return ip
        
        if '/' in ip:
            # CIDR格式验证
            ipaddress.ip_network(ip, strict=False)
            return ip
        else:
            # 单IP添加/32
            ipaddress.ip_address(ip)
            return f"{ip}/32"
    except (ipaddress.AddressValueError, ipaddress.NetmaskValueError, ValueError):
        logger.warning(f"无法规范化IP格式: {ip}")
        return ip


def parse_ip_list(ip_list_str):
    """解析IP列表字符串"""
    if not ip_list_str:
        return []
    
    # 统一各种分隔符为逗号
    normalized_str = ip_list_str.replace('，', ',').replace('\n', ',').replace(';', ',').replace(' ', ',')
    
    ips = []
    for ip in normalized_str.split(','):
        ip = ip.strip()
        if ip:
            normalized_ip = normalize_ip(ip)
            ips.append(normalized_ip)
    
    return ips


def ip_matches(ip1, ip2):
    """检查两个IP是否匹配"""
    try:
        normalized_ip1 = normalize_ip(ip1)
        normalized_ip2 = normalize_ip(ip2)
        return normalized_ip1 == normalized_ip2
    except:
        return ip1.strip() == ip2.strip()


def check_address_type(addresslist):
    """检查地址类型"""
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
