# -*- coding: utf-8 -*-
"""
火山云云防火墙工具类
"""

import os, ipaddress, yaml, platform
from loguru import logger
from functools import wraps

def setup_logging(use_instance_name=True):
    """日志装饰器，自动配置日志系统
    
    Args:
        use_instance_name: True=使用应用名_实例名，False=仅使用应用名
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                # 获取应用ID（从文件路径中提取）
                current_dir = os.path.dirname(__file__)
                src_dir = os.path.dirname(os.path.dirname(current_dir))
                logs_path = os.path.join(src_dir, "logs")
                
                # 确保日志目录存在
                os.makedirs(logs_path, exist_ok=True)
                
                app_id = os.path.basename(current_dir)
                
                # 构建日志文件名
                if use_instance_name:
                    # 尝试获取实例名
                    instance_name = getattr(self, 'asset', None) and getattr(self.asset, 'name', None)
                    if instance_name:
                        log_filename = f"{app_id}_{instance_name}.log"
                    else:
                        log_filename = f"{app_id}.log"
                else:
                    log_filename = f"{app_id}.log"
                
                log_file_path = os.path.join(logs_path, log_filename)
                
                # 配置日志
                logger.remove()
                logger.add(
                    log_file_path,
                    rotation='500MB',
                    encoding='utf-8',
                    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {name}:{function}:{line} - {message}",
                    level="INFO"
                )
                logger.info(f"日志配置完成: {log_file_path}")
                
            except Exception as e:
                print(f"日志配置失败: {e}")
                # 降级到控制台输出
                logger.remove()
                logger.add(
                    lambda msg: print(msg, end=''),
                    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {message}",
                    level="INFO"
                )
            
            return func(self, *args, **kwargs)
        return wrapper
    return decorator


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
