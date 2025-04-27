from requests import Response
import asyncio
from typing import Union, Optional
import aiohttp


class PolyException(Exception):
    def __init__(self, msg):
        self.msg = msg


class PolyApiException(PolyException):
    def __init__(self, resp: Union[Response, aiohttp.ClientResponse, None] = None, error_msg=None):
        assert resp is not None or error_msg is not None
        
        if resp is not None:
            # 兼容处理aiohttp和requests响应对象
            if hasattr(resp, 'status_code'):  # requests响应对象
                self.status_code = resp.status_code
                self.error_msg = self._get_message_sync(resp)
            elif hasattr(resp, 'status'):  # aiohttp响应对象
                self.status_code = resp.status
                self.error_msg = self._get_message_async(resp)
            else:
                self.status_code = None
                self.error_msg = str(resp)
        
        if error_msg is not None:
            self.error_msg = error_msg
            if not hasattr(self, 'status_code'):
                self.status_code = None

    def _get_message_sync(self, resp: Response):
        try:
            return resp.json()
        except Exception:
            return resp.text
    
    def _get_message_async(self, resp: aiohttp.ClientResponse):
        try:
            # 尝试获取已解析的JSON（如果响应已被读取）
            if hasattr(resp, '_body') and resp._body is not None:
                import json
                try:
                    return json.loads(resp._body.decode('utf-8'))
                except:
                    return resp._body.decode('utf-8')
            else:
                # 如果响应尚未被读取，这里不能异步获取内容
                # 因为我们已经在异步上下文中
                return f"HTTP Error: {resp.status}"
        except Exception as e:
            return f"Error parsing response: {str(e)}"

    def __repr__(self):
        return "PolyApiException[status_code={}, error_message={}]".format(
            self.status_code, self.error_msg
        )

    def __str__(self):
        return self.__repr__()
