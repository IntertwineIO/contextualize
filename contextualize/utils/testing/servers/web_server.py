import os
from http.server import HTTPServer, SimpleHTTPRequestHandler


class ScopedHTTPHandler(SimpleHTTPRequestHandler):
    """Web handler scoped to server's base_path within current working directory"""
    def translate_path(self, path):
        relative_path = path.lstrip('/')
        scoped_path = os.path.join(self.server.base_path, relative_path)
        return super().translate_path(scoped_path)


class ScopedHTTPServer(HTTPServer):
    """Web server scoped to base_path within current working directory"""
    def __init__(self, server_address, handler_class=ScopedHTTPHandler, base_path='/'):
        self.base_path = base_path
        super().__init__(server_address, handler_class)
