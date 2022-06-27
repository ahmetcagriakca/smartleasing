import io
import os

from flask import request, make_response, send_file
from flask_restx import Resource
from injector import inject
from pdip.configuration.models.application import ApplicationConfig


class FileResource(Resource):
    @inject
    def __init__(self,
                 application_config: ApplicationConfig,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.application_config = application_config

    def get(self):
        args = request.args
        folder = ''
        if "Folder" in args:
            folder = args["Folder"]
        name = ''
        if "Name" in args:
            name = args["Name"]
        mimetype = 'image/png'
        if "MimeType" in args:
            mimetype = args["MimeType"]

        byte_io = self.read_file_from_server(folder, name)
        response = make_response(send_file(byte_io, mimetype=mimetype))
        response.headers['Content-Transfer-Encoding'] = 'base64'
        return response

    def read_file_from_server(self, folder, image_name):
        path = os.path.join(self.application_config.root_directory, "files", folder, image_name)
        file = open(path, 'rb')

        byte_io = io.BytesIO()
        byte_io.write(file.read())
        byte_io.seek(0)

        file.close()
        return byte_io

    def post(self):
        file = request.data
        file_name = "kira_bedel.csv"
        self.write_binary_file_to_server(file, file_name)

    def write_binary_file_to_server(self, file, file_name):
        path = os.path.join(self.application_config.root_directory, 'files', 'data', file_name)
        with open(path, 'wb') as ff:
            ff.write(file)
            ff.close()
        return file_name
