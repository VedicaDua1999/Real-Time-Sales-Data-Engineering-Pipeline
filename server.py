import os
import json
from http.server import BaseHTTPRequestHandler, HTTPServer

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/api/auth":
            response = {"message": "Auth service working"}
        else:
            response = {"message": "Unknown route"}

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(response).encode())

def run():
    port = int(os.environ.get("FUNCTIONS_CUSTOMHANDLER_PORT", os.environ.get("PORT", 8080)))
    server_address = ("0.0.0.0", port)

    httpd = HTTPServer(server_address, Handler)
    print(f"Server running on port {port}")
    httpd.serve_forever()

if __name__ == "__main__":
    run()
