import pyodata
from functools import lru_cache
from oauthlib.oauth2 import WebApplicationClient
from requests_oauthlib import OAuth2Session

from exaexact.based_entities import based_entitites_patch, rebase_schema

def _urljoin(url1, url2):
    return f"{url1.rstrip('/')}/{url2.lstrip('/')}"

def _fix_system_schema(schema):
    # Fix Divisions metadata
    try:
        division_type = schema.entity_set('Divisions').entity_type
        division_type.proprty('Class_01')._nullable = True
        division_type.proprty('Class_02')._nullable = True
        division_type.proprty('Class_03')._nullable = True
        division_type.proprty('Class_04')._nullable = True
        division_type.proprty('Class_05')._nullable = True
    except KeyError:
        pass
    
class ExactOnline:
    def __init__(self, host, oauth: OAuth2Session) -> None:
        self.host = host # e.g. 'https://start.exactonline.nl'
        self.oauth = oauth

    @classmethod
    def create_session(cls, host, client_id, client_secret, redirect_uri = 'http://localhost:8080', token=None, **kwargs):
        extra  = { 'client_id' : client_id, 'client_secret' : client_secret }
        client = WebApplicationClient(client_id)
        oauth  = OAuth2Session(client=client, redirect_uri=redirect_uri, token=token,
                               auto_refresh_url=f'{host}/api/oauth2/token', 
                               auto_refresh_kwargs=extra,
                               **kwargs)
        return cls(host, oauth)

    @lru_cache()
    def odata(self, path, division=None):
        if '{division}' in path:
            division = division or self.current_division
        with based_entitites_patch():
            client = pyodata.Client(_urljoin(self.host, path.format(division=division)), self.oauth)
            rebase_schema(client._schema)
            if path.endswith('/system'):
                _fix_system_schema(client._schema)
            return client

    def get_entity_set(self, endpoint, division=None):
        division = division or self.current_division
        service, entity = endpoint.rsplit('/', 1)
        return getattr(self.odata(service, division=division).entity_sets, entity)

    @property
    def me(self):
        current = self.odata('/api/v1/current')
        me, = current.entity_sets.Me.get_entities().top(1).execute()
        return me

    @property
    @lru_cache()
    def current_division(self):
        return self.me.CurrentDivision

    def request_token(self, host, redirect_uri, client_secret):
        import webbrowser
        from http.server import HTTPServer, BaseHTTPRequestHandler
        from urllib.parse import urlparse, parse_qs
        
        class GetURI(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args) -> None:
                pass

            def do_GET(request):
                try:
                    response = urlparse(request.path)
                    code = parse_qs(response.query)['code'][0]
                    self.oauth.fetch_token(token_url = f'{host}/api/oauth2/token', code=code, client_secret=client_secret)
                    if self.oauth.token_updater:
                        self.oauth.token_updater(self.oauth.token)
                    request.send_response(200, 'OK')
                    request.wfile.write(b'OK')
                except Exception as e:
                    request.send_response(500, 'ERROR')
                    request.wfile.write(str(e))
                
        uri = urlparse(redirect_uri)
        with HTTPServer((uri.hostname, uri.port), GetURI) as httpd:
            uri, state = self.oauth.authorization_url(f'{host}/api/oauth2/auth')
            webbrowser.open(uri)
            httpd.socket.settimeout(60)
            httpd.handle_request() # Handle on request and quit.