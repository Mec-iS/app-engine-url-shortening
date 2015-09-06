'''Shortie - URL shortener

An AppEngine Demo.
Taken from https://bitbucket.org/tebeka/appengine-py-demo/src/
'''
import base62

from google.appengine.ext import webapp
from google.appengine.api import users
from google.appengine.ext import db
from google.appengine.api import memcache
from google.appengine.api import taskqueue
import jinja2

from os.path import dirname
from urlparse import urlparse
import re
from httplib import NOT_FOUND
import logging as log

# Load templates from application directory
get_template = jinja2.Environment(
    loader=jinja2.FileSystemLoader(dirname(__file__))).get_template

# Check if URL has schema prefix
has_schema = re.compile('^[a-z]+://.+').match

class Counter(db.Model):
    '''Global counter.'''
    key_name = 'counter-key'

    count = db.IntegerProperty(default=0)

def get_counter():
    '''Get global counter object, create one first time.'''
    counter = Counter.get_by_key_name(Counter.key_name)
    if counter is None:
        counter = Counter(key_name=Counter.key_name)
    return counter

def next_id():
    '''Get next short url id.

    We do that by incrementing the global counter and then base62 encoding the
    current count.
    '''

    id = [0]

    # Update counter in transaction
    def txn():
        counter = get_counter()
        counter.count += 1
        counter.put()
        id[0] = counter.count

    db.run_in_transaction(txn)

    return base62.encode(id[0])

class Url(db.Model):
    '''Url object in database.

    Key name will be the short id.
    '''
    long = db.LinkProperty()
    user = db.UserProperty()
    created = db.DateTimeProperty(auto_now_add=True)
    hits = db.IntegerProperty(default=0)

def get_url(short_url):
    '''Get a Url from datastore by short url.'''
    return Url.get_by_key_name(short_url)

def inc_hits(short_url):
    '''Increment hits on a url.'''

    # Transaction function
    def txn():
        url = get_url(short_url)
        if not url:
            log.error('inc_hits - {0} not found'.format(short_url))
            return
        url.hits += 1
        url.put()

    db.run_in_transaction(txn)

def user_urls(user, max=100):
    '''Last 100 urls for user.'''
    return Url.all().filter('user =' , user).order('-created').fetch(max)

def fix_url(url):
    '''Fix url by appending http:// if needed.

    Will raise an ValueError on malformed url.
    '''
    url = url.strip()
    if not url:
        raise ValueError('empty url')
    if '.' not in url:
        raise ValueError('{0} - malformed url'.format(url))

    if not has_schema(url):
        url = 'http://{0}'.format(url)

    return url

class Home(webapp.RequestHandler):
    def get(self):
        self.reply()

    def post(self):
        url = self.request.get('url') or ''
        try:
            long_url = fix_url(url)
        except ValueError as e:
            self.reply(error=str(e))
            return

        try:
            short_url = next_id()
            url = Url(key_name=short_url)
            url.long = long_url
            url.user = users.get_current_user()
            url.put()
        except db.Error as e:
            self.reply(error=str(e))
            return

        self.reply(short_url=self.full_url(short_url))

    def reply(self, **kw):
        '''Fills index template with kw and other variables, send to client.'''
        template = get_template('index.html')
        user = users.get_current_user()
        env = kw.copy()
        env.update({
            'login' : self.login_html(),
            'user' : user or 'stranger',
            'count' : get_counter().count,
            'urls' : user_urls(user) if user else None
        })
        self.response.out.write(template.render(**env))

    def login_html(self):
        '''Login/Logout HTML sinppet.'''
        user = users.get_current_user()
        if user:
            fn, txt = users.create_logout_url, 'Logout'
        else:
            fn, txt = users.create_login_url, 'Login'

        return '<a href="{0}">{1}</a>'.format(fn(self.request.uri), txt)

    def full_url(self, short):
        '''Adds <schema>:://netloc before short url to get full url.

        This works both locally and on AppEngine.'''
        url = urlparse(self.request.uri)
        return '{0.scheme}://{0.netloc}/{1}'.format(url, short)

class Redirect(webapp.RequestHandler):
    '''Redirect handler, gets short URL and issue an HTTP redirect to the long
    one.'''
    def get(self, short_url):
        long_url = memcache.get(short_url)
        if not long_url:
            url = get_url(short_url)
            if not url:
                log.error('redirect for {0} not found'.format(short_url))
                self.abort(NOT_FOUND)
            long_url = str(url.long)
            memcache.set(short_url, long_url)

        # Increment count async
        taskqueue.add(url=INC_URL, params={'url' : short_url})

        self.redirect(long_url)

INC_URL = '/_worker/hit'
class Hit(webapp.RequestHandler):
    '''Hit worker, will increment hit count for url.'''
    def post(self):
        short_url = self.request.get('url')
        inc_hits(short_url)

app = webapp.WSGIApplication([
    ('/', Home),
    (INC_URL, Hit),
    ('/([a-zA-Z0-9]+)', Redirect),
], debug=True)