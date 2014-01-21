# -*- coding: utf-8 -*-

__license__ = """
    This file is part of BigD.

    BigD is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    BigD is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with BigD.  If not, see <http://www.gnu.org/licenses/>.

    Store Flask in sessions in pycassa/cassandra

    Inspired from http://flask.pocoo.org/snippets/75/

    How to use it :

    - Copy this file in your project

    - Create the column_family in your 'keyspace': 'Sessions' (ie)

    - And use it :

    from cassasession import PycassaSessionInterface
    ...

    #Define a secret key
    app.secret_key = 'secret'

    #and create a pool
    pool = ConnectionPool('keyspace',['localhost'])

    #Define a column family for 'Sessions'
    session_family = ColumnFamily(dbpool, 'Sessions')

    #And define the session interface or your app
    app.session_interface = PycassaSessionInterface(session_family)

    - That's all folk !!!

"""
__copyright__ = "2013 : (c) Sébastien GALLET aka bibi21000"
__author__ = 'Sébastien GALLET aka bibi21000'
__email__ = 'bibi21000@gmail.com'

import pickle
from datetime import timedelta
from uuid import uuid4
from werkzeug.datastructures import CallbackDict
from flask.sessions import SessionInterface, SessionMixin
from pycassa import NotFoundException

class PycassaSession(CallbackDict, SessionMixin):

    def __init__(self, initial=None, sid=None, new=False):
        def on_update(self):
            self.modified = True
        CallbackDict.__init__(self, initial, on_update)
        self.sid = sid
        self.new = new
        self.modified = False

class PycassaSessionInterface(SessionInterface):
    serializer = pickle
    session_class = PycassaSession

    def __init__(self, column_fam, column='session'):
        self.column_fam = column_fam
        self.column = column

    def generate_sid(self):
        return str(uuid4())

    def get_db_expiration_time(self, app, session):
        if session.permanent:
            return app.permanent_session_lifetime
        return timedelta(days=1)

    def open_session(self, app, request):
        sid = request.cookies.get(app.session_cookie_name)
        if not sid:
            sid = self.generate_sid()
            return self.session_class(sid=sid, new=True)
        try :
            item=self.column_fam.get(sid,[self.column])
            data = self.serializer.loads(item[self.column])
            return self.session_class(data, sid=sid)
        except NotFoundException :
            return self.session_class(sid=sid, new=True)

    def save_session(self, app, session, response):
        domain = self.get_cookie_domain(app)
        if not session:
            try :
                self.column_fam.remove(session.sid,[self.column])
            except NotFoundException :
                pass
            if session.modified:
                response.delete_cookie(app.session_cookie_name,
                                       domain=domain)
            return
        cassa_exp = self.get_db_expiration_time(app, session)
        cookie_exp = self.get_expiration_time(app, session)
        val = self.serializer.dumps(dict(session))
        self.column_fam.insert(session.sid, {self.column:val}, ttl=cassa_exp.total_seconds())
        response.set_cookie(app.session_cookie_name, session.sid,
                            expires=cookie_exp, httponly=True,
                            domain=domain)
