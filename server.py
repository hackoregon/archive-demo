import re
import psycopg2
from json import dumps, loads
from werkzeug.wrappers import Request, Response

from psycopg2.extras import register_json, Json
register_json(oid=3802, array_oid=3807) # jsonb type from 9.4


location = re.compile(
    r"/(?P<db>\w+)/(?P<schema>\w+)/(?P<function>\w+)(?P<path>/.*){0,}$"
    )

query_template = """
select array_to_json(array_agg(row_to_json(r, true)), true)::text
 from (select * from {}(%s, %s::jsonb, %s::jsonb
"""

methods = dict(GET=query_template + ") as result ) r",
               POST=query_template + ", %s) as result ) r")

@Request.application
def application(request):
    groups = location.match(request.path).groups()
    if not groups:
        raise Exception('badz')
    db, schema, func_name, path = groups
    path = '' if path is None else path
    path = Json(filter(None, path.split('/')))
    method = request.method
    function = "{}.{}{}".format(schema, method, func_name)
    template = methods.get(method)
    if not template:
        raise Exception('bad method')
    query = template.format(function)
    args = Json(dict(request.args))
    if method == 'GET':
        args = ('', path, args)
    elif method == 'POST':
        args = ('', path, args, Json(loads(request.get_data())))
    with psycopg2.connect("dbname=%s user=server" % db) as conn:
        with conn.cursor() as cur:
            cur.execute(query, args)
            return Response(cur.fetchone()[0])

if __name__ == '__main__':
    from werkzeug.serving import run_simple
    run_simple('localhost', 4000, application)
