from databases import Database


db = Database('postgresql://postgres_user:password@localhost/my_postgres_db')


async def execute(query, is_many, values=None):
    if is_many:
        await db.execute_many(query=query, values=values)
    else:
        await db.execute(query=query, values=values)


async def fetch(query, is_one, values=None):
    if is_one:
        result = await db.fetch_one(query=query, values=values)
        out = dict(result)
    else:
        result = await db.fetch_all(query=query, values=values)
        out = []
        for row in result:
            out.append(dict(row))
    return out