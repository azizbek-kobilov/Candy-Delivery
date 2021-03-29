import uvicorn
from utils.db import db, execute, fetch
from fastapi import FastAPI, Body, Request, status, Response
from typing import List
from models.couriers import CourierIn
from models.orders import OrderIn
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi import HTTPException
from datetime import time, datetime


app = FastAPI(title='Candy Delivery App', version='1.0')


@app.on_event("startup")
async def connect_db():
    await db.connect()


@app.on_event("shutdown")
async def disconnect_db():
    await db.disconnect()


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    if str(request.url).__contains__('/couriers'):
        r = []
        for item in exc.errors():
            r.append({'id': exc.body['data'][item['loc'][2]]['courier_id']})
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=jsonable_encoder({"validation_error": {"couriers": r}}),
        )

    if str(request.url).__contains__('/orders/assign'):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST)

    if str(request.url).__contains__('/orders/complete'):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST)

    if str(request.url).__contains__('/orders'):
        r = []
        for item in exc.errors():
            r.append({'id': exc.body['data'][item['loc'][2]]['order_id']})
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=jsonable_encoder({"validation_error": {"orders": r}}),
        )

    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST
    )


@app.post('/couriers',
          description='Import couriers',
          status_code=status.HTTP_201_CREATED)
async def post_couriers(request: Request,
                        response: Response,
                        data: List[CourierIn] = Body(..., embed=True)):

    result = []
    error = []
    r = await request.json()

    for item in r['data']:
        if len(item) == 4 and ('courier_id' and 'courier_type' and 'regions' and 'working_hours' in item):
            continue
        else:
            error.append({'id': item['courier_id']})

    if len(error) >= 1:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"validation_error": {"couriers": error}}

    for item in data:
        query = 'INSERT INTO couriers VALUES(:courier_id, :courier_type, :regions, :working_hours)'
        values = {
            "courier_id": item.courier_id,
            "courier_type": item.courier_type,
            "regions": item.regions,
            "working_hours": item.working_hours
        }
        await execute(query, False, values)
        result.append({'id': item.courier_id})

    return {'couriers': result}


@app.patch('/couriers/{courier_id}',
           description='Update courier by id')
async def patch_couriers_by_id(request: Request,
                               courier_id: int):

    r = await request.json()
    values = ''
    count = 1
    for item in r:
        if item not in ['courier_type', 'regions', 'working_hours']:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST)
        else:
            if count < len(r):
                if item in ['regions', 'working_hours']:
                    values += f'{item} = ARRAY{r[item]}, '
                    count += 1
                else:
                    values += f'{item} = \'{r[item]}\', '
                    count += 1
            else:
                if item in ['regions', 'working_hours']:
                    values += f'{item} = ARRAY{r[item]}'
                else:
                    values += f'{item} = \'{r[item]}\''

    query = f'UPDATE couriers SET {values} WHERE courier_id={courier_id}'
    await execute(query, False)
    query = f'SELECT * FROM couriers WHERE courier_id={courier_id}'
    courier = await fetch(query, False)
    courier = courier[0]

    query = f'SELECT * FROM orders WHERE assign={courier_id} AND complete IS NULL'
    assigned_orders = await fetch(query, False)

    orders = []
    for item in assigned_orders:
        if courier['courier_type'] == 'foot':
            if (item['weight'] <= 10) and (item['region'] in courier['regions']) and (item['complete'] is None):
                orders.append(item)
        elif courier['courier_type'] == 'bike':
            if (item['weight'] <= 15) and (item['region'] in courier['regions']) and (item['complete'] is None):
                orders.append(item)
        elif courier['courier_type'] == 'car':
            if (item['weight'] <= 15) and (item['region'] in courier['regions']) and (item['complete'] is None):
                orders.append(item)

    def date_intersection(t1, t2):
        t1start, t1end = t1[0], t1[1]
        t2start, t2end = t2[0], t2[1]
        return (t1start <= t2start <= t1end) or (t2start <= t1start <= t2end)

    available = []
    for order in range(len(orders)):
        available.append({'order_id': orders[order]['order_id'], 'good': []})
        for a in range(len(courier['working_hours'])):
            for b in range(len(orders[order]['delivery_hours'])):
                d1 = time(hour=int(courier['working_hours'][a].split('-')[0].split(':')[0]),
                          minute=int(courier['working_hours'][a].split('-')[0].split(':')[1]))
                d2 = time(hour=int(courier['working_hours'][a].split('-')[1].split(':')[0]),
                          minute=int(courier['working_hours'][a].split('-')[1].split(':')[1]))
                d3 = time(hour=int(orders[order]['delivery_hours'][b].split('-')[0].split(':')[0]),
                          minute=int(orders[order]['delivery_hours'][b].split('-')[0].split(':')[1]))
                d4 = time(hour=int(orders[order]['delivery_hours'][b].split('-')[1].split(':')[0]),
                          minute=int(orders[order]['delivery_hours'][b].split('-')[1].split(':')[1]))
                available[order]['good'].append(date_intersection((d1, d2), (d3, d4)))

    unassign = []
    for order in available:
        if True in order['good']:
            unassign.append(order['order_id'])

    for order in assigned_orders:
        order_id = order['order_id']
        if order['order_id'] not in unassign:
            query = f'UPDATE orders SET assign=NULL, courier_type=NULL ' \
                    f'WHERE order_id={order_id} AND complete IS NULL'
            await execute(query, False)

    return courier


@app.post('/orders',
          description='Import orders',
          status_code=status.HTTP_201_CREATED)
async def post_orders(request: Request,
                      response: Response,
                      data: List[OrderIn] = Body(..., embed=True)):

    result = []
    error = []
    r = await request.json()

    for item in r['data']:
        if len(item) == 4 and ('order_id' and 'weight' and 'region' and 'delivery_hours' in item):
            continue
        else:
            error.append({'id': item['order_id']})

    if len(error) >= 1:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"validation_error": {"orders": error}}

    for item in data:
        query = 'INSERT INTO orders VALUES(:order_id, :weight, :region, :delivery_hours)'
        values = {
            "order_id": item.order_id,
            "weight": item.weight,
            "region": item.region,
            "delivery_hours": item.delivery_hours
        }
        await execute(query, False, values)
        result.append({'id': item.order_id})

    return {'orders': result}


@app.post('/orders/assign',
          description='Assign orders to a courier by id')
async def post_orders_assign(courier_id: int = Body(..., embed=True)):

    query = f'SELECT * FROM couriers WHERE courier_id={courier_id}'
    courier = await fetch(query, False)
    if len(courier) == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST)
    else:
        courier = courier[0]

    region = ''
    count = 1
    for i in courier['regions']:
        if count < len(courier['regions']):
            region += f'{i}, '
            count += 1
        else:
            region += f'{i}'

    if courier['courier_type'] == 'foot':
        query = f'SELECT * FROM orders WHERE weight <= 10 AND region IN ({region})' \
                f' AND complete IS NULL '
    elif courier['courier_type'] == 'bike':
        query = f'SELECT * FROM orders WHERE weight <= 15 AND region IN ({region}) ' \
                f'AND complete IS NULL '
    elif courier['courier_type'] == 'car':
        query = f'SELECT * FROM orders WHERE weight <= 50 AND region IN ({region}) ' \
                f'AND complete IS NULL '
    orders = await fetch(query, False)

    def date_intersection(t1, t2):
        t1start, t1end = t1[0], t1[1]
        t2start, t2end = t2[0], t2[1]
        return (t1start <= t2start <= t1end) or (t2start <= t1start <= t2end)

    available = []
    for order in range(len(orders)):
        available.append({'order_id': orders[order]['order_id'], 'good': []})
        for a in range(len(courier['working_hours'])):
            for b in range(len(orders[order]['delivery_hours'])):
                d1 = time(hour=int(courier['working_hours'][a].split('-')[0].split(':')[0]),
                          minute=int(courier['working_hours'][a].split('-')[0].split(':')[1]))
                d2 = time(hour=int(courier['working_hours'][a].split('-')[1].split(':')[0]),
                          minute=int(courier['working_hours'][a].split('-')[1].split(':')[1]))
                d3 = time(hour=int(orders[order]['delivery_hours'][b].split('-')[0].split(':')[0]),
                          minute=int(orders[order]['delivery_hours'][b].split('-')[0].split(':')[1]))
                d4 = time(hour=int(orders[order]['delivery_hours'][b].split('-')[1].split(':')[0]),
                          minute=int(orders[order]['delivery_hours'][b].split('-')[1].split(':')[1]))
                available[order]['good'].append(date_intersection((d1, d2), (d3, d4)))

    result = []
    assign_time = ''
    courier_type = courier['courier_type']
    for order in available:
        if True in order['good']:
            order_id = order['order_id']
            assign_time = str(datetime.now())
            query = f'UPDATE orders SET assign={courier_id}, courier_type=\'{courier_type}\'' \
                    f', assign_time=TIMESTAMP\'{assign_time}\' WHERE order_id={order_id}'
            await execute(query, False)
            result.append({"id": order_id})

    if len(result) == 0:
        return {'orders': result}
    else:
        return {'orders': result, 'assign_time': assign_time}


@app.post('/orders/complete',
          description='Marks orders as completed')
async def post_orders_complete(courier_id: int = Body(..., embed=True),
                               order_id: int = Body(..., embed=True),
                               complete_time: datetime = Body(..., embed=True)):

    query = f'SELECT * FROM orders WHERE order_id={order_id} AND assign={courier_id}'
    orders = await fetch(query, False)

    if len(orders) == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST)
    else:
        query = f'UPDATE orders SET complete=True, complete_time=TIMESTAMP\'{complete_time}\' WHERE order_id={order_id}'
        await execute(query, False)
        return {'order_id': order_id}


@app.get('/couriers/{courier_id}',
         description='Update courier by id')
async def get_couriers_by_id(courier_id: int):

    query = f'SELECT * FROM couriers WHERE courier_id={courier_id}'
    courier = await fetch(query, False)

    if len(courier) == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST)

    else:
        query = f'SELECT * FROM orders WHERE assign={courier_id} AND complete=True ORDER BY assign_time'
        orders = await fetch(query, False)

        time_complete = {}
        count = 0
        for order in orders:
            if order['region'] not in time_complete:
                time_complete[order['region']] = []
            if count == 0:
                differense = int(order['complete_time'].timestamp() - order['assign_time'].timestamp())
                time_complete[order['region']].append(differense)
                count += 1
            else:
                differense = int(order['complete_time'].timestamp() - orders[count-1]['complete_time'].timestamp())
                time_complete[order['region']].append(differense)
                count += 1

        averege = []
        for region in time_complete:
            averege.append(float(sum(time_complete[region])) / max(len(time_complete[region]), 1))
        if len(averege) > 0:
            rating = (60*60 - min(min(averege), 60*60))/(60*60) * 5
            courier[0]['rating'] = format(rating, '.2f')

        earnings = 0
        for order in orders:
            c = 0
            if order['courier_type'] == 'foot':
                c = 2
            elif order['courier_type'] == 'bike':
                c = 5
            elif order['courier_type'] == 'car':
                c = 9
            earnings += 500 * c
        courier[0]['earnings'] = earnings

        return courier[0]

if __name__ == "__main__":
    uvicorn.run("run:app", host="0.0.0.0", port=8080, log_level="info")
