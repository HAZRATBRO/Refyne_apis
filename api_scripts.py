import psycopg2 as ps
from flask import Flask, jsonify, request
from flask_restful import Resource, Api
import pandas as pd
from sqlalchemy import create_engine
import datetime as dt


# creating the flask app
app = Flask(__name__)
# creating an API object
api = Api(app)

class User(Resource):
    def __init__(self) -> None:
        self.conn = ps.connect(
            host="localhost",
            database="refyne_task",
            user="postgres",
            password="tigerdb",
        )
        self.init_query = "CREATE TABLE IF NOT EXISTS users(uid INTEGER NOT NULL,cid CHAR(10) unique,b_timestamp TIMESTAMP without time zone NOT NULL,delta_sec INTEGER,CONSTRAINT fk_customer FOREIGN KEY(cid) REFERENCES cars(carId));"
        super().__init__()
    
    def cursor_exec(self , query):
        try:
            cur = self.conn.cursor()
            cur.execute(query)
            cur.close()
        except Exception as e:
            print(e.with_traceback())

    def get(self , uid):
        query = "SELECT * FROM users where uid={}".format(uid)
        cur = self.conn.cursor()
        cur.execute(query)
        res_lis = cur.fetchall()
        res_mp = {}
        res_mp[uid] = {"bookings":[]}
        for i,res in enumerate(res_lis):
            res_map = {}
            res_map['carId'] = res[1]
            res_map['booking_date'] = res[2]
            res_map['duration'] = res[3]
            res_map['bookin_no.'] = i
            res_mp[uid]["bookings"].append(res_map)
        return jsonify(res_mp)

    def post(self):
        json_data = request.get_json(force=True) 
        cid = json_data['carId']
        booking_date = json_data['booking_date']
        duration = json_data['duration']
        uid = json_data['uid'] #replace with uid generation 
        query = "INSERT INTO public.users(uid, cid, b_timestamp, delta_sec) VALUES ({}, '{}', {}, {})".format(uid ,cid, booking_date ,duration)
        self.cursor_exec(query)
        return jsonify("success")
    def put(self):
        json_data = dict(request.get_json(force=True)) 
        query = 'UPDATE users SET'
        for k in json_data.keys():
            query += (' {}={}, '.format(k , json_data[k]))
        query += ' WHERE cid={}'.format(json_data['cid'])
        self.cursor_exec(query)
        return jsonify("success")
    def delete(self):
        json_data = dict(request.get_json(force=True))
        uid = json_data['uid']
        cid = json_data['cid']
        query = "DELETE FROM users WHERE uid='{}' and cid='{}'".format(uid , cid)
        self.cursor_exec(query)
        return jsonify("success")

class Car(Resource):
    def __init__(self) -> None:
        self.conn = ps.connect(
            host="localhost",
            database="refyne_task",
            user="postgres",
            password="tigerdb",
        )
        self.init_query = "CREATE TABLE IF NOT EXISTS users(uid INTEGER NOT NULL,cid CHAR(10) unique,b_timestamp TIMESTAMP without time zone NOT NULL,delta_sec INTEGER,CONSTRAINT fk_customer FOREIGN KEY(cid) REFERENCES cars(carId));"
        super().__init__()
    
    def cursor_exec(self , query):
        try:
            cur = self.conn.cursor()
            cur.execute(query)
            cur.close()
        except Exception as e:
            print(e.with_traceback())

    def get(self , cid):
        query = "SELECT * FROM cars where carid='{}'".format(cid)
        cur = self.conn.cursor()
        cur.execute(query)
        res_lis = cur.fetchall()[0]
        res_map = {}
        res_map['carId'] = res_lis[0]
        res_map['manufacturer'] = res_lis[1]
        res_map['model'] = res_lis[2]
        res_map['b_price.'] = res_lis[3]
        res_map["ph_price"] = res_lis[4]
        res_map["deposit"] = res_lis[5]
        return jsonify(res_map)

    def post(self):
        json_data = request.get_json(force=True) 
        carId = json_data['carId']
        manufacturer = json_data['manufacturer']
        model = json_data['model']
        b_price = json_data['b_price']
        ph_price = json_data['ph_price'] 
        deposit = json_data['deposit']
        query = "INSERT INTO cars(carid, manufacturer, model, b_price, ph_price, deposit) VALUES ('{}', '{}', '{}', {} , {} , {})".format(carId , manufacturer , model , b_price , ph_price ,deposit)
        self.cursor_exec(query)
        return jsonify("success")
    def put(self):
        json_data = dict(request.get_json(force=True)) 
        query = 'UPDATE cars SET'
        for k in json_data.keys():
            query += (' {}={}, '.format(k , json_data[k]))
        query += ' WHERE cid={}'.format(json_data['cid'])
        self.cursor_exec(query)
        return jsonify("success")
    def delete(self):
        json_data = dict(request.get_json(force=True))
        cid = json_data['cid']
        query = "DELETE FROM users WHERE cid={}".format(cid)
        self.cursor_exec(query)
        return jsonify("success")
     
@app.route('/search-cars/', methods = ['GET'])
def searchCars():
   query = 'select * from "users"'
   car = Car()
   dat = request.get_json()
   fromTime = dt.datetime.fromisoformat(dat['from']).timestamp()
   toTime = dt.datetime.fromisoformat(dat['to']).timestamp()
   engine = create_engine('postgresql://postgres:tigerdb@localhost:5432/refyne_task')
   df = pd.read_sql_query(query,con=engine)
   df['b_timestamp'] = (df['b_timestamp'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
   df['to_time'] =  df['b_timestamp'] + pd.to_timedelta(df['delta_sec'] , unit='s')
   df_grps = df.groupby('cid')
   res_map = {"available_cars" : []}
   for cid in df_grps.groups:
       temp_df = df_grps.get_group(cid)
       temp_df = temp_df[(temp_df['b_timestamp'] > fromTime) & (temp_df['to_time'] < toTime) ]
       if not temp_df.empty:
           res_map['available_cars'].append(car.get(cid)) 
   return jsonify(res_map)

@app.route('/car/bookings', methods = ['GET'])
def bookCar():    
   dat = request.get_json()
   query = "select * from 'users' where cid='{}'".format(dat['cid'])
   fromTime = dt.datetime.fromisoformat(dat['from']).timestamp()
   duration = dt.datetime.fromtimestamp(dat['duration'])
   toTime = (fromTime + duration)
   engine = create_engine('postgresql://postgres:tigerdb@localhost:5432/refyne_task')
   df = pd.read_sql_query(query,con=engine)
   df['b_timestamp'] = (df['b_timestamp'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
   df['to_time'] =  df['b_timestamp'] + pd.to_timedelta(df['delta_sec'] , unit='s')
   df['next_booking'] = df['b_timestamp'].shift(-1).fillna(dt.datetime.max)
   if not df[(fromTime > df['to_time']) & (toTime < df['next_booking'])].empty :
       user = User()
       insert_query = "INSERT INTO public.users(uid, cid, b_timestamp, delta_sec) VALUES ({}, '{}', {}, {})".format(dat['uid'] ,dat['cid'], dat['from'] ,duration)
       user.cursor_exec(insert_query)

@app.route('/calculate-price/', methods = ['GET'])
def priceCalc():
   dat = request.get_json()
   car = Car()
   fromTime = dt.datetime.fromisoformat(dat['from']).timestamp()
   toTime = dt.datetime.fromisoformat(dat['to']).timestamp()
   car_dat = car.get(dat['cid'])
   delta = ((toTime - fromTime).total_seconds())//60
   price = (car_dat['ph_price']*delta + car_dat['b_price'] + car_dat['deposit'])
   return jsonify({"price": price})

@app.route('/user/bookings' , methods=['GET'])
def getUserBookings():
    dat = request.get_json()
    uid = int(dat['uid'])
    user = User()
    user_dat = user.get(uid).get_data()
    print(dict((user_dat).decode('utf-8')))
    return jsonify(user_dat[uid]["bookings"])

@app.route('/car/bookings' , methods=['GET'])
def getCarBookings():
    dat = request.get_json()
    cid = dat['cid']
    query = "SELECT * FROM users where cid='{}'".format(cid)
    engine = create_engine('postgresql://postgres:tigerdb@localhost:5432/refyne_task')
    df = pd.read_sql_query(query,con=engine).to_json(orient='records')
    return jsonify(df)
    

#dt.timedelta.total_seconds
api.add_resource(User, '/')
api.add_resource(Car, '/')


if __name__ == '__main__':
    app.run(debug=True)