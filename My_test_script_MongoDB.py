#Modules
import Adafruit_DHT
import datetime
import serial
import csv
import time
from pymongo import MongoClient


#Global Variables:
starttime=time.time()
temperature=-9999
humidity=-9999
PM25=-9999
pm10=-9999

#Define Variables
def dhtdata():
    try:
        sensor=22
        pin=4
        humidity,temperature = Adafruit_DHT.read(sensor, pin)
    except:
        print('sensor or pin error, try checking GPIOpin#')

    return temperature, humidity




def dylosdata():
    serialport=serial.Serial('//dev/ttyUSB0',
                             baudrate=9600,
                             parity=serial.PARITY_NONE,
                             bytesize=serial.EIGHTBITS,
                             timeout=None,
                             writeTimeout=1,)
    y=serialport.readline().strip()
    PM25,PM10=[int(_) for _ in y.decode('ascii').strip().split(',')]
    try:
        print(PM25,PM10)
    except:
        print("not working1")
    return PM25,PM10




''' Wrtie Data to CSV sheet -> Monthly sheets '''
def cssv(alldata):
    try:
        month=datetime.datetime.now().month
        year=datetime.datetime.now().year

        if(month == 1):
            month = 'Jan'
        if(month == 2):
            month = 'Feb'
        if(month == 3):
            month = 'Mar'
        if(month == 4):
            month = 'Apr'
        if(month == 5):
            month = 'May'
        if(month == 6):
            month = 'Jun'
        if(month == 7):
            month = 'July'
        if(month == 8):
            month = 'Aug'
        if(month == 9):
            month = 'Sep'
        if(month == 10):
            month = 'Oct'
        if(month == 11):
            month = 'Nov'
        if(month == 12):
            month = 'Dec'

        with open('{}{}.csv'.format(month,year), 'a', newline='') as csv_file:

            writer= csv.writer(csv_file, delimiter=',')
            writer.writerow(alldata)

        print("wrote to csv")

    except:
        print("Failed to log to CSV")




''' Establish a connection to MongoDB '''
def ConnectToMongoDB():
	try:
		client = MongoClient("mongodb+srv://gpmoral:gMC20002@cluster0.paxidwi.mongodb.net/sample_mflix?retryWrites=true&w=majority")

		DB = client["sample_mflix"];
		collection = DB["Users"];
		print("Connected to DB")
		return collection;

	except Exception as e:
		print(e)
		print("Failed to connect to DB")



''' After establishing connection to MongoDB, Transmit Data '''
def SendToMongo(Collection, alldata):
	try:
		timestamp = alldata[0]
		temperature = alldata[1]
		humidity = alldata[2]
		pm25 = alldata[3];
		pm10 = alldata[4];

		Airly_Parcel = {
            "TimeStamp" : timestamp,
            "Temperature" : temperature,
            "Humidity" : humidity,
            "PM25" : pm25,
            "PM10" : pm10,
		}

		Collection.insert_one(Airly_Parcel);

	except Exception as e:
		print(e)
		print("Failed to upload to DB")



#Log to CSV and Connect to DB
COLLECTION = ConnectToMongoDB()

while True:
    temperature, humidity= dhtdata()
    PM25,PM10=dylosdata()

    #Line below: writes timestamp all in one cell
    timestamp=str(datetime.datetime.now())

    #Alternatively, we can have the year, month, day, hour, minute, second writen in separate columns. For that, uncomment below
    #year=datetime.datetime.now().year
    #month=datetime.datetime.now().month
    #day=datetime.datetime.now().day
    #hour=datetime.datetime.now().hour
    #minute=datetime.datetime.now().minute
    #change all data to include each unit of time

    alldata=[timestamp,temperature, humidity, PM25, PM10]

    cssv(alldata);

    SendToMongo(COLLECTION, alldata);

    time.sleep(60.0 - ((time.time()-starttime)%60))

''' END OF SCRIPT '''
