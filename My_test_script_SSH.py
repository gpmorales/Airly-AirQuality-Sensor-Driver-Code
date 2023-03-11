#Modules
import Adafruit_DHT
import datetime
import serial
import csv
import time
import paramiko


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
        print("Dylos sensor not working")
    return PM25,PM10




''' ------------- Write Data to CSV sheet -> Monthly sheets ------------ '''
def cssv(alldata):
    try:
        month=datetime.datetime.now().month
        year=datetime.datetime.now().year
        #CSV's will have format "Mar2023"

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

        fileName = month + str(year) + ".csv"

        if(newMonth()):
            with open('{}{}.csv'.format(month,year), 'a', newline='') as csv_file:

                writer = csv.writer(csv_file, delimiter=',')
                writer.writerow(["date", "pm10", "pm25", "Temperature", "Humidity"])
                writer.writerow(alldata)

            print("Wrote to {}".format(fileName))

        else:
            with open('{}{}.csv'.format(month,year), 'a', newline='') as csv_file:

                writer = csv.writer(csv_file, delimiter=',')
                writer.writerow(alldata)

            print("Wrote to {}".format(fileName))

        return fileName;

    except Exception as e:
        print(e)
        print("Failed to log to CSV")




''' ------------------------- UPLOAD CSV SHEET TO CLUSTER ------------------------- '''

''' FIRST -> Secure Shell (SSH) to our Emory MSC Cluster and return SFTP client'''
def Establish_SFTP_Connection():
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname = "lab0z.mathcs.emory.edu", username = "gpmoral", password = "gMC12345#$")
        sftp_client = ssh_client.open_sftp()

        print("Successfully Connected to Cluster ... Returning SFTP & SSH clients")
        return [sftp_client, ssh_client];

    except Exception as e:
        print(e)
        print("Failed to Connect To Cluster")


''' Second-> Use our Secure File Transfer Protocol (SFTP) Client to upload CSV sheet to our MSC Cluster '''
def UPLOAD_CSV(sftp_client, ssh_client, csv_name):
    try:
        sftp_client.put(csv_name, "/home/gpmoral/{}".format(csv_name))

        print("Successfully Uploaded CSV To Cluster")

        #Close all connections
        sftp_client.close()
        ssh_client.close()
        print("Closed Clients...")
        return;

    except Exception as e:
        print(e)
        print("Failed to Upload To Cluster")




''' ---------------- Hour Counter, returns true whenever an hour has passed/we enter a new hour -------------- '''
curMonth = datetime.datetime.now().month
curHr = datetime.datetime.now().hour # current Hour

def hasBeenHour():
    #If the hour has changed (an hour has passed), update our current Hour and return true
    global curHr
    if(curHr != datetime.datetime.now().hour):
        curHr = datetime.datetime.now().hour
        return True
    else:
        #Otherwise an hour has not passed 
        return False

def newMonth():
    global curMonth
    #if the month has changed, we will make a
    if(curMonth != datetime.datetime.now().month):
        curMonth = datetime.datetime.now().month
        return True;
    else:
        return False



''' GOALS -> WE WANT TO CREATE  NEW CSV SHEET EVEYR NEW MONTH '''
# -> WE WANT TO WRTIE TO CSV THROUGHT THE DAY, AND THEN AFTER 24 HOURS UPLOAD/UPDATE via SFTP to MSC Cluster

#Log to CSV 
while True:
    ''' UNCOMMENT'''
    #temperature, humidity = dhtdata()
    PM25,PM10=dylosdata()

    #Line below: writes timestamp all in one cell
    date = str(datetime.datetime.now())

    #Alternatively, we can have the year, month, day, hour, minute, second writen in separate columns. For that, uncomment below
    #year=datetime.datetime.now().year
    #month=datetime.datetime.now().month
    #day=datetime.datetime.now().day
    #hour=datetime.datetime.now().hour
    #minute=datetime.datetime.now().minute
    #change all data to include each unit of time

    
    alldata=[date, pm10, pm25,temperature, humidity]
    
    csvPath = cssv(alldata);

    # UPLOAD UPDATED CSV SHEET TO CLUSTER EVERY 24 HOURS (WILL OVERWRITE PREV EXISTING ONE)
    if(hasBeenHour()):
        serverClients = Establish_SFTP_Connection()
        SFTPClient = serverClients[0]
        SSHClient = serverClients[1]
        UPLOAD_CSV(SFTPClient, SSHClient, csvPath)

    time.sleep(60.0 - ((time.time()-starttime)%60))

''' END OF SCRIPT '''
