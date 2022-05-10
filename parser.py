import simplejson as json
from ftplib import FTP
import os
from zipfile import ZipFile
from io import StringIO
from io import BytesIO
from datetime import datetime
from datetime import timedelta
import emlparse
import logresults
import schedule
import time

#Generates a list of zip files from the AEC
# 2013 id = '17496'
# 2016 address = results.aec.gov.au
# 2016 id = '20499'
# 2016 address = mediafeed.aec.gov.au

print("CHANGE TO mediafeed.aec.gov.au ON ELECTION NIGHT JFC")

# 2019 election is 24310
# 2016 is 20499

verbose = False
feedtest = True
electionID = '24310'
testTime = datetime.strptime("2016-07-02 19:00","%Y-%m-%d %H:%M")
path = '/{electionID}/Standard/Verbose/'.format(electionID=electionID)


if feedtest:
	print("yeh")
	ftpPath = 'mediafeedarchive.aec.gov.au'
else:
	print("nah")
	ftpPath = 'mediafeed.aec.gov.au'	

print("using ", ftpPath)
def parse_results(test):
	print("Logging in to AEC FTP")

	print(ftpPath)
	ftp = FTP(ftpPath)
	ftp.login()

	# print("yep")
	ftp.cwd(path)

	my_files = []

	def get_filenames(ln):
		# global my_files
		cols = ln.split(' ')
		objname = cols[len(cols)-1] # file or directory name
		if objname.endswith('.zip'):
			my_files.append(objname) # full path

	print("Getting all the filenames")

	ftp.retrlines('LIST', get_filenames)

	# try:
	# 	ftp.retrlines('LIST', get_filenames)
	
	# except BrokenPipeError as e:
	# 	print(e)
	# 	print("Can't reach the AEC server, retrying in 20 seconds")
	# 	time.sleep(20)
	# 	ftp = FTP(ftpPath)
	# 	ftp.login()
	# 	ftp.cwd(path)
	# 	ftp.retrlines('LIST', get_filenames)

	timestamps = []

	if verbose:
		print(my_files)

	#Get latest timestamp

	print("Getting latest timestamp")

	for f in my_files:
		timestamp = f.split("-")[-1].replace(".zip","")

		if test:
			if datetime.strptime(timestamp,"%Y%m%d%H%M%S") < testTime:
				# print("test time is ", testTime)
				if verbose:
					print(timestamp)
				timestamps.append(datetime.strptime(timestamp,"%Y%m%d%H%M%S"))
		else:
			if verbose:
				print(timestamp)

			timestamps.append(datetime.strptime(timestamp,"%Y%m%d%H%M%S"))

	latestTimestamp = max(timestamps)
	latestTimestampStr = datetime.strftime(latestTimestamp, '%Y%m%d%H%M%S')

	print("latest timestamp is", latestTimestamp)

	# Check if results log exists

	if os.path.exists('recentResults.json'):

		# Get recent timestamps of results

		with open('recentResults.json','r') as recentResultsFile:
			recentResults = json.load(recentResultsFile)

		print(recentResults)

		# Check if we have it or not

		if latestTimestampStr not in recentResults:
			
			print("{timestamp} hasn't been saved, saving now".format(timestamp=latestTimestampStr))
			
			#Get latest file

			latestFile = "aec-mediafeed-Standard-Verbose-{electionID}-{timestamp}.zip".format(electionID=electionID,timestamp=datetime.strftime(latestTimestamp, '%Y%m%d%H%M%S'))
			r = BytesIO()

			print('Getting ' + latestFile)

			#Get file, read into memory

			ftp.retrbinary('RETR ' + latestFile, r.write)
			input_zip=ZipFile(r, 'r')
			ex_file = input_zip.open("xml/aec-mediafeed-results-standard-verbose-" + electionID + ".xml")
			content = ex_file.read()
			
			# print content

			print("Parsing the feed into JSON")

			emlparse.eml_to_JSON(content,'media feed',False,latestTimestampStr,test)
			logresults.saveRecentResults(latestTimestampStr, test)

		if latestTimestampStr in recentResults:
			print("{timestamp} has already been saved".format(timestamp=latestTimestampStr))

	# It doesn't exist, so treat timestamp as first

	else:
		print("Results file not found, saving {timestamp} as first entry".format(timestamp=latestTimestampStr))
			
		#Get latest file

		latestFile = "aec-mediafeed-Standard-Verbose-{electionID}-{timestamp}.zip".format(electionID=electionID,timestamp=datetime.strftime(latestTimestamp, '%Y%m%d%H%M%S'))
		r = BytesIO()

		print('Getting ' + latestFile)

		#Get file, read into memory

		ftp.retrbinary('RETR ' + latestFile, r.write)
		input_zip = ZipFile(r, 'r')
		ex_file = input_zip.open("xml/aec-mediafeed-results-standard-verbose-" + electionID + ".xml")
		content = ex_file.read()
		
		# print content

		print("Parsing the feed into JSON")

		emlparse.eml_to_JSON(content,'media feed',False,latestTimestampStr, test)
		logresults.saveRecentResults(latestTimestampStr, test)

	print("Done, results all saved")
	ftp.quit()

# Use scheduler to time function every 2 minutes

parse_results(False)

schedule.every(2).minutes.do(parse_results,False)

while True:
    schedule.run_pending()
    time.sleep(1)
    print(datetime.now())

# Test function, counts from 6 pm to 11 pm on election night 2013    

# def runTest():
# 	global testTime
# 	endTime = datetime.strptime("2016-07-02 23:00","%Y-%m-%d %H:%M")
# 	parse_results(True)
# 	schedule.every(2).minutes.do(parse_results,True)
	
# 	while testTime < endTime:
# 		schedule.run_pending()
# 		testTime = testTime + timedelta(seconds=1)
# 		print(testTime)
# 		time.sleep(1)


# runTest()

# parse_results(True)
# ftp.quit()

