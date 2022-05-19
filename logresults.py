import os
import simplejson as json
from datetime import datetime
import boto3

AWS_KEY = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET = os.environ['AWS_SECRET_ACCESS_KEY']

if 'AWS_SESSION_TOKEN' in os.environ:
	AWS_SESSION = os.environ['AWS_SESSION_TOKEN']


def saveRecentResults(timestamp, test):

	# check if file exists already

	jsonObj = []

	if os.path.exists('recentResults.json'):

		print("Results file exists, updating")

		with open('recentResults.json','r') as recentResultsFile:
			
			# Convert the results to a list of datetime objects

			tempList = []
			recentResults = json.load(recentResultsFile)
			for result in recentResults:
				tempList.append(datetime.strptime(result,"%Y%m%d%H%M%S"))

			# Sort it	

			tempList.sort(reverse=True)

			# Check if it's less than 20 and append the new timestamp

			if len(tempList) < 20:

				print("Less than twenty results, appending latest now")

				tempList.append(datetime.strptime(timestamp,"%Y%m%d%H%M%S"))
				tempList.sort(reverse=True)
				for temp in tempList:
					jsonObj.append(datetime.strftime(temp, '%Y%m%d%H%M%S'))	
				print(jsonObj)

			# If it's 20, remove the oldest timestamp, then append the new one	

			elif len(tempList) == 20:

				print("Twenty results, removing oldest and appending newest")

				del tempList[-1]
				tempList.append(datetime.strptime(timestamp,"%Y%m%d%H%M%S"))
				tempList.sort(reverse=True)
				for temp in tempList:
					jsonObj.append(datetime.strftime(temp, '%Y%m%d%H%M%S'))	
				print(jsonObj)
					
		# Write the new version
		newJson = json.dumps(jsonObj, indent=4)
		with open('recentResults.json','w') as fileOut:
				fileOut.write(newJson)				

		print("Finished saving results log locally")

		print("Connecting to S3")
		bucket = 'gdn-cdn'
		if 'AWS_SESSION_TOKEN' in os.environ:
			session = boto3.Session(
			aws_access_key_id=AWS_KEY,
			aws_secret_access_key=AWS_SECRET,
			aws_session_token = AWS_SESSION
			)
		else:
			session = boto3.Session(
			aws_access_key_id=AWS_KEY,
			aws_secret_access_key=AWS_SECRET,
			)
		s3 = session.resource('s3')
		if test:
			key = "2022/05/aus-election/results-data-test/recentResults.json"
		else:
			key = "2022/05/aus-election/results-data/recentResults.json"

		object = s3.Object(bucket, key)
		object.put(Body=newJson, CacheControl="max-age=30", ACL='public-read', ContentType="application/json")
		print("Done, JSON is updated")

	# Otherwise start a new file		

	else:
		print("No results file, making one now")
		jsonObj.append(timestamp)
		newJson = json.dumps(jsonObj, indent=4)
		with open('recentResults.json','w') as fileOut:
				fileOut.write(newJson)

		print("Finished creating results log")

		print("Connecting to S3")
		bucket = 'gdn-cdn'
		if 'AWS_SESSION_TOKEN' in os.environ:
			session = boto3.Session(
			aws_access_key_id=AWS_KEY,
			aws_secret_access_key=AWS_SECRET,
			aws_session_token = AWS_SESSION
			)
		else:
			session = boto3.Session(
			aws_access_key_id=AWS_KEY,
			aws_secret_access_key=AWS_SECRET,
			)
		s3 = session.resource('s3')
		if test:
			key = "2022/05/aus-election/results-data-test/recentResults.json"
		else:
			key = "2022/05/aus-election/results-data/recentResults.json"	
		object = s3.Object(bucket, key)
		object.put(Body=newJson, CacheControl="max-age=30", ACL='public-read', ContentType="application/json")
		print("Done")		

# saveRecentResults()
