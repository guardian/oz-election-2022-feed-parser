import json
import xmltodict
import boto3
import os

AWS_KEY = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET = os.environ['AWS_SECRET_ACCESS_KEY']

if 'AWS_SESSION_TOKEN' in os.environ:
	AWS_SESSION = os.environ['AWS_SESSION_TOKEN']

def convertPartyCode(partycode):
	partyCodes = {'LP':'LIB', 'NP':'NAT'}
	if partycode in partyCodes:
		return partyCodes[partycode]
	else:
		return partycode	

def candidate_party(candidate,candidateType):
	if candidateType == 'short':
		if 'eml:AffiliationIdentifier' in candidate:
			return candidate['eml:AffiliationIdentifier']['@ShortCode']
		else:
			return 'IND'
	if candidateType == 'long':
		if 'eml:AffiliationIdentifier' in candidate:
			return candidate['eml:AffiliationIdentifier']['eml:RegisteredName']
		else:
			return 'Independent'

def eml_to_JSON(eml_file, type,local,timestamp, test):
	
	#convert xml to json
	
	if local:
		elect_data = xmltodict.parse(open(eml_file, 'rb'))
	else:
		elect_data = xmltodict.parse(eml_file)	
	
	if type == "media feed":
	  
		#parse house of reps
		results_json = {}
		summary_json = {}
		swing_list = []
		electorates_list = []

		for election in elect_data['MediaFeed']['Results']['Election']:
			# House of Representative contests
			
			if 'House' in election:
				# National summary
				results_json['enrollment'] = int(election['House']['Analysis']['National']['Enrolment'])
				results_json['votesCountedPercent'] = float(election['House']['Analysis']['National']['FirstPreferences']['Total']['Votes']['@Percentage'])
				results_json['votesCounted'] = int(election['House']['Analysis']['National']['FirstPreferences']['Total']['Votes']['#text'])

				natSwing = election['House']['Analysis']['National']['TwoPartyPreferred']

				results_json['nationalSwing'] = {}

				for coalition in election['House']['Analysis']['National']['TwoPartyPreferred']['Coalition']:
					print(coalition)
					if coalition['CoalitionIdentifier']['@ShortCode'] == "LNC":
						results_json['nationalSwing']['tppCoalition'] = float(coalition['Votes']['@Swing'])
					if coalition['CoalitionIdentifier']['@ShortCode'] == "ALP":
						results_json['nationalSwing']['tppLabor'] = float(coalition['Votes']['@Swing'])	

				partyNational = election['House']['Analysis']['National']['FirstPreferences']['PartyGroup']
					
				results_json['partyNationalResults'] = [
					{
						'partygroup_id': int(partygroup['PartyGroupIdentifier']['@Id']),
						'partygroup_name': partygroup['PartyGroupIdentifier']['PartyGroupName'],
						'coalition_short': partygroup['PartyGroupIdentifier']['@ShortCode'],
						'votesTotal': int(partygroup['Votes']['#text']),
						'votesPercent': float(partygroup['Votes']['@Percentage']),
						'swing':float(partygroup['Votes']['@Swing'])
					}
					for partygroup in partyNational
				]				


				summary_json['enrollment'] = int(election['House']['Analysis']['National']['Enrolment'])
				summary_json['votesCountedPercent'] = float(election['House']['Analysis']['National']['FirstPreferences']['Total']['Votes']['@Percentage'])
				summary_json['votesCounted'] = int(election['House']['Analysis']['National']['FirstPreferences']['Total']['Votes']['#text'])

				# Division summaries

				for contest in election['House']['Contests']['Contest']:

					electorates_json = {}
					swing_json = {}
					electorates_json['id'] = int(contest['PollingDistrictIdentifier']['@Id'])
					swing_json['id'] = electorates_json['id']
					electorates_json['name'] = contest['PollingDistrictIdentifier']['Name']
					swing_json['name'] = electorates_json['name']

					print(contest['PollingDistrictIdentifier']['Name'])
					electorates_json['state'] = contest['PollingDistrictIdentifier']['StateIdentifier']['@Id']
					swing_json['state'] = electorates_json['state']

					electorates_json['enrollment'] = int(contest['Enrolment']['#text'])
					electorates_json['votesCounted'] = int(contest['FirstPreferences']['Total']['Votes']['#text'])
					candidates = contest['FirstPreferences']['Candidate']
					electorates_json['candidates'] = [
						{
							'candidate_id': int(candidate['eml:CandidateIdentifier']['@Id']),
							'candidate_name': candidate['eml:CandidateIdentifier']['eml:CandidateName'],
							'votesTotal': int(candidate['Votes']['#text']),
							'votesPercent': float(candidate['Votes']['@Percentage']),
							'party_short': convertPartyCode(candidate_party(candidate,'short')),
							'party_long':candidate_party(candidate,'long'),
							'incumbent':candidate['Incumbent']['#text']
						}
						for candidate in candidates
					]
					# print contest['TwoCandidatePreferred']
					if "@Restricted" not in contest['TwoCandidatePreferred'] and "@Maverick" not in contest['TwoCandidatePreferred']:
						twoCandidatePreferred = contest['TwoCandidatePreferred']['Candidate']
						electorates_json['twoCandidatePreferred'] = [
							{
								'candidate_id': int(candidate['eml:CandidateIdentifier']['@Id']),
								'candidate_name': candidate['eml:CandidateIdentifier']['eml:CandidateName'],
								'votesTotal': int(candidate['Votes']['#text']),
								'votesPercent': float(candidate['Votes']['@Percentage']),
								'swing':float(candidate['Votes']['@Swing']),
								'party_short': convertPartyCode(candidate_party(candidate,'short')),
								'party_long':candidate_party(candidate,'long')
							}
							for candidate in twoCandidatePreferred
						]
						swing_json['tcp'] = electorates_json['twoCandidatePreferred']

					elif "@Restricted" in contest['TwoCandidatePreferred']:
						electorates_json['twoCandidatePreferred'] = "Restricted"
						swing_json['tcp'] = electorates_json['twoCandidatePreferred']

					elif "@Maverick" in contest['TwoCandidatePreferred']:
						electorates_json['twoCandidatePreferred'] = "Maverick"
						swing_json['tcp'] = electorates_json['twoCandidatePreferred']						

					twoPartyPreferred = contest['TwoPartyPreferred']['Coalition']
					
					electorates_json['twoPartyPreferred'] = [
						{
							'coalition_id': int(coalition['CoalitionIdentifier']['@Id']),
							'coalition_long': coalition['CoalitionIdentifier']['CoalitionName'],
							'coalition_short': coalition['CoalitionIdentifier']['@ShortCode'],
							'votesTotal': int(coalition['Votes']['#text']),
							'votesPercent': float(coalition['Votes']['@Percentage']),
							'swing':float(coalition['Votes']['@Swing'])
						}
						for coalition in twoPartyPreferred
					]		

					swing_json['tppCoalition'] = electorates_json['twoPartyPreferred'][0]['swing']
					swing_json['tppLabor'] = electorates_json['twoPartyPreferred'][1]['swing']

					# print electorates_json
					electorates_list.append(electorates_json)
					swing_list.append(swing_json)			

				# print electorates_list
				results_json['divisions'] = electorates_list

			if 'Senate' in election:
				pass


		newJson = json.dumps(results_json, indent=4)
		summaryJson = json.dumps(summary_json, indent=4)
		swingJson = json.dumps(swing_list, indent=4)

		# Save the file locally

		with open('{timestamp}.json'.format(timestamp=timestamp),'w') as fileOut:
			print("saving results locally")
			fileOut.write(newJson)	

		with open('{timestamp}-swing.json'.format(timestamp=timestamp),'w') as fileOut:
			print("saving results locally")
			fileOut.write(swingJson)		

		with open('summaryResults.json','w') as fileOut:
			print("saving results locally")
			fileOut.write(summaryJson)		

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
			key = "2022/05/aus-election/results-data-test/{timestamp}.json".format(timestamp=timestamp)
			object = s3.Object(bucket, key)
			object.put(Body=newJson, CacheControl="max-age=90", ACL='public-read', ContentType="application/json")
			print("Done")


			key2 = "2022/05/aus-election/results-data-test/summaryResults.json"	
			object = s3.Object(bucket, key2)
			object.put(Body=summaryJson, CacheControl="max-age=90", ACL='public-read', ContentType="application/json")
			print("Done")


			key3 = "2022/05/aus-election/results-data-test/{timestamp}-swing.json".format(timestamp=timestamp)	
			object = s3.Object(bucket, key3)
			object.put(Body=swingJson, CacheControl="max-age=90", ACL='public-read', ContentType="application/json")
			print("Done")

			print("Done, JSON is uploaded")

		else:
			key = "2022/05/aus-election/results-data/{timestamp}.json".format(timestamp=timestamp)
			object = s3.Object(bucket, key)
			object.put(Body=newJson, CacheControl="max-age=90", ACL='public-read', ContentType="application/json")
			print("Done")


			key2 = "2022/05/aus-election/results-data/summaryResults.json"	
			object = s3.Object(bucket, key2)
			object.put(Body=summaryJson, CacheControl="max-age=90", ACL='public-read', ContentType="application/json")
			print("Done")


			key3 = "2022/05/aus-election/results-data/{timestamp}-swing.json".format(timestamp=timestamp)	
			object = s3.Object(bucket, key3)
			object.put(Body=swingJson, CacheControl="max-age=90", ACL='public-read', ContentType="application/json")
			print("Done")

			print("Done, JSON is uploaded")

# eml_to_JSON('aec-mediafeed-results-standard-verbose-24310.xml','media feed',True,'20190726164221', True)	