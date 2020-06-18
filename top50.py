import numpy as np
import collections

import os
import psutil
import time

process = psutil.Process(os.getpid())

Streamming = collections.namedtuple(
	"Stream",
	("sng_id", "user_id", "country")
)

# limiter
limiter = lambda gen, limit: (data for _, data in zip(range(limit), gen))

def open_log(filePath):
	'''
		File reader generator
		Inputs:
			- filePath (String): system path of file
		Output:
			- Generator
	'''
	file = open(filePath)

	with open(filePath) as file:
		for row in file:
			sng_id, user_id, country = row[:-1].split('|')

			yield Streamming(sng_id, user_id, country)

def get_top50_one_day(streams):
	'''
		Function that iterates over generator and calcule top50 for each country
		Inputs:
			- streams (Generator object): a generator object that contains the data
		Output:
			- top50 (Dictionary): a dictionary with the top50 of each country and a reference for the 50th element (pivot)
	'''

	start_time = time.time()

	# Dictionaries
	country_sngCount = {} # Song counter per country
	top50 = {} # Top50 per country

	for stream in streams:
		# Get attributes for each line in .log file generator
		sng_id, user_id, country = stream

		# Check data conformity
		if ((sng_id != '') and (user_id != '') and (country !='')):
			# Check if there is count dictionary for the county
			if not (country in country_sngCount):
				# Create count dictionary for new country
				country_sngCount[country] = {}

				# Create top50 dictionary for new country
				top50[country] = {'top50_list': {sng_id: 1}, "ref_idx": [sng_id,1]}
				
			if (sng_id in country_sngCount[country]):
				# Increse number of streams of song for each country
				country_sngCount[country][sng_id] += 1
			else:
				# Insert song in stream count for each country
				country_sngCount[country][sng_id] = 1

			# Count bigger than 50th value count in top50
			if (country_sngCount[country][sng_id] > top50[country]['ref_idx'][1]):
				# If not in top50
				if not (sng_id in top50[country]['top50_list']):
					# If top50 is full
					if (len(top50[country]['top50_list']) == 50):
						# Delete the min value for top50
						#print (top50[country]['ref_idx'][0])

						# Pop
						del top50[country]['top50_list'][top50[country]['ref_idx'][0]]

						# Set min reference with new min value for top50
						#top50[country]['ref_idx'] = [sng_id, country_sngCount[country][sng_id]]

						# Update top50 list with new min value
						top50[country]['top50_list'][sng_id] = country_sngCount[country][sng_id]

						# Check for a new pivot
						new_ref_idx = min(top50[country]['top50_list'], key=top50[country]['top50_list'].get)

						top50[country]['ref_idx'] = [new_ref_idx, top50[country]['top50_list'][new_ref_idx]]

					elif (len(top50[country]['top50_list']) < 50):
						# If less than 50 values on top50 list add to list
						top50[country]['top50_list'][sng_id] = country_sngCount[country][sng_id]
					else:
						pass

				# If is in top50 list
				else:
					# Update value in the top50 dictionary
					top50[country]['top50_list'][sng_id] += 1

					# If it is the 50th element, then updates it
					if (sng_id == top50[country]['ref_idx'][0]):
						top50[country]['ref_idx'][1] += 1

					# Check for a new pivot
					new_ref_idx = min(top50[country]['top50_list'], key=top50[country]['top50_list'].get)

					top50[country]['ref_idx'] = [new_ref_idx, top50[country]['top50_list'][new_ref_idx]]
			else:
				pass

	# View memory usage & runtime
	print("Memory usage to calculate top50: ", process.memory_info().rss/(10**9))
	print("--- %s seconds ---" % (time.time() - start_time))

	# Delete dictionary to free memory
	del country_sngCount

	return (top50)

def get_top50_period(dateBegin, dateEnd):
	'''
		Calculate top50 of a given period
		Inputs:
			- dateBegin (Integer): begin date
			- dateEnd (Integer): end date
		Output:
			- None: write results in files
	'''

	# Iterate over period	
	for date in range(dateBegin, dateEnd+1):
		dataPath = 'data/' + 'listen-' + str(date) + '.log'

		# Open log file
		dayLog = open_log(dataPath)

		print ("Getting top50 for ", date)
		top50_per_country = get_top50_one_day(dayLog)

		# File to write
		fileName =  'top50_daily/' + 'country_top50_' + str(date) + '.txt'
		file = open(fileName, 'w')

		for country in top50_per_country:
			top50_row = str(country) + "|"
			for sng_ind, count in zip(top50_per_country[country]['top50_list'].keys(), top50_per_country[country]['top50_list'].values()):
				top50_row = top50_row + str(sng_ind) + ":" + str(count) + ','

			top50_row = top50_row[:-1]
			top50_row = top50_row + '\n'
			file.write(top50_row)

	print("Top50 done!")

def get_top50_userBase(dateBegin, dateEnd):
	# Iterate over period	
	for date in range(dateBegin, dateEnd+1):
		dataPath = 'data/' + 'listen-' + str(date) + '.log'

		# Open log file
		dayLog = open_log(dataPath)

		userBase = open('userBase.txt')
		
		# File to write
		fileName =  'top50_user/' + 'user_top50_' + str(date) + '.txt'
		file = open(fileName, 'w') 

		for user in userBase:
			user = user[:-1]
			top50_user = get_top50_user(user, dayLog)
			top50_user_row = user + "|"

			for sng_id, count in zip(top50_user[user]['top50_list'].keys(), top50_user[user]['top50_list'].values()):
				top50_user_row = top50_user_row + str(sng_id) + str(count) + ','

			top50_user_row = top50_user_row[:-1]
			top50_user_row = top50_user_row + '\n'
			file.write(top50_user_row)

def get_top50_user(uid, streams):
	start_time = time.time()

	# Dictionaries
	user_sngList = {} # Song counter per country
	top50 = {} # Top50 per country

	for stream in streams:
		# Get attributes for each line in .log file generator
		sng_id, user_id, country = stream

		# Check data conformity
		if ((sng_id != '') and (user_id != '') and (country !='')):
			if (user_id == uid):
				# Check if there is count dictionary for the county
				if not (sng_id in user_sngList):
					# Create count dictionary for new song
					user_sngList[sng_id] = 1

					# Create top50 dictionary for new song
					top50[uid] = {'top50_list': {sng_id: 1}, "ref_idx": [sng_id,1]}
				else:
					user_sngList[sng_id] += 1

				# Count is bigger than 50th value count in top50
				if (user_sngList[sng_id] > top50[uid]['ref_idx'][1]):
					# If not in top50
					if not (sng_id in top50[uid]['top50_list']):
						# If top50 is full
						if (len(top50[uid]['top50_list']) == 50):
							# Delete the min value for top50
							del top50[uid]['top50_list'][top50[uid]['ref_idx'][0]]

							# Update top50 list with new min value
							top50[uid]['top50_list'][sng_id] = user_sngList[sng_id]

							# Check for a new pivot
							new_ref_idx = min(top50[uid]['top50_list'], key=top50[uid]['top50_list'].get)

							top50[uid]['ref_idx'] = [new_ref_idx, top50[uid]['top50_list'][new_ref_idx]]

						elif (len(top50[uid]['top50_list']) < 50):
							# If less than 50 values on top50 list add to list
							top50[uid]['top50_list'][sng_id] = user_sngList[sng_id]
						else:
							pass
					# If is in top50 list
					else:
						# Update value in the top50 dictionary
						top50[uid]['top50_list'][sng_id] += 1

						# If it is the 50th element, then updates it
						if (sng_id == top50[uid]['ref_idx'][0]):
							top50[uid]['ref_idx'][1] += 1

						# Check for a new pivot
						new_ref_idx = min(top50[uid]['top50_list'], key=top50[uid]['top50_list'].get)

						top50[uid]['ref_idx'] = [new_ref_idx, top50[uid]['top50_list'][new_ref_idx]]
				else:
					pass

	# View memory usage & runtime
	print("Memory usage to calculate top50: ", process.memory_info().rss/(10**9))
	print("--- %s seconds ---" % (time.time() - start_time))

	# Delete dictionary to free memory
	del user_sngList

	return (top50)