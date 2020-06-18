import uuid
import random
import datetime

countries = ['BR', 'US', 'FR', 'UK', 'NL', 'NZ', 'CA', 'HR', 'DK', 'GR']

def gen_ids(n_ids):
	rd = random.Random()
	rd.seed(3272)
	for i in range(n_ids):
		yield (uuid.UUID(int=rd.getrandbits(128)))

def create_userBase(date):
	fileName =  'userBase.txt'
	file = open(fileName, 'w')

	doublesDict = {}

	for sng, uid, contry in date:
		if not (uid in doublesDict):
			doublesDict[uid] = 1
			file.write(uid+'\n')
		else:
			pass

	del doublesDict

def gen_sampleData(date_id, n_rows=10000, n_songs=10000 ,n_users=1000):
	'''userBase = gen_ids(n_users)
	usersDict = check_dataDuplicate(userBase)

	songBase = gen_ids(n_songs)
	songsDict = check_dataDuplicate(songBase)'''

	userBase = gen_ids(n_users)
	songBase = gen_ids(n_songs)

	# Create .log file
	fileName =  'data/' + 'listen-' + str(date_id) + '.log'
	file = open(fileName, 'w')

	currentUser = next(userBase)
	currentSong = next(songBase)

	for i in range(n_rows):
		if (random.random() >= 0.75):
			currentUser = next(userBase)
		
		if (random.random() >= 0.25):
			currentSong = next(songBase)

		data = [
			str(currentSong), 
			str(currentUser),
			countries[random.randint(0, len(countries)-1)]
		]

		if (random.random() >= 0.975):
			data[random.randint(0,2)] = ""
		
		row =  data[0] + "|" + data[1] + "|" + data[2] + "\n"
		
		file.write(row)