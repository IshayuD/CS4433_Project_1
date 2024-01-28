import math
from datetime import datetime, date, timedelta, time
import random
import pandas as pd
from faker import Faker
import time


# Config constants
NUM_PEOPLE = 20
NUM_FRIENDS = 40
NUM_ACCESSES = 50

START = date(2004, 2, 4)
END = date.today()

START_TIME = datetime.combine(START, datetime.min.time())
END_TIME = datetime.combine(END, datetime.min.time())


def create_random_date(start_date, end_date):
	delta = end_date - start_date
	int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
	random_second = random.randint(0, int_delta)
	return start_date + timedelta(seconds=random_second)


def secsToMinSecs(secs) -> str:
	mins = math.floor(secs/60)
	spareSecs = round(secs % 60, 2)
	return "{}m {}s".format(mins, spareSecs)


def gen_mypage(num_people=NUM_PEOPLE) -> pd.DataFrame:
	ids = []
	names = []
	nationalities = []
	country_codes = []
	hobby = []

	# Generate fake info
	fake = Faker()
	hobbies = pd.read_csv("input/hobbies.csv")["hobby"]
	nat = pd.read_csv("input/nationalities.csv", encoding='latin1')

	start_time = time.time()

	for iterator in range(num_people):
		ids.append(iterator)
		names.append(fake.name())
		nationalities.append(nat.sample()['country'].values[0])
		country_codes.append(int(nat.sample()['code'].values[0]))
		hobby.append(hobbies.sample().values[0])
		timeTaken = time.time() - start_time
		timeRemaining = ((timeTaken * num_people) / (iterator + 1)) - timeTaken
		print("\rFinished {}/{} rows ({}%) in {}. About {} remaining"
			.format(iterator + 1, num_people, round(100 * ((iterator + 1) / num_people), 2), secsToMinSecs(timeTaken),
				secsToMinSecs(timeRemaining)), end='')

	print("")

	# Add data to dataframe
	data_frame = pd.DataFrame({
		"PersonID": ids,
		"Name": names,
		"Nationality": nationalities,
		"Country Code": country_codes,
		"Hobby": hobby
	})

	return data_frame


def gen_friends(people: pd.DataFrame, num_friends: int = NUM_FRIENDS) -> pd.DataFrame:

	num_people = people.shape[0]

	data = {
		"FriendRel": [],
		"PersonID": [],
		"MyFriend": [],
		"DateOfFriendship": [],
		"Desc": []
	}

	start_time = time.time()

	relationship_types = ["Friend", "Father", "Mother", "Son", "Daughter", "Uncle", "Aunt", "Cousin", "Brother", "Sister"]
	numForRange = num_people + 1
	list_of_IDS = range(0, numForRange, 1)

	# Add data to dataframe
	for i in range(num_friends):
		# options for relationship
		thisDesc = random.choice(relationship_types)

		# personId and Friend generation - should not repeat
		random_combination = random.sample(list_of_IDS, 2)
		creationOfFriend = create_random_date(START, END)

		data["FriendRel"].append(i)
		data["PersonID"].append(random_combination[0])
		data["MyFriend"].append(random_combination[1])
		data["DateOfFriendship"].append(creationOfFriend)
		data["Desc"].append(thisDesc)

		nDone = i+1
		timeTaken = time.time() - start_time
		timeRemaining = ((timeTaken * num_friends) / nDone) - timeTaken
		print("\rFinished {}/{} rows ({}%) in {}. About {} remaining"
		      .format(nDone, num_friends, round(100 * (nDone / num_friends), 2), secsToMinSecs(timeTaken),
		              secsToMinSecs(timeRemaining)), end='')

	print("")

	data_frame = pd.DataFrame(data)
	data_frame.set_index("FriendRel")

	return data_frame


def gen_accesslog(people: pd.DataFrame, num_accesses: int = NUM_ACCESSES) -> pd.DataFrame:

	num_people = people.shape[0]

	data = {
		"AccessID": [None]*num_accesses,
		"ByWho": [None]*num_accesses,
		"WhatPage": [None]*num_accesses,
		"TypeOfAccess": [None]*num_accesses,
		"AccessTime": [None]*num_accesses
	}

	access_types = ["Viewed Profile", "Shared Post", "Left a Comment", "Liked Post", "Followed Profile"]

	start_time = time.time()

	# Add data to dataframe
	for i in range(num_accesses):
		# Ensures no one accesses their own page
		ids = random.sample(range(num_people), 2)

		random_time = create_random_date(START_TIME, END_TIME).timestamp()

		data["AccessID"][i] = i
		data["ByWho"][i] = ids[0]
		data["WhatPage"][i] = ids[1]
		data["TypeOfAccess"][i] = random.choice(access_types)
		data["AccessTime"][i] = datetime.fromtimestamp(random_time)

		nDone = i+1
		timeTaken = time.time() - start_time
		timeRemaining = ((timeTaken * num_accesses) / nDone) - timeTaken
		print("\rFinished {}/{} rows ({}%) in {}. About {} remaining"
		      .format(nDone, num_accesses, round(100 * (nDone / num_accesses), 2), secsToMinSecs(timeTaken),
		              secsToMinSecs(timeRemaining)), end='')

	print("")

	data_frame = pd.DataFrame(data)
	data_frame.set_index("AccessID")

	return data_frame


if __name__ == "__main__":
	print("Generating people...")
	mypage = gen_mypage()
	mypage.to_csv("output/pages.csv", index=False)

	print("Generating friends...")
	friends = gen_friends(mypage)
	friends.to_csv("output/friends.csv", index=False)

	print("Generating access...")
	accessLog = gen_accesslog(mypage)
	accessLog.to_csv("output/access_logs.csv", index=False)
