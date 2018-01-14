import csv

READ_FP = '../data/meteorite_landings.csv'
WRITE_FP = '../data/meteorite_landings_latlon_noempty.csv'
LAT_IND = 7
LON_IND = 8

with open(READ_FP) as read:
	reader = csv.reader(read)
	with open(WRITE_FP, 'w') as out:
		writer = csv.writer(out)
		for row in reader:
			try:
				lat = float(row[LAT_IND])
				lon = float(row[LON_IND])
				if lat != 0 or lon != 0:
					writer.writerow((lat, lon))
			except ValueError:
				pass
	# print max_mass, name
