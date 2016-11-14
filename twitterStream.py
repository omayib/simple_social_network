import tweepy as tw
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import argparse
import string
import config
import json

def getParser():
	parser = argparse.ArgumentParser(description="Twitter streamer")
	parser.add_argument("-q",
			    "--query",
			    dest="query",
			    help="Query/Filter",
			    default='-')
	parser.add_argument("-d",
			    "--data-dir",
			    dest="data_dir",
			    help="Output/Data Directory")
	return parser

class TwitterListener(StreamListener):
	def __init__(self,data_dir, query):
	    query_fname = format_filename(query)
	    self.outfile = "%s/stream_%s.json" % (data_dir, query_fname)

	def on_data(self, data):
		try:
			with open(self.outfile,'a') as f:
				f.write(data)
				print(data)
				return True
		except BaseException as e:
			print("Error on_data: %s"%str(e))
			time.sleep(5)
		return True
	def on_error(self, status):
		print(status)
		return True

def format_filename(fname):
	"""
	Arguments:
		fname -- the file name to convert
	Return:
		String -- converted file name
	"""
	return ''.join(convert_valid(one_char) for one_char in fname)

def convert_valid(one_char):
	validChars = "-_.%s%s"%(string.ascii_letters, string.digits)
	if one_char in validChars:
		return one_char
	else:
		return "_"

@classmethod
def parse(cls, api, raw):
	status = cls.first_parse(api,raw)
	setattr(status,'json',json.dumps(raw))
	return status

if __name__ == '__main__' :
	parser = getParser()
	args = parser.parse_args()
	auth = OAuthHandler(config.consumerKey, config.consumerSecret)
	auth.set_access_token(config.accessToken, config.accessTokenSecret)
	api = tw.API(auth)
	
	twitterStream = Stream(auth, TwitterListener(args.data_dir, args.query))
	twitterStream.filter(track=[args.query])
