
from twilio.rest import Client


def send_me_sms(to_num,message):
	'''
	Send sms to cellphone
	:param message:  message content
	:param to_num: receiving phone#
	:return: None
	'''
	sid = 'ACc9af4189fcf10edde94bebe0802ff4d5'
	auth = '6c0335fee4453f048e56b49b94a9e783'
	myTwilio_num = '+15203293250'

	twilio_client=Client(sid,auth)

	twilio_client.messages.create(body=message,from_=myTwilio_num,to=to_num)



class SendSms():
	"""
	a class to send SMS to specified cell#
	:return:
	"""

	def __init__(self):
		self.sid='ACc9af4189fcf10edde94bebe0802ff4d5'
		self.auth = '6c0335fee4453f048e56b49b94a9e783'
		self.myTwilio_num = '+15203293250'
	def send_sms(self,message,to_num):
		twilio_client = Client(self.sid, self.auth)
		twilio_client.messages.create(body=message, from_=self.myTwilio_num, to=to_num)


if __name__=='__main__':
	to_num = '+8618665932236'
	message='this is a test message from twilio'
	sms=SendSms()
	sms.send_sms(message,to_num)

