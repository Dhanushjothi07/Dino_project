import datetime

#  Date function
def get_datetime():
    dt1 = datetime.datetime.now()
    return dt1.strftime("%d %B, %Y")

monthstr = get_datetime()

#  SSM Parameter Names (UPDATED based on your screenshot)
URL_API_PARAM = '/dino/data'
ERROR_ARN_PARAM = '/dino/errorarn'
SUCCESS_ARN_PARAM = '/dino/successarn'
ENVIRONMENT_PARAM = '/dino/env'

#  Component Name (UPDATED)
COMPONENT_NAME = 'Dinosaur Project'

#  Messages (Dynamic with date)
ERROR_MSG = f'NEED ATTENTION **API ERROR KEY EXPIRED ON {monthstr}**'
SUCCESS_MSG = f'SUCCESSFULLY EXTRACTED FILES FOR {monthstr}'

#  Status
SUCCESS_DESCRIPTION = 'Success'