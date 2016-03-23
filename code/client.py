__author__ = 'jaime'

import httplib

# IP - AWS EC2 instance
# ADD YOURS HERE:
IP = "XX.XX.XX.XX"
SERVER_IP = IP + ':5000'
# ADD YOURS HERE:
SERVER = 'ec2-XX-XX-XX-XX.us-west-1.compute.amazonaws.com:5000'

''' GENERIC HTTP FUNCTION =================================================='''
def get_output(command, url_end):
    """ Executes a command (eg. GET) in the url from the server
    :param a command, a url (excluding 'http://' and the SERVER)
    :return: a dictionary """
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    h.request(command, 'http://' + SERVER + url_end)
    resp = h.getresponse()
    out = resp.read()
    return out

''' MAIN SCRIPT ============================================================'''
if __name__ == '__main__':
    print "\n**********************************************************"
    print "Test of my flask app running at: \n  ", SERVER, "\n  ", SERVER_IP
    print "\nCreated by Jaime Pastor"
    print "**********************************************************\n"
    print "ANALYSIS OF DOMESTIC FLIGHTS DEPARTING FROM SAN FRANCISCO \n"
    print "******** Average delay (in minutes) per carrier ************"
    print get_output('GET', '/avg_delay/carrier')
    print "******** Average delay (in minutes) per destination ********"
    print get_output('GET', '/avg_delay/destination')
    print "******** Average delay (in minutes) per day of week ********"
    print get_output('GET', '/avg_delay/dayofweek')
    print "******** Average delay (in minutes) per departure time *****"
    print get_output('GET', '/avg_delay/departure-hour')
    print "******** Percentage of flights cancelled by airline ********"
    print get_output('GET', '/perc_cancelled')
    print "******** Percentage of flights delayed by airline **********"
    print get_output('GET', '/perc_delayed_carrier')
    print "\n******** Comparing airlines by destination ***************"
    print "\nFor example, let's input destination Honolulu, HI"
    print get_output('GET', '/destination/Honolulu,%20HI')
    print "\n******** Get percentage problematic by cause *************"
    print "\nLet's input again destination Honolulu, HI"
    print get_output('GET', '/problem_by_destination/Honolulu,%20HI')
    print "\nLet's try destination Madison, WI"
    print get_output('GET', '/problem_by_destination/Madison,%20WI')
    print "\n\n See accompanying pdf with explanation of each query"

'''=========================================================================='''