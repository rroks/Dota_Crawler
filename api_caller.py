import time
from dota2api.src.exceptions import APIError
import multiprocessing

SLEEP_TIME = multiprocessing.cpu_count() * 30


def get_match_details(api, match_id):
    """
    Get match info by match id.
    :param api: An api object to call Dota 2 APIs.
    :param match_id: Target match id number.
    :return: Data pared by parse_raw_match().
    """
    for tries in range(5):
        try:
            match = api.get_match_details(match_id)
            break
        except APIError as e:
            # print(e.msg, "raise")
            raise APIError('Getting match ' + str(match_id) + ' Failed')
        except Exception as e:
            # print(sys.exc_info())
            # print(e)
            if 4 == tries:
                raise APIError('Getting match ' + str(match_id) + ' Failed')
            else:
                print("API ACCESS TOO OFTEN")
                time.sleep(SLEEP_TIME)

    return parse_raw_match(match)


def get_latest_match(api):
    """
    Get the latest played match id number.
    :param api: An api object to call Dota 2 APIs.
    :return: The latest played match id number.
    """
    for tries in range(5):
        try:
            matches = api.get_match_history()
            for match in matches["matches"]:
                if len(match["players"]) is 10:
                    return match["match_id"]
        except APIError:
            # print (e.msg)
            raise APIError('Getting most recent match Failed')
        except Exception as e:
            # print (sys.exc_info())
            # print (e)
            if 4 == tries:
                raise APIError('Getting match history Failed')
            else:
                print("API ACCESS TOO OFTEN")
                time.sleep(SLEEP_TIME)


def parse_raw_match(raw_match):
    """
    Verify and parse data obtained by API.
    :param raw_match: Raw data obtain by API.
    :return: A dict() object with match info.
    """
    if 10 != raw_match['human_players'] or 10 != len(raw_match['players']):
        raise APIError('Match %d wrong number of players' % raw_match['match_id'])
    if 'radiant_win' not in raw_match.viewkeys():
        raise APIError('Match %d is not a standard game or has not finished yet.' % raw_match['match_id'])

    parsed_match_details = dict()
    for key in raw_match:
        if key != 'players':
            parsed_match_details[key.encode('utf-8')] = raw_match[key]
        else:
            for idx, v in enumerate(raw_match[key]):
                parsed_match_details['player%d' % idx] = v['hero_id']
    return parsed_match_details
