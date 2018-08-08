from dota2api.src.exceptions import APIError


def parse_raw_match(raw_match):
    if 10 != raw_match['human_players'] or 10 != len(raw_match['players']):
        raise APIError('Match %d wrong number of players' % raw_match['match_id'])
    if 'radiant_win' not in raw_match.viewkeys():
        raise APIError('Match %d has not finished yet.' % raw_match['match_id'])

    # parsed_match_details = [0] * 17
    # parsed_match_details[0] = raw_match['match_id']
    # parsed_match_details[1] = raw_match['radiant_win']
    # parsed_match_details[2] = raw_match['duration']
    # parsed_match_details[3] = raw_match['lobby_type']
    # parsed_match_details[4] = raw_match['game_mode']
    # parsed_match_details[5] = raw_match['human_players']
    # for i in range(10):
    #     parsed_match_details[i + 6] = raw_match['players'][i]['hero_id']
    # parsed_match_details[16] = raw_match['start_time']
    parsed_match_details = dict()
    for key in raw_match:
        if key != 'players':
            parsed_match_details[key] = raw_match[key]
        else:
            for idx, v in enumerate(raw_match[key]):
                parsed_match_details['player%d' % idx] = v['hero_id']
    return parsed_match_details
