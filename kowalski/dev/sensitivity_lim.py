from penquins import Kowalski
import pandas as pd


if __name__ == '__main__':
    k = Kowalski(username='USER', password='PASSWORD')

    # q_get_fields = {"query_type": "general_search",
    #                 "query": "db['ZTF_alerts'].distinct('candidate.field')"
    #                 }
    # fields = k.query(q_get_fields)['result_data']['query_result']

    fields = [245, 246, 744]
    rcids = list(range(0, 64))
    # print(rcids)


    # field, rcid = 245, 7
    for field in fields:
        for rcid in rcids:
            q_count = {"query_type": "general_search",
                       "query": f"db['ZTF_alerts'].count_documents({{'candidate.field': {field}, 'candidate.rcid': {rcid}}})"
                       }
            num_alerts = k.query(q_count)['result_data']['query_result']
            print(f'field: {field}, rcid: {rcid}, num_alerts: {num_alerts}')

            q = {"query_type": "general_search",
                 "query": f"db['ZTF_alerts'].find({{'candidate.field': {field}, 'candidate.rcid': {rcid}}}, "
                          "{'_id': 0, 'candidate.jd': 1, 'candidate.magpsf': 1, 'candidate.sigmapsf': 1})"
                 }
            data = k.query(q)['result_data']['query_result']
            data = [d['candidate'] for d in data]
            df = pd.DataFrame(data)
            print(df)
