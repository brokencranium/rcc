import json
import pytz

from pymongo import MongoClient
from dataclasses import dataclass, asdict
from datetime import datetime

# pprint library is used to make the output look more pretty
from pprint import pprint

# Issue the serverStatus command and print the results
# serverStatusResult=db.command("serverStatus")
# pprint(serverStatusResult)
COST_OF_BALLS = 55.00
INSERT = "INSERT"
UPDATE_BALLS = "UPDATE_BALLS"
CREDIT = 'CREDIT'
DEBIT = 'DEBIT'
BALLS = 'BALLS'
NETS = 'NETS'
COST_BALLS = 'COST_BALLS'
REPORT = 'REPORT'


@dataclass
class Transaction:
    _id: str
    member: str
    date: datetime
    tran_type: str
    description: str
    amt: float = 0.00
    update_ts: datetime = datetime.utcnow()
    create_ts: datetime = datetime.utcnow()


def get_transactions(path):
    with open(path) as f:
        data = json.load(f)

    return data


def get_collection():
    # client = MongoClient(port=27017)
    client = MongoClient("mongodb://localhost:27017/admin")
    # client.admin
    return client.rcc.PracticeSessions


def add_transaction(transaction_date, attributes, collection):
    for member in attributes['members']:
        member_name = "".join(member.split()).upper()
        for attribute in attributes['transactions']:
            unique_id = member_name + transaction_date.strftime('%Y%m%d') + attribute['type'] + attribute['description']
            transaction = Transaction(unique_id,
                                      member_name,
                                      transaction_date,
                                      attribute['type'],
                                      attribute['description'],
                                      attribute['amt'] / len(attributes['members'])
                                      )
            try:
                collection.insert_one(asdict(transaction))
            except Exception as ex:
                print(ex)


def add_transactions(transactions: dict, collection):
    for transaction in transactions:
        [[key, values]] = transaction.items()
        transaction_date = datetime.strptime(key, '%Y-%m-%d').astimezone(pytz.utc)
        add_transaction(transaction_date, values, collection)


def get_cost_of_balls_session(start_date: datetime, end_date: datetime, collection):
    counts = collection.aggregate([
        {
            "$match": {"tran_type": CREDIT,
                       "description": NETS,
                       "date": {"$gte": start_date,
                                "$lte": end_date
                                }

                       }
        },
        {"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d",
                                              "date": "$date"
                                              }
                            },
                    }
         }
        ,
        {"$count": "counts"}
    ])

    count = 0
    for batch in counts:
        count = batch['counts']

    return COST_OF_BALLS / count


def add_cost_of_balls(start_date: datetime, end_date: datetime, collection):
    cost_per_session = get_cost_of_balls_session(start_date, end_date, collection)
    print(f'Cost per session {cost_per_session}')

    docs = collection.aggregate([
        {
            "$match": {"tran_type": "CREDIT",
                       "description": 'NETS',
                       "date": {"$gte": start_date,
                                "$lte": end_date
                                }

                       }
        },
        {"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d",
                                              "date": "$date"
                                              }
                            },
                    "member": {"$push": "$$ROOT"}
                    }
         }
    ])

    for doc in docs:
        cost_per_person = cost_per_session / len(doc['member'])
        for member in doc['member']:
            unique_id = member['member'] + member['date'].strftime('%Y%m%d') + CREDIT + BALLS

            transaction = Transaction(unique_id,
                                      member['member'],
                                      member['date'],
                                      CREDIT,
                                      BALLS,
                                      cost_per_person
                                      )
            collection.insert_one(asdict(transaction))


def display_report(start_date, end_date, collection):
    docs = collection.aggregate([{"$match": {"date": {"$gte": start_date,
                                                      "$lte": end_date}
                                             }
                                  },
                                 {"$group": {"_id": "$member",
                                             "Credit": {
                                                 "$sum": {
                                                     "$switch": {
                                                         "branches": [
                                                             {
                                                                 "case": {"$eq": ["$tran_type", CREDIT]},
                                                                 "then": "$amt"
                                                             }
                                                         ],
                                                         "default": 0
                                                     }}},
                                             "Debit": {
                                                 "$sum": {
                                                     "$switch": {
                                                         "branches": [
                                                             {
                                                                 "case": {"$eq": ["$tran_type", DEBIT]},
                                                                 "then": "$amt"
                                                             }
                                                         ],
                                                         "default": 0
                                                     }}}
                                             }
                                  }
                                 ])
    total_credit = 0
    total_debit = 0
    total_due = 0
    for doc in docs:
        due = doc['Credit'] - doc['Debit']
        print(f"{doc['_id']:<15}"
              f"Credit: {doc['Credit']:<10.2f}"
              f"Debit: {doc['Debit']:<10.2f}"
              f"Due: {due:<10.2f}")
        total_credit = doc['Credit'] + total_credit
        total_debit = doc['Debit'] + total_debit
        total_due = due + total_due

    print(f"\nTotal Credit: {total_credit:<12.2f} Total Debit:{total_debit:<12.2f} Total Due:{total_due:<12.2f}")


if __name__ == '__main__':
    # transaction_mode = INSERT
    # transaction_mode = COST_BALLS
    transaction_mode = REPORT

    collection = get_collection()

    if transaction_mode == INSERT:
        transactions = get_transactions('./transactions.json')
        add_transactions(transactions, collection)

    elif transaction_mode == COST_BALLS:
        start_date = datetime.strptime('2019-01-01', '%Y-%m-%d').astimezone(pytz.utc)
        end_date = datetime.strptime('2020-12-31', '%Y-%m-%d').astimezone(pytz.utc)
        add_cost_of_balls(start_date, end_date, collection)

    elif transaction_mode == REPORT:
        start_date = datetime.strptime('2019-01-01', '%Y-%m-%d').astimezone(pytz.utc)
        end_date = datetime.strptime('2020-12-31', '%Y-%m-%d').astimezone(pytz.utc)
        display_report(start_date, end_date, collection)
