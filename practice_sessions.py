import json

from pymongo import MongoClient

from dataclasses import dataclass
from dataclasses import field
from typing import List

from datetime import datetime
from datetime import date

# pprint library is used to make the output look more pretty
# from pprint import pprint


# Issue the serverStatus command and print the results
# serverStatusResult=db.command("serverStatus")
# pprint(serverStatusResult)

BALLS_COST = 55
NETS_COST = 35


@dataclass
class Transaction:
    name: str
    amt: float = 0.00


@dataclass
class Member:
    name: str
    date: date = date.today()
    credit_list: List[Transaction] = field(default_factory=list)
    debit_list: List[Transaction] = field(default_factory=list)


def default(o):
    if isinstance(o, (date, datetime)):
        return o.isoformat()

    if isinstance(o, Transaction):
        return o.__dict__


def get_transactions():
    with open('./transactions.json') as f:
        data = json.load(f)

    return data


def get_collection():
    # client = MongoClient(port=27017)
    client = MongoClient("mongodb://localhost:27017/admin")
    # client.admin
    return client.rcc.PracticeSessions


def add_transaction(transaction_date, attendees, collection):
    for attendee in attendees:
        member = Member(attendee,
                        transaction_date,
                        credit_list=[Transaction("balls", BALLS_COST),
                                     Transaction("nets", NETS_COST)]
                        )
        member_json = json.dumps(member.__dict__,
                                 default=default
                                 )

        member_dict = json.loads(member_json)
        collection.insert_one(member_dict)


def add_transactions(transactions: dict, collection):
    for key, values in transactions.items():
        transaction_date = datetime.strptime(key, '%Y-%m-%d').date()
        add_transaction(transaction_date, values, collection)


def update_balls_cost(collection, new_cost):
    collection.update_many(
        {},
        {
            "$set": {"credit_list.$[].amt": new_cost}
        }
    )


if __name__ == '__main__':
    collection = get_collection()
    transactions = get_transactions()
    add_transactions(transactions, collection)
    # update_balls_cost(collection, BALLS_COST, 9.17)
