import asyncio
from config import *
import pyodbc
import datetime

"""Запрос к базе данных"""

sql_query = '''SELECT name, mail FROM users2'''

"""Соединение с базой данных"""

list_for_send = []
list_class = []
user_for_send = dict()


async def connect_with_db():
    while True:
        await asyncio.sleep(1)
        strCon = "Driver=" + driver + ";SERVER=" + srv + ";DATABASE=" + DB + ";UID=" + log + ";PWD=" + pwd + ";"
        lnk = pyodbc.connect(strCon)
        db = lnk.cursor()
        db.execute(sql_query)
        objects = db.fetchall()
        dict_users = dict()
        for row in objects:
            if row[1] not in dict_users:
                dict_users[row[1]] = [row[0], 1, 0, datetime.datetime.now()]
            else:
                dict_users[row[1]][1] += 1

        print(dict_users)
        for user in dict_users:
            if user not in user_for_send:
                user_for_send[user] = dict_users[user]
            else:
                user_for_send[user][1] = dict_users[user][1]
        print(2, user_for_send)
        for key, value in list(user_for_send.items()):
            if key not in dict_users:
                user_for_send.pop(key)

        await on_sheet(user_for_send)



async def on_sheet(user_for_send):
    with open('example.txt', 'w') as f:
        for user in user_for_send:
            print(1, user_for_send[user])
            f.write(f'{user_for_send[user][0]}, '
                    f'{user_for_send[user][1]}, '
                    f'{user_for_send[user][2]}, '
                    f'{user_for_send[user][3]}\n')

'''переписать '''
# async def send():
#     while True:
#         await asyncio.sleep(2)
#         global list_class
#         user_5 = list(filter(lambda s: s.send == 0, list_class))
#         await rocketchat5(user_5)
#         user_10 = list(filter(lambda s: s.send == 1, list_class))
#         await rocketchat10(user_10)
#         user_20 = list(filter(lambda s: s.send == 2, list_class))
#         await rocketchat20(user_20)


async def rocketchat5(users):
    print(1, users)
    await asyncio.sleep(20)
    for user in users:
        print('5min')
        print(f'На почту {user.mail} отправлено о {user.msg} не утвержденных рассписаниях')
        user.send += 1


async def rocketchat10(users):
    print(2, users)

    await asyncio.sleep(30)
    for user in users:
        print('10min')
        print(f'На почту {user.mail} отправлено о {user.msg} не утвержденных рассписаниях')
        user.send += 1


async def rocketchat20(users):
    print(3, users)
    await asyncio.sleep(50)
    for user in users:
        print('20min')
        print(f'На почту {user.mail} отправлено о {user.msg} не утвержденных рассписаниях')


async def main():
    tasks = [
        connect_with_db(),
        # send()
    ]
    await asyncio.gather(*tasks)


asyncio.run(main())
