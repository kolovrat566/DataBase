import pymysql
import requests
import config
import vk_api


def getinfo(user_ids):
    response = requests.get('https://api.vk.com/method/users.get',
                            params={
                                'access_token': config.ServisKey,
                                'v': config.version,
                                'user_ids': user_ids,
                                'fields': 'photo_id, followers_count, city'
                            }
                            )
    data = response.json()
    return data


vk_session = vk_api.VkApi(config.login, config.password)
vk_session.auth()
vk = vk_session.get_api()

friend = vk.friends.get()
ListFriends = friend['items']
count = friend['count']


try:
    con = pymysql.connect(host=config.Host, port=config.Port, user=config.User,
                          password=config.Password, database='mybd',
                          cursorclass=pymysql.cursors.DictCursor)
    print('подключение')

    '''
    drop_table_query = "DROP TABLE users"
    with con.cursor() as cursor:
        cursor.execute(drop_table_query)'''

    try:

        cursor = con.cursor()
        with con.cursor() as cursor:
            create_table_query = "CREATE TABLE `users`(id int AUTO_INCREMENT," \
                                 " firstname varchar(32)," \
                                 " lastname varchar(32)," \
                                 " city text(32)," \
                                 " id_users int(20),"\
                                 " followers varchar(20), PRIMARY KEY (id));"

            cursor.execute(create_table_query)


        with con.cursor() as cursor:
            for i in range(count):

                Fname = getinfo(ListFriends[i]) ['response'] [0] ['first_name']
                Lname = getinfo(ListFriends[i]) ['response'] [0] ['last_name']
                #
                id_users = getinfo(ListFriends[i]) ['response'] [0] ['id']
                try:
                    followers = getinfo(ListFriends[i]) ['response'] [0] ['followers_count']
                except:
                    followers = '0'
                try:
                    city = getinfo(ListFriends[i]) ['response'] [0] ['city'] ['title']
                except:
                    city = 'None'

                Fname = "'" + str(Fname) + "'"
                Lname = "'" + str(Lname) + "'"
                city = "'" + str(city) + "'"
                id_users = "'" + str(id_users) + "'"
                followers = "'" + str(followers) + "'"

                insert = "INSERT INTO `users` (firstname, lastname, id_users, followers, city) " \
                         "VALUES (" + str(Fname) + ", " + str(Lname) + ", " + str(id_users) + ", " + str(followers) + ", " + str(city) + ");"
                cursor.execute(insert)
                con.commit()

        print('ok')
    finally:
        con.close()

except Exception as ex:
    print('problem')
    print(ex)
