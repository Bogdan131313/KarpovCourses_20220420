from datetime import datetime, timedelta
import pandas as pd
import pandahouse
# import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Connection parameters for simulator_20220420
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20220420'
}

# Connection parameters for test
connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'
}

# Default DAG's parameters
default_args = {
    'owner': 'mbs',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 5, 19),
}

# Launchumg interval
schedule_interval = '59 11,17,23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def my_first_dag():
    # PART 1
    # Views, Likes
    @task()
    def get_feedinfo():
        q_0 = """ SELECT DISTINCT toDate(time) AS date,
                                user_id, 
                                countIf(action='view') AS Views,
                                countIf(action='like') AS Likes
                FROM simulator_20220420.feed_actions
                WHERE date = today()
                GROUP BY user_id, date
                ORDER BY Views desc
                LIMIT 50  """
        feed_data = pandahouse.read_clickhouse(q_0, connection=connection)

        return feed_data

    # Messages
    @task()
    def get_messageinfo():
        q_1 = """ SELECT if(date<toDate('2020-01-01'), t2.date, date) AS date,
                        if(user_id=0, reciever_id, user_id) AS user_id, 
                        OutMessages, 
                        InMessages, 
                        Uniq_recievers, 
                        Uniq_senders
                FROM 
                    (SELECT DISTINCT toDate(time) AS date,
                            user_id, 
                            COUNT(*) AS OutMessages,
                            COUNT(DISTINCT reciever_id) AS Uniq_recievers
                    FROM simulator_20220420.message_actions
                    GROUP BY user_id, date) t1

                FULL JOIN 

                    (SELECT DISTINCT toDate(time) AS date,
                            reciever_id, 
                            COUNT(*) AS InMessages,
                            COUNT(DISTINCT user_id) AS Uniq_senders
                    FROM simulator_20220420.message_actions
                    GROUP BY reciever_id, date) t2

                ON t1.user_id = t2.reciever_id and t1.date = t2.date

                WHERE date = today()
                ORDER BY OutMessages desc
                LIMIT 25 """
        message_data = pandahouse.read_clickhouse(q_1, connection=connection)

        return message_data
    # PART 2
    # Merging tables
    @task()
    def get_feedmessage():
        q_2 = """ SELECT DISTINCT if(date<toDate('2020-01-01'), t2.date, date) AS date,
                                if(user_id=0, t2.user_id, user_id) AS user_id,
                                OutMessages, 
                                InMessages, 
                                Uniq_recievers, 
                                Uniq_senders,
                                Views,
                                Likes
                FROM (SELECT DISTINCT toDate(time) AS date,
                                        user_id, 
                                        countIf(action='view') AS Views,
                                        countIf(action='like') AS Likes
                        FROM simulator_20220420.feed_actions
                        GROUP BY user_id, date) t1

                FULL JOIN

                    (SELECT if(date<toDate('2020-01-01'), t2.date, date) AS date,
                            if(user_id=0, reciever_id, user_id) AS user_id, 
                            OutMessages, 
                            InMessages, 
                            Uniq_recievers, 
                            Uniq_senders
                    FROM 
                        (SELECT DISTINCT toDate(time) AS date,
                                user_id, 
                                COUNT(*) AS OutMessages,
                                COUNT(DISTINCT reciever_id) AS Uniq_recievers
                        FROM simulator_20220420.message_actions
                        GROUP BY user_id, date) t1

                        FULL JOIN 

                        (SELECT DISTINCT toDate(time) AS date,
                                reciever_id, 
                                COUNT(*) AS InMessages,
                                COUNT(DISTINCT user_id) AS Uniq_senders
                        FROM simulator_20220420.message_actions
                        GROUP BY reciever_id, date) t2

                        ON t1.user_id = t2.reciever_id AND t1.date = t2.date) t2

                    ON t1.user_id = t2.user_id AND t1.date = t2.date
                
                WHERE date = today()
                ORDER BY user_id, date """
        feedmessage_data = pandahouse.read_clickhouse(q_2, connection=connection)

        return feedmessage_data

    # Get data of all existing users for futher merging
    @task()
    def get_userinfo():
        q_3 = """ WITH userinfo AS (
        SELECT DISTINCT
                    user_id, 
                    if(gender=1, 'female', 'male') AS gender,
                    os AS os,
                    multiIf(age <=20, '0 - 20', age > 20
                                    and age <= 30, '21-30', age between 31 and 50, '31-50', '50+') AS age_group
            FROM simulator_20220420.message_actions

        UNION DISTINCT 

        SELECT DISTINCT
                    user_id, 
                    if(gender=1, 'female', 'male') AS gender,
                    os AS os,
                    multiIf(age <=20, '0 - 20', age > 20
                                    and age <= 30, '21-30', age between 31 and 50, '31-50', '50+') AS age_group
            FROM simulator_20220420.feed_actions
                        )
        SELECT * FROM userinfo 
        ORDER BY 1 """
        userinfo = pandahouse.read_clickhouse(q_3, connection=connection)

        return userinfo
    # PART 3
    # Getting grouped data
    @task()
    def age_grouped(userinfo, feed_message):
        age_grouped = feed_message.merge(userinfo[['user_id', 'age_group']], how='inner', on='user_id') \
            .groupby(['date', 'age_group']) \
            .sum() \
            .reset_index() \
            .drop(labels='user_id', axis=1)
        return age_grouped

    @task()
    def gender_grouped(userinfo, feed_message):
        gender_grouped = feed_message.merge(userinfo[['user_id', 'gender']], how='inner', on='user_id') \
            .groupby(['date', 'gender']) \
            .sum() \
            .reset_index() \
            .drop(labels='user_id', axis=1)
        return gender_grouped

    @task()
    def os_grouped(userinfo, feed_message):
        os_grouped = feed_message.merge(userinfo[['user_id', 'os']], how='inner', on='user_id') \
            .groupby(['date', 'os']) \
            .sum() \
            .reset_index() \
            .drop(labels='user_id', axis=1)
        return os_grouped
    # PART 4
    @task()
    def final_data(age_grouped, gender_grouped, os_grouped):
        os_grouped.insert(1, 'metric', 'os')
        gender_grouped.insert(1, 'metric', 'gender')
        age_grouped.insert(1, 'metric', 'age')

        gender_grouped.rename(columns={'gender': 'metric_value'}, inplace=True)
        age_grouped.rename(columns={'age_group': 'metric_value'}, inplace=True)
        os_grouped.rename(columns={'os': 'metric_value'}, inplace=True)

        full_df = pd.concat([age_grouped, gender_grouped, os_grouped], axis=0)
        full_df = full_df.sort_values(by=['date', 'metric', 'metric_value']).reset_index(drop=True)

        full_df = full_df.astype({'date': 'datetime64',
                                  'metric': 'str',
                                  'metric_value': 'str',
                                  'OutMessages': 'uint64',
                                  'InMessages': 'uint64',
                                  'Uniq_recievers': 'uint64',
                                  'Uniq_senders': 'uint64',
                                  'Views': 'uint64',
                                  'Likes': 'uint64'})
        return full_df

    @task()
    def load(full_df, feed_data, message_data):
        # выгружу результат первых двух запроосов в лог
        context = get_current_context()
        ds = context['ds']
        print(f'Feed info for {ds}')
        print(feed_data.to_csv(index=False, sep='\t'))
        print(f'LMessage info for {ds}')
        print(message_data.to_csv(index=False, sep='\t'))
        
        # сотрем неактуальные данные за сегодня и загрузим свежие. Такой способ самый безопасный для контроля данных в таблице
        # и так как воздействуем на ограниченное и небольшое количество строк, еще и не сильно затратный
        delete_old = """ ALTER TABLE test.mbs_table DELETE WHERE date = today() """ 
        pandahouse.execute(query=delete_old, connection=connection_test)
        pandahouse.to_clickhouse(df=full_df, table='mbs_table', index=False, connection=connection_test)

    feed_data = get_feedinfo()
    message_data = get_messageinfo()
    feedmessage_data = get_feedmessage()
    userinfo = get_userinfo()
    age_grouped = age_grouped(userinfo=userinfo, feed_message=feedmessage_data)
    gender_grouped = gender_grouped(userinfo=userinfo, feed_message=feedmessage_data)
    os_grouped = os_grouped(userinfo=userinfo, feed_message=feedmessage_data)
    full_df = final_data(age_grouped, gender_grouped, os_grouped)
    load(full_df, feed_data, message_data)

my_first_dag = my_first_dag()
