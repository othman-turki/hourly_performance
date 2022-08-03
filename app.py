"""
    PYTHON APP(/SCRIPT) TO CALCULATE PERFORMANCES HOURLY AND STORE DATA IN
    MYSQL DATABASE
"""

import time
from datetime import datetime
from mysql.connector import connect, Error

TRIGGERS = ("08:30:00", "09:30:00", "10:30:00", "11:30:00", "12:30:00",
            "13:30:00", "14:30:00", "15:30:00", "16:30:00", "17:30:00")
# TRIGGERS = ("19:00:00", "20:00:00")


def main():
    """ MAIN FUNCTION: FOR LOCAL SCOPING """

    try:
        with connect(
            host="localhost",
            user="root",
            password="",
            database="db_isa",
        ) as connection:
            print("Connection to DB succeeded!")

            while True:
                now = datetime.now()
                cur_day = now.strftime("%d/%m/%Y")
                cur_time = now.strftime("%H:%M:%S")

                if cur_time in TRIGGERS:
                    select_query = """
                        SELECT
                            registration_number, Firstname, Lastname, ROUND(SUM((quantity * tps_ope_uni)) / 60, 2) AS performance
                        FROM
                            `pack_operation`
                        WHERE
                            cur_day = '""" + cur_day + """' AND cur_time > SUBTIME('""" + cur_time + """', 005900)
                        GROUP BY
                            registration_number;
                    """
                    insert_query = """
                        INSERT INTO performance_per_hour
                        (registration_number, first_name,
                        last_name, performance, cur_day, cur_time)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """

                    with connection.cursor() as cursor:

                        cursor.execute(select_query)
                        results = cursor.fetchall()

                        print(results)

                        if len(results) > 0:
                            insert_records = [
                                (result[0], result[1], result[2],
                                 result[3], cur_day, cur_time,)
                                for result in results
                            ]
                            cursor.executemany(insert_query, insert_records)
                            connection.commit()

                # SLEEP FOR 1 SECOND
                print(cur_day, cur_time)
                time.sleep(1)

    except Error as err_msg:
        print(err_msg)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
