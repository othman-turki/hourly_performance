"""
    PYTHON APP(/SCRIPT) TO CALCULATE PERFORMANCES HOURLY AND STORE DATA IN
    MYSQL DATABASE
"""

import time
from datetime import datetime
from mysql.connector import connect, Error


triggers = {
    "08:30:00": {"start": "07:30:00", "end": "08:29:59", "work_time": "60"},
    "09:30:00": {"start": "08:30:00", "end": "09:29:59", "work_time": "60"},
    "10:30:00": {"start": "09:30:00", "end": "10:29:59", "work_time": "60"},
    "11:30:00": {"start": "10:30:00", "end": "11:29:59", "work_time": "60"},
    "11:50:00": {"start": "11:30:00", "end": "11:50:00", "work_time": "20"},
    "13:30:00": {"start": "12:30:00", "end": "13:29:59", "work_time": "60"},
    "14:30:00": {"start": "13:30:00", "end": "14:29:59", "work_time": "60"},
    "15:30:00": {"start": "14:30:00", "end": "15:29:59", "work_time": "60"},
    "16:30:00": {"start": "15:30:00", "end": "16:29:59", "work_time": "60"},
    "17:10:00": {"start": "16:30:00", "end": "17:10:00", "work_time": "40"},
    # TEST
    # "22:17:00": {"start": "21:30:00", "end": "22:29:59", "work_time": "20"},
}


def main():
    """ MAIN FUNCTION: FOR LOCAL SCOPING """

    try:
        with connect(
            host="localhost",
            user="root",  # ISA
            password="",  # SmarTex2021
            database="db_isa",
        ) as connection:
            print("Connection to DB succeeded!")

            while True:
                now = datetime.now()
                cur_day = now.strftime("%d/%m/%Y")
                cur_time = now.strftime("%H:%M:%S")

                if cur_time in triggers:
                    select_query = """
                        SELECT
                            registration_number,
                            Firstname,
                            Lastname,
                            ROUND(SUM((quantity * tps_ope_uni)) / """ + triggers[cur_time]["work_time"] + """, 2) AS performance
                        FROM
                            `pack_operation`
                        WHERE
                            cur_day = '""" + cur_day + """' AND cur_time BETWEEN '""" + triggers[cur_time]["start"] + """' AND '""" + triggers[cur_time]["end"] + """'
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
