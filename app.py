"""
    PYTHON APP(/SCRIPT) TO CALCULATE PERFORMANCES HOURLY AND STORE DATA IN
    MYSQL DATABASE
"""

import logging
from datetime import datetime
from mysql.connector import connect, Error


now = datetime.now()
curDay = now.strftime("%d/%m/%Y")
curTime = now.strftime("%H:%M:%S")
# print(curDay, curTime)
logging.basicConfig(filename="log.txt", level=logging.DEBUG,
                    format="%(asctime)s %(message)s")


def main():
    """ MAIN FUNCTION: FOR LOCAL SCOPING """

    try:
        with connect(
            host="localhost",
            user="root",
            password="",
            database="etc",
        ) as connection:
            print("Connection to DB succeeded!")

            select_query = """
                SELECT
                    registration_number,
                    Firstname,
                    Lastname,
                    ROUND(
                        SUM((quantity * tps_ope_uni)) / 60,
                        2
                    ) AS performance
                FROM
                    `pack_operation`
                WHERE
                    registration_number IS NOT NULL AND cur_day = DATE_FORMAT(CURDATE(), '%d/%m/%Y') AND cur_time > SUBTIME(CURRENT_TIME(), 005900)
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

                # print(results, len(results))
                logging.debug("Fetch Success")

                if len(results) > 0:
                    insert_records = [
                        (result[0], result[1], result[2],
                         result[3], curDay, curTime,)
                        for result in results
                    ]
                    cursor.executemany(insert_query, insert_records)
                    connection.commit()
                    logging.debug("Inserted Succfully")
                else:
                    logging.debug("No Results For Fetch")

    except Error as err_msg:
        print(err_msg)
        logging.debug(err_msg)


if __name__ == '__main__':
    main()
