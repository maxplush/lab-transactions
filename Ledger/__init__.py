import time
import sqlalchemy.exc
import sqlalchemy
from sqlalchemy.sql import text
import os
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format=f'%(asctime)s.%(msecs)03d - {os.getpid()} - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)


class Ledger:
    '''
    This class provides a python interface to the ledger database.
    Each method performs appropriate SQL commands to manipulate the database.
    '''

    def __init__(self, url):
        '''
        The constructor just creates a connection to the database.
        The same connection is re-used between all SQL commands,
        and the right way to think about a connection is as a single psql process where those commands will be entered.
        '''
        self.engine = sqlalchemy.create_engine(url)
        self.connection = self.engine.connect()

    def get_all_account_ids(self):
        '''
        This function is used inside of the random_transfers.py script.
        '''
        sql = text('SELECT account_id FROM accounts;')
        logging.debug(sql)
        results = self.connection.execute(sql)
        return [row[0] for row in results.all()]

    def create_account(self, name):
        '''
        In order to create an account, we need to insert a new row into the "accounts" able and the "balances" table.
        Because of the FOREIGN KEY constraint on the "balances" table,
        we need to know the "account_id" column of the row we've inserted into "accounts".
        This value is generated for us automatically by the database, and not within python.
        So we need to query the database after inserting into "accounts" to get the value.
        '''
        with self.connection.begin():

            # insert the name into "accounts"
            sql = text('INSERT INTO accounts (name) VALUES (:name);')
            sql = sql.bindparams(name=name)
            logging.debug(sql)
            self.connection.execute(sql)

            # get the account_id for the new account
            sql = text('SELECT account_id FROM accounts WHERE name=:name')
            sql = sql.bindparams(name=name)
            logging.debug(sql)
            results = self.connection.execute(sql)
            account_id = results.first()[0]

            # add the row into the "balances" table
            sql = text('INSERT INTO balances VALUES (:account_id, 0);')
            sql = sql.bindparams(account_id=account_id)
            logging.debug(sql)
            self.connection.execute(sql)

    def transfer_funds(self, debit_account_id, credit_account_id, amount):
        '''
        Transfers funds between two accounts using double-entry bookkeeping.
        Uses row-level locks and retries on deadlock.
        '''

        MAX_RETRIES = 5
        for attempt in range(MAX_RETRIES):
            try:
                # Begin transaction
                # Lock rows for update (row-level locks)
                sql = text(f'SELECT balance FROM balances WHERE account_id = {debit_account_id} FOR UPDATE;')
                logging.debug(sql)
                results = self.connection.execute(sql)
                debit_balance = results.first()[0]

                sql = text(f'SELECT balance FROM balances WHERE account_id = {credit_account_id} FOR UPDATE;')
                logging.debug(sql)
                results = self.connection.execute(sql)
                credit_balance = results.first()[0]

                # Insert transaction record
                sql = text(
                    f'INSERT INTO transactions (debit_account_id, credit_account_id, amount) '
                    f'VALUES ({debit_account_id}, {credit_account_id}, {amount});'
                )
                logging.debug(sql)
                self.connection.execute(sql)

                # Update balances
                new_debit = debit_balance - amount
                new_credit = credit_balance + amount

                sql = text(f'UPDATE balances SET balance = {new_debit} WHERE account_id = {debit_account_id};')
                logging.debug(sql)
                self.connection.execute(sql)

                sql = text(f'UPDATE balances SET balance = {new_credit} WHERE account_id = {credit_account_id};')
                logging.debug(sql)
                self.connection.execute(sql)

                # Commit transaction
                self.connection.commit()
                break  # Success, break out of retry loop

            except sqlalchemy.exc.OperationalError as e:
                self.connection.rollback()
                logging.warning(f"Deadlock detected. Retry attempt {attempt + 1}...")
                time.sleep(0.1 * (attempt + 1))  # Slight backoff
                continue  # Try again

            except Exception as e:
                self.connection.rollback()
                logging.error(f"Transfer failed: {e}")
                raise

