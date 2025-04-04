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
        Ensures that the sum of all balances remains zero after the transaction.
        '''

        # BEGIN transaction (implicitly starts with first execute)
        try:
            # 🚨 Lock balances table for exclusive access
            sql = text('LOCK TABLE balances IN ACCESS EXCLUSIVE MODE;')
            logging.debug(sql)
            self.connection.execute(sql)

            # Insert the transaction record
            sql = text(
                f'INSERT INTO transactions (debit_account_id, credit_account_id, amount) '
                f'VALUES ({debit_account_id}, {credit_account_id}, {amount});'
            )
            logging.debug(sql)
            self.connection.execute(sql)

            # Update debit account
            sql = text(f'SELECT balance FROM balances WHERE account_id = {debit_account_id};')
            logging.debug(sql)
            results = self.connection.execute(sql)
            debit_balance = results.first()[0]

            debit_new_balance = debit_balance - amount
            sql = text(f'UPDATE balances SET balance = {debit_new_balance} WHERE account_id = {debit_account_id};')
            logging.debug(sql)
            self.connection.execute(sql)

            # Update credit account
            sql = text(f'SELECT balance FROM balances WHERE account_id = {credit_account_id};')
            logging.debug(sql)
            results = self.connection.execute(sql)
            credit_balance = results.first()[0]

            credit_new_balance = credit_balance + amount
            sql = text(f'UPDATE balances SET balance = {credit_new_balance} WHERE account_id = {credit_account_id};')
            logging.debug(sql)
            self.connection.execute(sql)

            # ✅ One final commit for all operations
            self.connection.commit()

        except Exception as e:
            logging.error(f"Transfer failed: {e}")
            self.connection.rollback()
            raise

