/*
File: SaveTest.cpp
Date: 09/27/2021
Developer: Alan Cannon
Description: This program will detect fraudulent activities on a customers 
account by analyzing the data in a MySQL database which contains 
account, merchant, and transaction data. The results are printed in
file fraud_amount.txt and fraud_location.txt as well as output to the
console.
*/

#include <iostream>
#include <fstream>
#include <iomanip>
#include <mysql.h>
#include <string>

using namespace std;

struct connection_details {
    const char* server, * user, * password, * database;
};

MYSQL* mysql_connection_setup(struct connection_details mysql_details) {
    MYSQL* connection = mysql_init(NULL); // mysql instance

    //connect database
    if (!mysql_real_connect(connection, mysql_details.server, mysql_details.user, mysql_details.password, mysql_details.database, 0, NULL, 0)) {
        cout << "Connection Error: " << mysql_error(connection) << endl;
        exit(1);
    }

    return connection;
}

// mysql_res = mysql result
MYSQL_RES* mysql_perform_query(MYSQL* connection, const char* sql_query) {
    //send query to db
    if (mysql_query(connection, sql_query)) {
        cout << "MySQL Query Error: " << mysql_error(connection) << endl;
        exit(1);
    }

    return mysql_use_result(connection);
}

int main()
{
    MYSQL* con;	// the connection
    MYSQL_RES* res;	// the results
    MYSQL_ROW row;	// the results rows (array)

    struct connection_details mysqlD;
    mysqlD.server = "localhost";  // where the mysql database is
    mysqlD.user = "root"; // user
    mysqlD.password = "password11"; // the password for the database
    mysqlD.database = "test";	// the database

    // connect to the mysql database
    con = mysql_connection_setup(mysqlD);

    cout << "Connection successful" << endl;

    /***************************************** 
     FIND TRANSACTION AMOUNT ANOMALIES 
    * ****************************************/
    cout << "\n\nFRAUDULENT AMOUNT:\n";
    // Console Header
    cout << setw(30) << left << "Name" << setw(30) << left << "Account Number" << setw(30) << left << "Transaction Number" << setw(30) << left << "Merchant" << setw(30) << left << "Transaction Amount" << endl << endl;

    // Open output file
    ofstream amtFraudFile("fraud_amount.txt");
    amtFraudFile << setw(30) << left << "Name" << setw(30) << left << "Account Number" << setw(30) << left << "Transaction Number" << setw(30) << left << "Merchant" << setw(30) << left << "Transaction Amount" << endl << endl;
    
    string anomaly_query =
        "SELECT accounts.first_name, accounts.last_name, transactions.account_number, transactions.transaction_number, transactions.merchant_name, transactions.tx_amount "
        "FROM test.transactions "
        "JOIN(SELECT account_number, avg(tx_amount) as avg_price FROM test.transactions GROUP BY account_number) tt "
        "ON transactions.account_number = tt.account_number "
        "INNER JOIN test.accounts ON transactions.account_number = accounts.account_number "
        "WHERE transactions.tx_amount > 2 * avg_price and transactions.tx_amount>500";

    res = mysql_perform_query(con, anomaly_query.c_str());

    while ((row = mysql_fetch_row(res)) != NULL) {
        cout << setw(30) << left << string(row[0]) + " " + string(row[1]) << setw(30) << left << row[2] << setw(30) << left << row[3] << setw(30) << left << row[4] << setw(30) << left << row[5] << endl << endl;
        amtFraudFile << setw(30) << left << string(row[0]) + " " + string(row[1]) << setw(30) << left << row[2] << setw(30) << left << row[3] << setw(30) << left << row[4] << setw(30) << left << row[5] << endl << endl;
    }

    // clean up the database result
    mysql_free_result(res);

    /***************************************** 
     FIND LOCATION FRAUD 
    * ****************************************/
    cout << "\n\nUNEXPECTED LOCATION:\n";
    // Console Header
    cout << setw(30) << left << "Name" << setw(30) << left << "Account Number" << setw(30) << left << "Transaction Number" << setw(30) << left << "Expected Transaction Location" << setw(30) << left << "Actual Transaction Location" << endl << endl;

    /* Open output file */
    ofstream locFraudFile("fraud_location.txt");
    locFraudFile << setw(30) << left << "Name" << setw(30) << left << "Account Number" << setw(30) << left << "Transaction Number" << setw(30) << left << "Expected Transaction Location" << setw(30) << left << "Actual Transaction Location" << endl << endl;
    
    string location_query =
        "SELECT transactions.account_number, transactions.transaction_number, accounts.state, transactions.merch_state, accounts.first_name, accounts.last_name "
        "FROM test.transactions INNER JOIN test.accounts ON transactions.account_number = accounts.account_number "
        "WHERE transactions.merch_state<>accounts.state";

    res = mysql_perform_query(con, location_query.c_str());

    while ((row = mysql_fetch_row(res)) != NULL) {
        cout << setw(30) << left << string(row[4]) + " " + string(row[5]) << setw(30) << left << row[0] << setw(30) << left << row[1] << setw(30) << left << row[2] << setw(30) << left << row[3] << endl << endl;
        locFraudFile << setw(30) << left << string(row[4]) + " " + string(row[5]) << setw(30) << left << row[0] << setw(30) << left << row[1] << setw(30) << left << row[2] << setw(30) << left << row[3] << endl << endl;
    }

    // clean up the database result
    mysql_free_result(res);

    // close output files
    amtFraudFile.close();
    locFraudFile.close();

    // shut down database connection
    mysql_close(con);

    return 0;
}
