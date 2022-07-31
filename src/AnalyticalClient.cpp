#include "AnalyticalClient.h"

AnalyticalClient::AnalyticalClient()
{
    // qStmt = vector<SQLHSTMT>(2*SQLDialect::analyticalQueries.size(), 0);
    qStmt = vector<SQLHSTMT>(SQLDialect::analyticalQueries.size(), 0);
    queriesNum = 0;
    freshnessVector = {0};                      // freshness score for each analytical query executed in the current thread
    executionTime = vector<vector<double>>(13); // execution time of each query
    testDuration = 0;
}

void AnalyticalClient::SetThreadNum(thread::id num)
{
    threadNum = num;
}

void AnalyticalClient::IncrementQueriesNum()
{
    queriesNum++;
}

int AnalyticalClient::GetQueriesNum() const
{
    return queriesNum;
}

thread::id AnalyticalClient::GetThreadNum()
{
    return threadNum;
}

void AnalyticalClient::PrepareAnalyticalStmt(SQLHDBC & dbc)
{
    for (unsigned int q = 0; q < 13; q++)
    {
        Driver::prepareStmt(dbc, qStmt[q], nullptr);
    }

    // if(UserInput::getdbChoice() != tidb){
    // string subQuery, query;
    // vector<string> queryParts = {"SELECT DISTINCT F_TXNNUM, F_CLIENTNUM FROM (",
    //     			 ") AS TMP1 CROSS JOIN (",
    // 			 "SELECT * FROM HAT.FRESHNESS",
    // 			 "",
    // 			 " UNION ALL ",
    // 			 ") AS TMP2 ORDER BY F_CLIENTNUM"};
    // for(unsigned int i=1; i<=(unsigned int)UserInput::getTranClients(); i++){
    //     if (UserInput::getTranClients() == 1){
    //             subQuery += queryParts[2]+std::to_string(i)+queryParts[3];
    //     }
    //     else if(i!=(unsigned int)UserInput::getTranClients()){
    //             subQuery += queryParts[2]+std::to_string(i)+queryParts[3]+queryParts[4];
    //     }
    //         else {
    //             subQuery += queryParts[2]+std::to_string(i)+queryParts[3];
    //     }
    // }
    // for(unsigned int q=0; q<13; q++){
    // if(UserInput::getTranClients()>0){
    //     query = queryParts[0]+SQLDialect::analyticalQueries[q]+queryParts[1]+subQuery+queryParts[5];
    //     Driver::prepareStmt(dbc, qStmt[q+13], query.c_str());
    // }
    // Driver::prepareStmt(dbc, qStmt[q], SQLDialect::analyticalQueries[q].c_str());
    // }
    // }
    // else {
    // string subQuery, query;
    // vector<string> queryParts = {"SELECT /*+ read_from_storage(tiflash[HAT.LINEORDER, HAT.PART, HAT.CUSTOMER, HAT.DATE, HAT.SUPPLIER",
    //                                  ", HAT.FRESHNESS",
    //                                  "]) */ DISTINCT F_TXNNUM, F_CLIENTNUM FROM (",
    //                                  ") AS TMP1 CROSS JOIN (",
    //                                  "SELECT * FROM HAT.FRESHNESS",
    // 			 " UNION ALL ",
    // 			 ") AS TMP2 ORDER BY F_CLIENTNUM"};
    // for(unsigned int i=1; i<=(unsigned int)UserInput::getTranClients(); i++){
    //         if (UserInput::getTranClients() == 1){
    // 	queryParts[0] += queryParts[1]+std::to_string(i)+queryParts[2];
    //                 subQuery += queryParts[4]+std::to_string(i);
    //         }
    //         else if(i!=(unsigned int)UserInput::getTranClients()){
    // 	queryParts[0] += queryParts[1]+std::to_string(i);
    // 	subQuery += queryParts[4]+std::to_string(i)+queryParts[5];
    //         }
    //         else {
    //                 queryParts[0] += queryParts[1]+std::to_string(i)+queryParts[2];
    // 	subQuery += queryParts[4]+std::to_string(i);
    //         }
    // }
    // for(unsigned int q=0; q<13; q++){
    // if(UserInput::getTranClients()>0){
    // 		query = queryParts[0]+SQLDialect::analyticalQueries[q]+queryParts[3]+subQuery+queryParts[6];
    // 	Driver::prepareStmt(dbc, qStmt[q+13], query.c_str());
    // }
    // Driver::prepareStmt(dbc, qStmt[q], SQLDialect::analyticalQueries[q].c_str());
    // }
    //    }
}

int AnalyticalClient::ExecuteQuery(int & q, Globals * g)
{
    int ret = -1;
    vector<double> fresh;
    double maxFresh = 0.0;
    // int ftxnNum[UserInput::getTranClients()];
    // SQLLEN indicator1 = 0;
    int done = 0;

    if (g->freshnessPeriod == 1 && UserInput::getTranClients() > 0)
    {
        while (ret != 0)
        {
            // ret = Driver::executeStmt(qStmt[q + 13]);
            ret = Driver::executeStmtDiar(qStmt[q], SQLDialect::analyticalQueries[q].c_str());
        }
        done = 1;
    }
    else if ((g->freshnessPeriod == 0 && UserInput::getTranClients() >= 0) || (g->freshnessPeriod == 1 && UserInput::getTranClients() == 0))
    {
        while (ret != 0)
        {
            ret = Driver::executeStmtDiar(qStmt[q], SQLDialect::analyticalQueries[q].c_str());
        }
    }

    if (ret == 0 && done == 1 && g->freshnessPeriod == 1 && UserInput::getTranClients() > 0)
    {
        // SQLBindCol(qStmt[q + 13], 1, SQL_C_DEFAULT, ftxnNum, sizeof(ftxnNum), &indicator1);
        // for (int j = 0; j < UserInput::getTranClients(); j++)
        // {
        //     Driver::fetchData(qStmt[q + 13]);
        //     Driver::getIntData(qStmt[q + 13], 1, ftxnNum[j]);
        //     fresh.push_back(g->containers[j]->GetFirstUnseenTxn(GetStartTimeQuery(), ftxnNum[j]));
        // }
        // maxFresh = *max_element(fresh.begin(), fresh.end());
        maxFresh = 0.0;
        SetFreshness(maxFresh);
        // Driver::resetStmt(qStmt[q + 13]);
        Driver::executeStmtDiar(qStmt[q], SQLDialect::analyticalQueries[q].c_str());
        done = 0;
    }
    else if ((g->freshnessPeriod == 0 && UserInput::getTranClients() >= 0) || (g->freshnessPeriod == 1 && UserInput::getTranClients() == 0))
    {
        // Driver::resetStmt(qStmt[q]);
        Driver::executeStmtDiar(qStmt[q], SQLDialect::analyticalQueries[q].c_str());
    }
    return ret;
}

void AnalyticalClient::FreeQueryStmt(Globals * g)
{
    for (unsigned int j = 0; j < SQLDialect::analyticalQueries.size(); j++)
    {
        if (UserInput::getTranClients() > 0 && g->freshnessPeriod == 1)
        {
            // Driver::freeStmtHandle(qStmt[j + 13]);
            Driver::freeStmtHandle(qStmt[j]);
        }
        if ((g->freshnessPeriod == 0 && UserInput::getTranClients() > 0) || (g->freshnessPeriod == 1 && UserInput::getTranClients() == 0))
            Driver::freeStmtHandle(qStmt[j]);
    }
}

void AnalyticalClient::SetExecutionTime(double execTime, int qType)
{
    executionTime[qType].push_back(execTime);
}

double AnalyticalClient::GetExecutionTimeSum(int qType)
{
    double sum = 0.0;
    int size = GetExecutionTimeSize(qType);
    for (int i = 0; i < size; i++)
        sum += executionTime[qType][i];
    return sum;
}

int AnalyticalClient::GetExecutionTimeSize(int qType)
{
    return executionTime[qType].size();
}


void AnalyticalClient::SetTestDuration(int duration)
{
    testDuration = duration;
}

int AnalyticalClient::GetTestDuration()
{
    return testDuration;
}

void AnalyticalClient::SetStartTimeQuery(long sTime)
{
    startTime = sTime;
}

long & AnalyticalClient::GetStartTimeQuery()
{
    return startTime;
}

void AnalyticalClient::SetFreshness(double & freshness)
{
    freshnessVector.push_back(freshness);
}

vector<double> & AnalyticalClient::GetFreshness()
{
    return freshnessVector;
}
