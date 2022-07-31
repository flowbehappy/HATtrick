
#ifndef HATTRICKBENCH_ANALYTICALCLIENT_H
#define HATTRICKBENCH_ANALYTICALCLIENT_H
#include "Driver.h"
#include "SQLDialect.h"
#include "Globals.h"
#include <thread>
#include <iostream>
#include <vector>
#include <chrono>
#include <algorithm>

using namespace std;

class AnalyticalClient{
private:
    thread::id threadNum;     // id of thread
    vector<SQLHSTMT> qStmt; 
    int queriesNum;       // counter of queries executed in testing period in total
    vector<double> freshnessVector;   // freshness  for each analytical query executed in the current thread
    vector<vector<double>> executionTime;   // execution time of each query
    int testDuration;
    long startTime;    // start time of the current analytical query, different for each A client

public:
    AnalyticalClient();
    void SetThreadNum(thread::id num);
    void IncrementQueriesNum();
    int GetQueriesNum() const;
    thread::id GetThreadNum();
    void PrepareAnalyticalStmt(SQLHDBC& dbc);
    int ExecuteQuery(int& q, Globals* g, SQLHDBC& dbc);
    void SetFreshness(double& freshness);
    vector<double>& GetFreshness();
    void FreeQueryStmt(Globals* g);
    void SetExecutionTime(double execTime, int qType);
    double GetExecutionTimeSum(int qType);
    int GetExecutionTimeSize(int qType);
    void GetFreshnessSnapshot(Globals* g);
    void SetTestDuration(int duration);
    int GetTestDuration();
    void SetStartTimeQuery(long sTime);
    long& GetStartTimeQuery();
    
};
#endif //HATTRICKBENCH_ANALYTICALCLIENT_H
