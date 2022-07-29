#ifndef HATTRICKBENCH_FRONTIER_H
#define HATTRICKBENCH_FRONTIER_H
#include "Driver.h"
#include "Barrier.h"
#include "Workload.h"
#include "GetFromDB.h"
#include "Globals.h"
#include "Results.h"
#include "UserInput.h"
#include "SQLDialect.h"
#include <stdio.h>
#include <sql.h>
#include <sqlext.h>
#include <iostream>
#include <vector>
#include <ctime>

using namespace std;

class Frontier{
private:
    int max_tc = 0;
    int max_ac = 0;
public:
    static void deleteTuples();
    static void createFreshnessTable(int tc);

    enum class WorkloadType {
        Transactional,
        Analytical,
    };
    double runBenchmark(int peak, WorkloadType workload);

    int findMaxClientCount(WorkloadType workload, int min_num);

    void setMaxTC(int tc);
    void setMaxAC(int ac);
    int getMaxTC() const;
    int getMaxAC() const;
    void findFrontier();
};
#endif //HATTRICKBENCH_FRONTIER_H
