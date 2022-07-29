#include "DataGen.h"
#include "Driver.h"
#include "DBInit.h"
#include "Barrier.h"
#include "UserInput.h"
#include "Workload.h"
#include "GetFromDB.h"
#include "Globals.h"
#include "Results.h"
#include "Frontier.h"
#include <sql.h>
#include <sqltypes.h>
#include <iostream>
#include <functional>
#include <ctime>
using namespace std;

int main(int argc, char* argv[]){
    /* Read user input for flag initialization. */
    UserInput::processUserIn(argc, argv);
    if(UserInput::getWork() == 1){             // User selected data generation.
        auto startTime = chrono::system_clock::to_time_t(chrono::system_clock::now());
        cout << "\nChoice: [1] Data generation" << endl;
        cout << "START TIME of [1] " << ctime(&startTime) << endl;
        /* Generating initial data. */
        if(DataGen::dbGen() == 0)
            cout << "\nData generation completed!\n\n";
        else
            cout << "\nData generation failed.\n\n";
        auto endTime = chrono::system_clock::to_time_t(chrono::system_clock::now());
        cout << "\n[DONE] Choice: [1] Data generation" << endl;
        cout << "END TIME of [1] " << ctime(&endTime) << endl;
    }
    else if(UserInput::getWork() == 2){        // User selected to initiate the database.
        auto startTime = chrono::system_clock::to_time_t(chrono::system_clock::now());
        cout << "\nChoice: [2] Init database" << endl;
        cout << "START TIME of [2] " << ctime(&startTime) << endl;
        /* Connect to the DB. */
        SQLHENV env = 0;
        Driver::setEnv(env);
        SQLHDBC dbc = 0;
        Driver::connectDB(env, dbc);
        /* Create Schema. */
        SQLHSTMT stmt = 0;
        SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
        if(DBInit::dropSchema(stmt) == 0)
            cout << "\nStep1: Previous schema was deleted!\n\n";
        else
            cout << "\n\tStep1: Previous schema deletion failed!\n\n";

        if(DBInit::createSchema(stmt) == 0)
            cout << "\nStep2: New schema creation completed!\n\n";
        else
            cout << "\n\tStep2: New schema creation failed!\n\n";

        if(DBInit::bulkLoad(stmt) == 0)
            cout << "\nStep3: Bulk loading completed!\n\n";
        else
            cout << "\n\tStep3: Bulk loading failed!\n\n";

        if(DBInit::indexCreation(stmt) == 0)
            cout << "\nStep4: Index creation completed!\n\n";
        else
            cout << "\n\tStep4: Index creation failed!\n\n";
        Driver::freeStmtHandle(stmt);
        Driver::disconnectDB(dbc);
        //Driver::freeEnvHandle(env);
        cout << "\nDB initialization was successfully completed!\n\n";
        auto endTime = chrono::system_clock::to_time_t(chrono::system_clock::now());
        cout << "\n[DONE] Choice: [2] Init database" << endl;
        cout << "END TIME of [2] " << ctime(&endTime) << endl;

    }

    else if(UserInput::getWork() == 3){         // User selected to run the benchmark
        auto * frontier = new Frontier();
        int    ac       = 0;
        if (UserInput::analInputClients != 0)
        {
            ac = UserInput::analInputClients;
        }
        else
        {
            ac = frontier->findMaxClientCount(Frontier::WorkloadType::Analytical, UserInput::analMinClients);
            if (ac < 10)
            {
                // keep the min to be 10, make it better to sample the performance under {0.1, 0.2, 0.5, 0.8} * max_a
                cout << "increase max ac from " << ac << " to 10" << endl;
                ac = 10;
            }
        }
        cout << "pick " << ac << " as max ac" << endl;
        frontier->setMaxAC(ac);

        int tc = 0;
        if (UserInput::tranInputClients != 0)
        {
            tc = UserInput::tranInputClients;
        }
        else
        {
            tc = frontier->findMaxClientCount(Frontier::WorkloadType::Transactional, UserInput::tranMinClients);
        }
        cout << "pick " << tc << " as max tc" << endl;
        frontier->setMaxTC(tc);

        frontier->findFrontier();
    }

    else if(UserInput::getWork() == 4){         // User selected to run the benchmark
        int ac = UserInput::getAnalClients();
        int tc = UserInput::getTranClients();
        Frontier::createFreshnessTable(tc); // the freshness table is recreated every time the number of T clients changes

        auto startTime = chrono::system_clock::to_time_t(chrono::system_clock::now());
        cout << "\nChoice: [4] Run Benchmark with tc=" << tc << ", ac=" << ac << endl;
        cout << "START TIME of [4] " << ctime(&startTime) << endl;
        SQLHENV env = 0;
        Driver::setEnv(env);
        auto g = std::make_unique<Globals>();
        GetFromDB::getNumOrders(reinterpret_cast<int &>(g->loOrderKey), env);
        g->barrierW = new Barrier(tc+ac);
        g->barrierT = new Barrier(tc+ac);
        g->typeOfRun = none;
        auto workload = std::make_unique<Workload>();
        workload->ExecuteWorkloads(g.get());
        auto r = std::make_unique<Results>();
        workload->ReturnResults(r.get());
        auto endTime = chrono::system_clock::to_time_t(chrono::system_clock::now());
        cout << "\n[DONE] Choice: [3] Run Benchmark with tc=" << tc << ", ac=" << ac << ", t_throughput=" << r->getTransactionalThroughput()
             << ", a_throughput=" << r->getAnalyticalThroughput() << endl;
        cout << "START TIME of [4] " << ctime(&startTime) << endl;
        cout << "END TIME of [4] " << ctime(&endTime) << endl;

        Frontier::deleteTuples();
    }
    return 0;

}
