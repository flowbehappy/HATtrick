#include "Frontier.h"
#include <memory>

void  Frontier::deleteTuples(){
    SQLHENV env = 0;
    Driver::setEnv(env);
    SQLHDBC dbc = 0;
    Driver::connectDB(env, dbc);
    SQLHSTMT     stmt        = 0;
    char         c           = ';';
    unsigned int num_of_stmt = SQLDialect::deleteTuplesStmt[UserInput::getdbChoice()].size();
    std::string  q(1, c);
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    for (unsigned int i = 0; i < num_of_stmt; i++)
    {
        if (i != num_of_stmt - 1 && i != num_of_stmt - 2)
            Driver::executeStmtDiar(stmt, SQLDialect::deleteTuplesStmt[UserInput::getdbChoice()][i].c_str());
        else
        {
            Driver::executeStmtDiar(
                stmt, (SQLDialect::deleteTuplesStmt[UserInput::getdbChoice()][i] + std::to_string(UserInput::getLoSize()) + q).c_str());
        }
    }
    Driver::freeStmtHandle(stmt);
    Driver::disconnectDB(dbc);
}


void Frontier::createFreshnessTable(const int tc)
{
    SQLHENV env = 0;
    Driver::setEnv(env);
    SQLHDBC dbc = 0;
    Driver::connectDB(env, dbc);
    SQLHSTMT stmt = 0;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    std::string s_tc;
    for (unsigned int i = 1; i <= (unsigned int)tc; i++)
    {
        s_tc = std::to_string(i);
        Driver::executeStmtDiar(stmt,
                                (SQLDialect::deleteFreshnessTableStmt[0].c_str() + s_tc + SQLDialect::deleteFreshnessTableStmt[1]).c_str());
        Driver::executeStmtDiar(stmt,
                                (SQLDialect::createFreshnessTableStmt[0].c_str() + s_tc + SQLDialect::createFreshnessTableStmt[1]).c_str());
        Driver::executeStmtDiar(stmt,
                                (SQLDialect::populateFreshnessTableStmt[0] + s_tc + SQLDialect::populateFreshnessTableStmt[1] + s_tc
                                 + SQLDialect::populateFreshnessTableStmt[2])
                                    .c_str());
        if (UserInput::getdbChoice() == tidb)
        {
            Driver::executeStmtDiar(stmt,
                                    (SQLDialect::populateFreshnessTableStmt[3] + s_tc + SQLDialect::populateFreshnessTableStmt[4]).c_str());
        }
    }
    Driver::freeStmtHandle(stmt);
    Driver::disconnectDB(dbc);
}

double Frontier::runBenchmark(const int peak, const WorkloadType type)
{
    int ac = 0, tc = 0;
    switch (type)
    {
    case WorkloadType::Transactional:
        // seeking max for transactional workload
        tc = peak;
        break;
    case WorkloadType::Analytical:
        // seeking max for analytical workload
        ac = peak;
        break;
    }
    createFreshnessTable(tc);

    auto startTime = chrono::system_clock::to_time_t(chrono::system_clock::now());
    cout << "\nChoice: [3] Run Benchmark with tc=" << tc << ", ac=" << ac << endl;
    cout << "START TIME of [3] " << ctime(&startTime) << endl;
    SQLHENV env = 0;
    Driver::setEnv(env);
    UserInput::setAnalyticalClients(ac);
    UserInput::setTransactionalClients(tc);
    auto g = std::make_unique<Globals>();
    GetFromDB::getNumOrders(reinterpret_cast<int &>(g->loOrderKey), env);
    g->barrierW   = new Barrier(UserInput::getTranClients() + UserInput::getAnalClients());
    g->barrierT   = new Barrier(UserInput::getTranClients() + UserInput::getAnalClients());
    g->typeOfRun  = none;
    auto workload = std::make_unique<Workload>();
    workload->ExecuteWorkloads(g.get());
    auto r = std::make_unique<Results>();
    workload->ReturnResults(r.get());
    double peak_throughput = 0;
    switch (type)
    {
    case WorkloadType::Transactional:
        peak_throughput = r->getTransactionalThroughput();
        break;
    case WorkloadType::Analytical:
        peak_throughput = r->getAnalyticalThroughput();
        break;
    }
    auto endTime = chrono::system_clock::to_time_t(chrono::system_clock::now());
    cout << "\n[DONE] Choice: [3] Run Benchmark with tc=" << tc << ", ac=" << ac << ", peak_throughput=" << peak_throughput << endl;
    cout << "START TIME of [3] " << ctime(&startTime) << endl;
    cout << "END TIME of [3] " << ctime(&endTime) << endl;

    g.reset();
    workload.reset();
    r.reset();

    deleteTuples();
    return peak_throughput;
}

int Frontier::findMaxClientCount(const WorkloadType type, const int min_num)
{
    auto is_peak_found = [](const double cur_th, const double prev_th) {
        if (prev_th > 0.5)
        {
            return cur_th - prev_th > 0.05 * prev_th;
        }
        else
        {
            return cur_th - prev_th > 0.01 * prev_th;
        }
    };
    // First find max AC, then find max TC
    bool init_peak_found = false;
    int  step            = 8;
    int  clients         = min_num;
    int  i               = std::max(1, clients / step);
    cout << "search begin with: " << clients << endl;
    int    init_peak           = clients;
    int    previous_peak       = clients;
    double previous_throughput = runBenchmark(clients, type); // first round
    while (init_peak_found == false)
    {
        i++; // begin with second round
        clients                   = i * step;
        double current_throughput = runBenchmark(clients, type);
        if (is_peak_found(current_throughput, previous_throughput))
        {
            previous_throughput = current_throughput;
            previous_peak       = clients;
        }
        else
        {
            init_peak_found = true;
            init_peak       = previous_peak;
        }
    }

    int  final_peak;
    bool final_peak_found = false;
    i                     = 1;
    while (final_peak_found == false)
    {
        clients                   = init_peak + i;
        double current_throughput = runBenchmark(clients, type);
        if (is_peak_found(current_throughput, previous_throughput))
        {
            previous_throughput = current_throughput;
            previous_peak       = clients;
        }
        else
        {
            final_peak_found = true;
            final_peak       = previous_peak;
        }
        i++;
    }
    return final_peak;
}

void Frontier::findFrontier()
{
    const int        max_ac = getMaxAC();
    const int        max_tc = getMaxTC();
    int              tc, ac = 0;
    constexpr double ratios[6]  = {0, 0.1, 0.2, 0.5, 0.8, 1};
    constexpr int    ratios_num = sizeof(ratios) / sizeof(ratios[0]);
    for (int i = 0; i < ratios_num; i++)
    {
        for (int j = 0; j < ratios_num; j++)
        {
            if (floor(ratios[i] * max_tc) == 0 && ratios[i] != 0)
                tc = 1;
            else
                tc = floor(ratios[i] * max_tc);
            if (floor(ratios[j] * max_ac) == 0 && ratios[j] != 0)
                ac = 1;
            else
                ac = floor(ratios[j] * max_ac);

            createFreshnessTable(tc); // the freshness table is recreated every time the number of T clients changes

            auto startTime = chrono::system_clock::to_time_t(chrono::system_clock::now());
            cout << "\nChoice: [3] Run Benchmark findFrontier with tc=" << tc << ", ac=" << ac << endl;
            cout << "START TIME of [3] " << ctime(&startTime) << endl;
            SQLHENV env = 0;
            Driver::setEnv(env);
            UserInput::setAnalyticalClients(ac);
            UserInput::setTransactionalClients(tc);
            auto g = std::make_unique<Globals>();
            GetFromDB::getNumOrders(reinterpret_cast<int &>(g->loOrderKey), env);
            g->barrierW   = new Barrier(UserInput::getTranClients() + UserInput::getAnalClients());
            g->barrierT   = new Barrier(UserInput::getTranClients() + UserInput::getAnalClients());
            g->typeOfRun  = none;
            auto workload = std::make_unique<Workload>();
            workload->ExecuteWorkloads(g.get());
            auto r = std::make_unique<Results>();
            workload->ReturnResults(r.get());
            auto endTime = chrono::system_clock::to_time_t(chrono::system_clock::now());
            cout << "\n[DONE] Choice: [3] Run Benchmark with tc=" << tc << ", ac=" << ac
                 << ", t_throughput=" << r->getTransactionalThroughput() << ", a_throughput=" << r->getAnalyticalThroughput() << endl;
            cout << "START TIME of [3] " << ctime(&startTime) << endl;
            cout << "END TIME of [3] " << ctime(&endTime) << endl;

            g.reset();
            workload.reset();
            r.reset();

            deleteTuples();
            sleep(2);
        }
    }
}

void Frontier::setMaxTC(int tc){
    max_tc = tc;
}

void Frontier::setMaxAC(int ac){
    max_ac = ac;
}

int Frontier::getMaxTC() const{
    return max_tc;
}

int Frontier::getMaxAC() const{
    return max_ac;
}
