#include <bits/stdc++.h>

using namespace std;

vector<pair<string, int>> metadata;
vector<int> columnPreference, columnSizes;
unordered_map<string, int> columnOrder;
string inputFile, outputFile;
long long int recordSize, sublistCount, recordLength, fileLength, sublistSize, maxMemory;
bool ascendingOrder;

bool compare_records(const vector<string> &a, const vector<string> &b)
{
    if (a.empty())
        return false;
    if (b.empty())
        return true;
    for (int i = 0; i < columnPreference.size(); i++)
    {
        if (a[columnPreference[i]] != b[columnPreference[i]])
        {
            if (ascendingOrder)
                return a[columnPreference[i]].compare(b[columnPreference[i]]) < 0;
            else
                return a[columnPreference[i]].compare(b[columnPreference[i]]) > 0;
        }
    }
    return true;
}

void phase1(long long int startofSublist, long long int endofSublist)
{
    ifstream input(inputFile);
    input.seekg(sublistSize * startofSublist, input.beg);
    for (long long int i = startofSublist; i < endofSublist; i++)
    {
        long long int records_to_read = sublistSize / recordLength;
        string record;
        vector<vector<string>> records;
        for (long long int j = 0; j < records_to_read; j++)
        {
            if (!getline(input, record))
                break;
            long long int read = 0;
            vector<string> row;
            for (int k = 0; k < columnSizes.size(); k++)
            {
                row.push_back(record.substr(read, columnSizes[k]));
                read += columnSizes[k];
                read += 2;
            }
            records.push_back(row);
        }
        cout << "> Sorting sublist: " << i << endl;
        sort(records.begin(), records.end(), compare_records);
        ofstream sublistOutFile;
        sublistOutFile.open(to_string(i) + ".sublist");
        cout << "> Writing sorted sublist " << i << " to disk\n";
        for (auto row : records)
        {
            for (auto col : row)
                sublistOutFile << col;
            sublistOutFile << "\n";
        }
        sublistOutFile.close();
    }
    input.close();
}

int main(int argc, char **argv)
{
    if (argc <= 5)
    {
        cout << "Usage: ./a.out <input_file> <output_file> <sublist_size> [<threads>] <order> <preference_columns>\n";
        return -1;
    }

    ifstream metadata_file("metadata.txt");
    if (!metadata_file.is_open())
    {
        cout << "> Error opening metadata file\n";
        return -1;
    }
    string line;

    while (getline(metadata_file, line))
    {
        int pos = line.find(",");
        string columnName = line.substr(0, pos);
        int columnBytes = stoi(line.substr(pos + 1));
        metadata.push_back({columnName, columnBytes});
        columnSizes.push_back(columnBytes);
        columnOrder[columnName] = metadata.size() - 1;
        recordSize += columnBytes;
    }
    metadata_file.close();

    inputFile = argv[1];
    outputFile = argv[2];
    maxMemory = stoi(argv[3]);
    sublistSize = maxMemory * 1000000;
    int columnsStart;
    long long int numThreads = 1;
    if (argv[4][0] == 'a')
        ascendingOrder = true, columnsStart = 5;
    else if (argv[4][0] == 'd')
        ascendingOrder = false, columnsStart = 5;
    else
    {
        numThreads = stoi(argv[4]);
        ascendingOrder = argv[5][0] == 'a';
        columnsStart = 6;
    }
    vector<bool> preferences(metadata.size(), false);
    for (int i = columnsStart; i < argc; i++)
    {
        columnPreference.push_back(columnOrder[argv[i]]);
        preferences[columnOrder[argv[i]]] = true;
    }
    for (int i = 0; i < preferences.size(); i++)
    {
        if (!preferences[i])
            columnPreference.push_back(i);
    }
    if (columnPreference.size() > metadata.size())
    {
        cout << "> Check column names of order preference provided\n";
        return -1;
    }
    cout << "> Input file: " << inputFile << endl;
    cout << "> Output file: " << outputFile << endl;
    cout << "> Sort order: " << (ascendingOrder ? "ascending" : "descending") << endl;
    cout << "> Preference: ";
    for (int i = 0; i < columnPreference.size(); i++)
    {
        cout << metadata[columnPreference[i]].first << ' ';
    }
    cout << endl;
    cout << "> Metadata: \n";
    for (auto k : metadata)
        cout << "> " << k.first << ", " << k.second << '\n';
    cout << "> Record size: " << recordSize << endl;
    cout << "> Parallelism: " << (numThreads == 1 ? "sequential" : "parallel") << endl;
    if (numThreads > 1)
        cout << "> Number of threads for first phase: " << numThreads << endl;

    ifstream input(inputFile);
    if (!input.is_open())
    {
        cout << "> Error opening input file\n";
        return -1;
    }
    input.seekg(0, input.end);
    fileLength = input.tellg();
    input.seekg(0, input.beg);
    cout << "> File length: " << fileLength << " Bytes\n";
    getline(input, line);
    recordLength = input.tellg();
    input.close();

    sublistSize /= numThreads;
    sublistSize = sublistSize - (sublistSize % recordLength);
    sublistCount = (fileLength + sublistSize - 1) / sublistSize;

    if (sublistCount * recordLength > maxMemory * 1000000)
    {
        if (numThreads == 1)
        {
            cout << "> Sublist size is too small\n";
            cout << "> Cannot fit " << sublistCount << " records of size " << recordLength << " in " << maxMemory << " MB\n";
            cout << "> Exiting\n";
            return -1;
        }
        while (numThreads > 1)
        {
            numThreads--;
            sublistSize = (maxMemory * 1000000) / numThreads; // in bytes
            sublistSize = sublistSize - (sublistSize % recordLength);
            sublistCount = (fileLength + sublistSize - 1) / sublistSize;
            if (sublistCount * recordLength <= maxMemory * 1000000)
            {
                cout << "> Cannot fit in given sublist size for given number of threads" << numThreads << endl;
                cout << "> Updating the number of threads to: " << numThreads << endl;
                break;
            }
        }
        if (sublistCount * recordLength > maxMemory * 1000000)
        {
            cout << "> Cannot perform 2-phase merge sort with given conditions\n";
            cout << "> Exiting\n";
            return -1;
        }
    }

    cout << "> Record length: " << recordLength << " bytes" << endl;
    cout << "> Actual record length: " << recordSize << " bytes" << endl;
    cout << "> Total records: " << fileLength / recordSize << endl;
    cout << "> Using memory: " << maxMemory << " MB" << endl;
    cout << "> Sublist size: " << sublistSize / 1000000 << " MB" << endl;
    cout << "> Sublist count: " << sublistCount << endl;
    cout << endl;

    cout << "> Running phase-1\n";

    auto start = std::chrono::high_resolution_clock::now();
    long long int sublistPerThread = (sublistCount + numThreads - 1) / numThreads;
    vector<thread> threads;
    for (long long int i = 0; i < sublistCount; i += sublistPerThread)
    {
        long long int end = min(i + sublistPerThread, sublistCount);
        threads.push_back(thread(phase1, i, end));
    }
    for (auto &t : threads)
        t.join();

    double phase1Time = chrono::duration_cast<chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start).count();

    ifstream sublistFiles[sublistCount];
    vector<vector<string>> sublistData(sublistCount);
    string record;
    ofstream result;
    result.open(outputFile);

    cout << "> Running Phase-2\n";
    start = std::chrono::high_resolution_clock::now();
    // phase 2
    for (long long int i = 0; i < sublistCount; i++)
    {
        sublistFiles[i].open(to_string(i) + ".sublist");
        if (!sublistFiles[i].is_open())
        {
            cout << "> Error opening temporary sublist file\n";
            return -1;
        }
    }
    for (long long int i = 0; i < sublistCount; i++)
    {
        if (!getline(sublistFiles[i], record))
            sublistData[i].clear();
        else
        {
            long long int currentPosition = 0;
            vector<string> row;
            for (int j = 0; j < columnSizes.size(); j++)
            {
                row.push_back(record.substr(currentPosition, columnSizes[j]));
                currentPosition += columnSizes[j];
            }
            sublistData[i] = row;
        }
    }

    for (long long int i = 0; i < (fileLength / recordLength); i++)
    {
        auto element = min_element(sublistData.begin(), sublistData.end(), compare_records);
        int index = distance(sublistData.begin(), element);
        for (auto j = sublistData[index].begin(); j != sublistData[index].end(); j++)
        {
            result << *j;
            if (j != sublistData[index].end() - 1)
                result << "  ";
        }
        result << '\n';
        if (!getline(sublistFiles[index], record))
            sublistData[index].clear();
        else
        {
            long long int currentPosition = 0;
            vector<string> row;
            for (int j = 0; j < columnSizes.size(); j++)
            {
                row.push_back(record.substr(currentPosition, columnSizes[j]));
                currentPosition += columnSizes[j];
            }
            sublistData[index] = row;
        }
    }
    result.close();

    for (long long int i = 0; i < sublistCount; i++)
        sublistFiles[i].close();

    for (long long int i = 0; i < sublistCount; i++)
        remove((to_string(i) + ".sublist").c_str());

    double phase2Time = chrono::duration_cast<chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start).count();
    cout << endl;
    cout << "> Phase-1 time: " << phase1Time << " ms\n";
    cout << "> Phase-2 time: " << phase2Time << " ms\n";
    cout << "> Total time: " << phase1Time + phase2Time << " ms\n";
}