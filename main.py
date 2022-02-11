import os
import sys
import time
import threading

if len(sys.argv) <= 5:
    print(
        "Usage: ./a.out <input_file> <output_file> <sublist_size> [<threads>] <order> <preference_columns>")
    sys.exit(1)
try:
    metadata_file = open("metadata.txt", "r")
except FileNotFoundError:
    print("Error: metadata.txt not found")
    sys.exit(1)

columnSizes = []
columnOrder = {}
recordSize = 0

metadata = metadata_file.read().splitlines()
for i in range(len(metadata)):
    colName, colSize = metadata[i].split(',')
    metadata[i] = (colName, int(colSize))
    columnSizes.append(int(colSize))
    columnOrder[colName] = i
    recordSize += int(colSize)
metadata_file.close()

inputFile = sys.argv[1]
outputFile = sys.argv[2]
maxMemory = int(sys.argv[3])
sublistSize = maxMemory*1000000
columnStart = -1
numThreads = 1
ascendingOrder = True
if sys.argv[4].lower() == "asc":
    ascendingOrder = True
    columnStart = 5
elif sys.argv[4].lower() == "desc":
    ascendingOrder = False
    columnStart = 5
else:
    numThreads = int(sys.argv[4])
    ascendingOrder = (sys.argv[5].lower() == "asc")
    columnStart = 6
columnPreference = []
preferences = [False] * len(metadata)
for i in range(columnStart, len(sys.argv)):
    columnPreference.append(columnOrder[sys.argv[i]])
    preferences[columnOrder[sys.argv[i]]] = True
for i in range(len(preferences)):
    if not preferences[i]:
        columnPreference.append(i)
if len(columnPreference) > len(metadata):
    print("Error: too many columns specified")
    sys.exit(1)
print("Input file: " + inputFile)
print("Output file: " + outputFile)
print("Metadata:")
for i in range(len(metadata)):
    print(" "+metadata[i][0] + " : " + str(metadata[i][1]))
print("Column preference: " + str(columnPreference))
print("Ascending order: " + str(ascendingOrder))
print("Number of threads: " + str(numThreads))
print("Record size: " + str(recordSize))

try:
    input_file = open(inputFile, "r")
except FileNotFoundError:
    print("Error: input file not found")
    sys.exit(1)

fileLength = os.stat(inputFile).st_size
print("File length: " + str(fileLength))
recordLength = len(input_file.readline())+1  # +1 for \r :TODO
input_file.close()

sublistSize /= numThreads
sublistSize = sublistSize - (sublistSize % recordLength)
sublistCount = int((fileLength+sublistSize-1)//sublistSize)

if sublistCount*recordLength > maxMemory*1000000:
    if numThreads == 1:
        print("Sublist size is too small")
        sys.exit(1)
    while numThreads > 1:
        numThreads -= 1
        sublistSize = (maxMemory*1000000)/numThreads
        sublistSize = sublistSize - (sublistSize % recordLength)
        sublistCount = (fileLength+sublistSize-1)//sublistSize
        if sublistCount*recordLength <= maxMemory*1000000:
            print("Cannot fit all records in given sublist size")
            print("Updated thread count: " + str(numThreads))
            break
    if sublistCount*recordLength > maxMemory*1000000:
        print("Cannot perform sorting with given configuration")
        sys.exit(1)

print("Record length: " + str(recordLength))
print("Actual record length: " + str(recordSize))
print("Total records: " + str(fileLength//recordLength))
print("Sublist size: " + str(sublistSize))
print("Sublist count: " + str(sublistCount))
print("Using Memory: " + str(maxMemory))

print("Running Phase 1\n")

start = time.time()
sublistPerThread = (sublistCount+numThreads-1)//numThreads
threads = []


def phase1(startIndex, endIndex):
    try:
        input_file = open(inputFile, "r")
    except FileNotFoundError:
        print("Error: input file not found")
        sys.exit(1)
    input_file.seek(startIndex*sublistSize, 0)
    for i in range(startIndex, endIndex):
        records_to_read = int(sublistSize//recordLength)
        records = []
        for j in range(records_to_read):
            z = input_file.readline()
            if z == "":
                break
            record = []
            for colSize in columnSizes:
                record.append(str(z[:colSize]))
                z = z[colSize+2:]
            records.append(record)
        records.sort(key=lambda x: tuple(
            x[i] for i in columnPreference), reverse=not ascendingOrder)
        sublistOutFile = open(str(i)+".sublist", "w")
        print("Writing to file: " + str(i)+".sublist")
        for record in records:
            sublistOutFile.write("  ".join(record) + "\n")
        sublistOutFile.close()
    input_file.close()


for i in range(0, sublistCount, sublistPerThread):
    endIndex = (i+sublistPerThread)
    if endIndex > sublistCount:
        endIndex = sublistCount
    thread = threading.Thread(target=phase1, args=(i, endIndex))
    threads.append(thread)
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
phase1Time = time.time() - start
print("\nPhase 1 time: ", phase1Time)

sublistFiles = []
sublistData = []
result = open(outputFile, "w")
print("\nRunning Phase 2")
start = time.time()
for i in range(sublistCount):
    sublistFile = open(str(i)+".sublist", "r")
    if sublistFile is None:
        print("Error: sublist file not found")
        sys.exit(1)
    sublistFiles.append(sublistFile)

for i in range(sublistCount):
    line = sublistFiles[i].readline()
    if line == "":
        sublistData.append(None)
    else:
        record = []
        for colSize in columnSizes:
            record.append(line[:colSize])
            line = line[colSize+2:]
        sublistData.append(record)


for i in range(int(fileLength//recordLength)):
    record = min(sublistData, key=lambda x: tuple(
        x[i] for i in columnPreference))
    i = sublistData.index(record)
    result.write("  ".join(record) + "\n")
    record = sublistFiles[i].readline().rstrip('\n')
    if record == "":
        sublistData[i] = None
    else:
        sublistData[i] = record.split("  ")
result.close()
for i in range(sublistCount):
    sublistFiles[i].close()
    os.remove(str(i)+".sublist")
phase2Time = time.time() - start
print("Phase 2 time: ", phase2Time)
print("\nTotal time: ", phase1Time+phase2Time)
