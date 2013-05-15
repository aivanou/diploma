import re


def computeHadoopQCounts():
    def computeQueryCounts(filename):
        fl = open(filename)
        queries = {}
        newQuery = False
        for line in fl:
            if line == '\n' or len(line) == 0:
                newQuery = True
                continue
            if (newQuery):
                query = re.split('[ \t]+', line)[1]

            if not query in queries:
                queries[query] = 1
            else:
                queries[query] += 1
                newQuery = False
                return queries

    return 0


def transformLetorDataset(fromFile="/home/tierex/opt/resourses/letor/Fold1/test.txt",
                          toFile='/home/tierex/opt/resourses/letor/Fold1/transformedTest.txt'):
    readFile = open(fromFile, 'r')
    writeFile = open(toFile, 'w+')
    bufferMaxSize = 100000
    buffer = []
    currBufferLength = 0
    for line in readFile:
        if bufferMaxSize == currBufferLength:
            writeFile.writelines(buffer)
            buffer = []
            currBufferLength = 0
        buffer.append(transformLine(line))
        currBufferLength += 1
    writeFile.flush()
    writeFile.close()
    return 0


def transformLine(line):
    parts = line.split(' ')
    for index in range(len(parts)):
        if parts[index].find(':') == -1:
            continue
        parts[index] = parts[index].split(':')[1]
    newLine = ''
    for index in range(len(parts) - 1):
        newLine += parts[index] + ' '
    newLine += parts[len(parts) - 1]
    return newLine


def transformClickData(filename, maxNumber=50000):
    data = []
    labels = []
    features = 136
    currNumber = 0
    click_fl = open(filename, 'r')
    for line in click_fl:
        attrs = line.split(' ')
        labels.append(int(attrs[0]))
        data.append([])
        for index in range(features):
            data[len(data) - 1].append(float(attrs[index + 2]))
        if currNumber == maxNumber:
            return data, labels
        currNumber += 1
    return data, labels


def read_by_query(filename, max_number=5000):
    data = {}

    feature_number = 136
    currNumber = 0
    prev_query = ""
    click_fl = open(filename, 'r')
    for line in click_fl:
        attrs = line.split(' ')
        relevance = int(attrs[0])
        query = attrs[1]
        if prev_query != query:
            data[query] = []
            prev_query = query
        features = []
        for index in range(feature_number):
            features.append(float(attrs[index + 2].split(':')[1]))
        data[query].append((features, relevance))
        currNumber += 1
        if currNumber == max_number:
            return data

    return data


def splitData(data, testPercent=0.6, trainPercent=0.2):
    train = []
    test = []
    for i in range(len(data)):
        train.append([])
        test.append([])
    splitIndex = int(len(data[0]) * testPercent)
    for col in range(splitIndex):
        for row in range(len(data)):
            test[row].append(data[row][col])
    for col in range(splitIndex, len(data[0])):
        for row in range(len(data)):
            train[row].append(data[row][col])
    return train, test


def getBounds(featuresLength, partsNumber, threshold=100):
    parts = []
    if featuresLength <= threshold: return [[0, featuresLength]]
    part = featuresLength / partsNumber
    low = 0
    high = 0
    for i in range(partsNumber - 1):
        high += part
        parts.append([low, high])
        low += part
    parts.append([low, featuresLength])
    return parts


def getSubsets(featureVector, outputVector, value, condition=lambda a, b: a == b):
    lowVector = []
    highVector = []
    for i in range(len(featureVector)):
        if condition(featureVector[i], value):
            lowVector.append(outputVector[i])
        else:
            highVector.append(outputVector[i])
    return lowVector, highVector


def getColumn(matrix, colIndex):
    col = []
    for index in range(len(matrix)):
        col.append(matrix[index][colIndex])
    return col


    #transformLetorDataset()