import sys
import numpy
import random
import multiprocessing
import Queue
import math

import diplomaCicksDBN as util

import sklearn
import sklearn.tree
import sklearn.datasets
import sklearn.ensemble as ensemble
import matplotlib.pyplot


class Document(object):
    def __init__(self, features, position, estimated_relevance, computed_relevance):
        self.features = features
        self.position = position
        self.estimated_relevance = estimated_relevance
        self.computed_relevance = computed_relevance


def compare_est_relevance(doc1, doc2):
    if doc1.estimated_relevance > doc2.estimated_relevance:
        return -1
    elif doc1.estimated_relevance < doc2.estimated_relevance:
        return 1
    else:
        return 0


def compare_comp_relevance(doc1, doc2):
    if doc1.computed_relevance > doc2.computed_relevance:
        return -1
    elif doc1.computed_relevance < doc2.computed_relevance:
        return 1
    else:
        return 0


def dcg(documents, rel_function):
    value = rel_function(documents[0])
    for index in range(1, len(documents)):
        relevance = rel_function(documents[index])
        value += float(relevance) / math.log(index + 1, 2)
    return value


def ndcg(documents):
    sdocs = sorted(documents, cmp=compare_est_relevance)
    computed_idcg = dcg(sdocs, lambda doc: doc.estimated_relevance)
    sdocs = sorted(documents, cmp=compare_comp_relevance)
    computed_dcg = dcg(sdocs, lambda doc: doc.estimated_relevance)
    return float(computed_dcg + 1) / (computed_idcg + 1)


def rotate(vector):
    new_vector = []
    for col in range(len(vector[0])):
        new_vector.append([])
        for row in range(len(vector)):
            new_vector[col].append(vector[row][col])
    return new_vector


def getCol(data, index):
    element = []
    for i in range(len(data)):
        element.append(data[i][index])
    return element


def computeErrors(classifier, testFeatures, testOutput):
    errors = 0
    for index in range(len(testFeatures)):
        features = testFeatures[index]
        prediction = classifier.predict(features)
        if prediction != testOutput[index]:
            errors += 1
    return errors, len(testOutput)


maxNumber = 15000


def classify(data):
    iterations = 100
    step = 5
    start = 50

    it_array = []
    avg_ndcg_array = []
    for iteration in range(iterations):
        print 'iteration:', iteration
        classifier = ensemble.GradientBoostingClassifier(n_estimators=start + iteration * step)
        trainFeatures, trainOutput = util.transformClickData(
            '/home/tierex/opt/resourses/letor/Fold1/transformedTrain.txt', 1000)

        classifier.fit(trainFeatures, trainOutput)
        avg_ndcg = compute_avg_ndcg(data, classifier)
        print avg_ndcg
        it_array.append(start + iteration * step)
        avg_ndcg_array.append(avg_ndcg)

    matplotlib.pyplot.plot(it_array, avg_ndcg_array)
    matplotlib.pyplot.show()


def compute_avg_ndcg(data, classifier):
    count = 0
    avg_ndcg = 0.0
    for key in data.keys():
        documents = []
        for vector in data[key]:
            computed_relevance = classifier.predict(vector[0])
            document = Document(vector[0], 0, vector[1], computed_relevance)
            documents.append(document)
        v = ndcg(documents)
    count += 1
    avg_ndcg += v
    return float(avg_ndcg) / count


data = util.read_by_query("/home/tierex/opt/resourses/letor/Fold1/train.txt", max_number=50000)

# classifier = ensemble.GradientBoostingClassifier(n_estimators=250)

classify(data)



#classify(classifier)


#clf=sklearn.tree.DecisionTreeClassifier()
#
#clf=clf.fit(trainFeatures,trainOutput)
#
#errors=0
#
#for index in range(len(testFeatures)):
#    features=testFeatures[index]
#    prediction=clf.predict(features)
#    if prediction!=testOutput[index]:
#        errors+=1
#
#print errors,len(testOutput),float(errors)/len(testOutput)
