# -*- coding: utf-8 -*-

import sys
import random
import matplotlib.pyplot
import pylab
import matplotlib.pyplot as plt
import math
import numpy

import sklearn.datasets

import treeHandler
import diplomaCicksDBN as util

class Iris:

    def __init__(self,sepalLength,sepalWidth,petalLength,petalWidth,name):
        self.sLength=sepalLength
        self.sWidth=sepalWidth
        self.pLength=petalLength
        self.pWidth=petalWidth
        self.name=name

class Feature:    
    
    # label represents the features class:
    # real(continous value) or class value
    
    def __init__(self, value,label):
        self.value=value
        self.label=label
    
    @property
    def getValue(self):
        return self.value
    
    @property
    def getLabel(self):
        return self.label

    def __repr__(self):
        return str(self.value)

idx1=2
idx2=3

path='/home/tierex/opt/resourses/iris_fisher/iris.data'

def parseFile(path):
    irises=[]
    fd=open(path,'r')
    for line in fd:
        vals=line.split('\n')[0].split(',')
        if(len(vals)!=5):
            continue
        iris=Iris(float(vals[0]),float(vals[1]),float(vals[2]),float(vals[3]),vals[4])
        irises.append(iris)
    fd.close
    return irises

def transform(labels,irises=[Iris(1,2,3,4,'default')]):
    vector=[[],[],[],[],[]]
    for i in range(len(irises)):
        vector[0].append(irises[i].sLength)
        vector[1].append(irises[i].sWidth)
        vector[2].append(irises[i].pLength)
        vector[3].append(irises[i].pWidth)
        vector[4].append(labels[irises[i].name])
    return vector

def plotscatter2d(vector,color='r',m='o'):
    pylab.scatter(vector[0], vector[1], linewidth=1.0,c=color,marker=m,s=70)

def parseComplexData(data,idx=[0,1],labels=['1','2','3'],labelIndex=2):
    v1=[[],[]]
    v2=[[],[]]
    v3=[[],[]]
    for i in range(len(data[0])):
        if(data[labelIndex][i]==labels[0]):
            v1[0].append(data[idx[0]][i])
            v1[1].append(data[idx[1]][i])
        elif(data[labelIndex][i]==labels[1]):
            v2[0].append(data[idx[0]][i])
            v2[1].append(data[idx[1]][i])
        elif(data[labelIndex][i]==labels[2]):
            v3[0].append(data[idx[0]][i])
            v3[1].append(data[idx[1]][i])
    return [v1,v2,v3]

def plotdata(data,idx=[0,1],labels=['1','2','3'],labelIndex=2,marker='o'):
    vectors=parseComplexData(data,idx=idx,labels=labels,labelIndex=labelIndex)
    plotscatter2d(vectors[0],color='r',m=marker)
    plotscatter2d(vectors[1],color='g',m=marker)
    plotscatter2d(vectors[2],color='y',m=marker)

def shuffle(data,count=10000):
    for i in range(count):
        fr=random.randint(1,len(data[0]))-1
        to=random.randint(1,len(data[0]))-1
        swap(data[0],fr,to)
        swap(data[1],fr,to)
        swap(data[2],fr,to)
        swap(data[3],fr,to)
        swap(data[4],fr,to)
    return data

def swap(data,fr,to,):
    temp=data[fr]
    data[fr]=data[to]
    data[to]=temp


def splitData(data,percent=0.3):
    train=[[],[],[],[],[]]
    test=[[],[],[],[],[]]
    testsize=int(len(data[0])*percent)
    for i in range(testsize):
        test[0].append(data[0][i])
        test[1].append(data[1][i])
        test[2].append(data[2][i])
        test[3].append(data[3][i])
        test[4].append(data[4][i])
    for i in range(testsize+1,len(data[0])):
        train[0].append(data[0][i])
        train[1].append(data[1][i])
        train[2].append(data[2][i])
        train[3].append(data[3][i])
        train[4].append(data[4][i])
    return [train,test]

def getCol(data,index):
    element=[]
    for i in range(len(data)):
        element.append(data[i][index])
    return element
    
def computeError(tree,testFeatures,testOutput,root):
    errors=0.0
    for index in range(len(testFeatures)):
        prediction=tree.classify(root,testFeatures[index])
        print "predicted: %f, real value: %f"%(prediction,testOutput[index])
        if prediction!=testOutput[index]:
            errors+=1
    return errors


#labels={'Iris-setosa':0, 'Iris-virginica':1,'Iris-versicolor':2}
#
#data=shuffle(transform(labels,parseFile(path)))
#
#train,test=splitData(data)
#
#print len(train[0]),len(test[0])
#
#root=treeHandler.buildCARTTree(train[0:4],train[4],[0,1,2])
#
#errors=computeError(test[0:4],test[4],root)
#
#print errors

def main():
#    take=10000
#    trainFeatures,trainOutput=util.transformClickData('/home/tierex/opt/resourses/letor/Fold1/transformedTrain.txt',take)
#    testFeatures,testOutput=util.transformClickData('/home/tierex/opt/resourses/letor/Fold1/transformedTest.txt',take)
#    
#    print len(trainFeatures),len(trainFeatures[0]),len(testFeatures),len(testFeatures[0])
#    
#    tree=treeHandler.CARTTree()    
#     
#    root=tree.buildCARTTree(trainFeatures,trainOutput,5)
#    
#    errors=computeError(tree,testFeatures,testOutput,root)
#    print errors,len(testOutput),float(errors)/len(testOutput)
    
    data=sklearn.datasets.load_iris()
    print data.target
    
    
#    print treeHandler.bestSplitOne(trainFeatures,[0,1,2,3,4,5,6,7,8],
#                               trainOutput,5,treeHandler.computeRealInformationGainParrallel)
    
    
if __name__ == "__main__":
    main()



#
#print treeHandler.bestSplitAll(trainFeatures,[0,1,2,3,4,5,6,7,8],
#                               trainOutput,5,treeHandler.computeRealInformationGain)


#


#print treeHandler.printTreeDistribution(root)


##print data[1:3]
#
#print treeHandler.findOutputRate(data[4],3)

#for row in data:
#    print row
#
#print treeHandler.computeRealInformationGain(data[4],data[4],3)
#
#tempArr=[(1,2),(4,3),(33,5)]
#
#def testFunc(arr):
#    arr=[] 
#    return  1
#    
#testFunc(tempArr)
#
#print tempArr
#
#print numpy.argmax(tempArr)