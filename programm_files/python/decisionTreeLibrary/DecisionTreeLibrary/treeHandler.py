import sys
import math
import numpy
import random
import multiprocessing
import diplomaCicksDBN as util
import Queue
import time

class DataRow(object):
    
    def __init__(self,features,output):
        self.features=features
        self.output=output

class Classificator(object):
    
    def __init__(self,function,featureIndex,criteriaValue):
        self.function=function
        self.featureIndex=featureIndex
        self.criteriaValue=criteriaValue
        
    def classify(self,features):
        return self.function(features[self.featureIndex],self.criteriaValue)

class Node(object):
    
    def __init__(self,right,left):
        self._right=right
        self._left=left
        
    @property
    def left(self):
        return self._left
    
    @property
    def right(self): 
        return self._right
        
        
class Leaf(Node):
    
    def __init__(self,right,left,outputDistribution):
        super(Leaf,self).__init__(right,left)
        self.outputDistribution=outputDistribution

        
class Branch(Node):
    
    def __init__(self,right,left,classificator):
        super(Branch,self).__init__(right,left)
        self.classificator=classificator
        
        
    def classify(self,features):
        if(self.classificator.classify(features)):
            return self.left
        return self.right

class CARTTree(object):
    
    def buildData(self,features,output):
        data=[]
        for index in range(len(output)):
            dataRow=DataRow(features[index],output[index])
            data.append(dataRow)
        return data
    
    def buildCARTTree(self,features,output,oLabels,minAmount=6):
        featureCount=130
        data=self.buildData(features,output)                
        m_vector=[]
        for f in range(featureCount):
            m_vector.append(f)
        root=self.buildCARTNode(data,m_vector,oLabels,0.6,1000,0,self.bestClassificationSplit,minAmount)
        return root
        
    def buildCARTNode(self,data,featureIndexes,labels,minPercent,maxDepth,currentDepth,bestSplit,minAmount):
        label,percent,labelsDistribution=self.findOutputRate(data,labels)    
        if len(data)<=minAmount or percent >= minPercent or maxDepth<=currentDepth:
            return Leaf(None,None,labelsDistribution)
        splitCriteria=lambda a,b:a<=b
        featureVectorIndex,featureValue,informativeSplit=self.bestClassificationSplit(data,featureIndexes,labels,splitCriteria)
        if not informativeSplit: return Leaf(None,None,labelsDistribution)
        classificator=Classificator(splitCriteria,featureVectorIndex,featureValue)
        leftData,rightData=self.splitVectors(data,len(data),classificator)
        print 'splitting result: left size: %s right size: %s'%(len(leftData),len(rightData))    
        node=Branch(self.buildCARTNode(rightData,featureIndexes,labels,minPercent,maxDepth,currentDepth+1,bestSplit,minAmount),
                      self.buildCARTNode(leftData,featureIndexes,labels,minPercent,maxDepth,currentDepth+1,bestSplit,minAmount),
                            classificator)
        return node

    def classify(self,node,features):        
        if isinstance(node,Leaf):
            return numpy.argmax(node.outputDistribution)
        return self.classify(node.classify(features),features)
        
    
    def printTreeDistribution(self,node):
        if isinstance(node,Leaf):
            print node.outputDistribution
            return 0
        elif not (node == None):
            self.printTreeDistribution(node.left)
            self.printTreeDistribution(node.right)
            
    def bestClassificationSplit(self,data,featureIndexes,labels,splitCriteria):
        index=random.randint(0,len(featureIndexes)-1)
        featureValue=0.0
        cycles=0
        informativeSplit=False
        while(not informativeSplit):
            featureVectorIndex=featureIndexes[index]
            featureValue,informativeSplit=self.computeInformationGain(data,labels,splitCriteria,featureVectorIndex)        
            if index!=len(featureIndexes)-1: index+=1
            else: index=0
            cycles+=1
            if cycles==len(featureIndexes)+1:
                print 'uninformative dataset'            
                break
        return featureVectorIndex,featureValue,informativeSplit
        
    def countOutputs(self,data,labels):        
        emptyDistribution=[]        
        outputDistribution=[]
        for i in range(labels):
            outputDistribution.append(0)
            emptyDistribution.append(0)
        for i in range(len(data)):
            outputDistribution[data[i].output]+=1
        return emptyDistribution,outputDistribution
        
    def dataRowCompare(self,dataRow1,dataRow2,featureVectorIndex):
        if(dataRow1.features[featureVectorIndex]>dataRow2.features[featureVectorIndex]):
            return 1
        elif(dataRow1.features[featureVectorIndex]<dataRow2.features[featureVectorIndex]):
            return -1
        else: return 0
    
    def computeInformationGain(self,data,labels,splitCriteria,
                               featureVectorIndex):
        leftDistr,rightDistr=self.countOutputs(data,labels)
        
        data=sorted(data,cmp=lambda x,y: self.dataRowCompare(x,y,featureVectorIndex))        
        
        minInfGain=100500.0
        informativeSplit=False
        featureValue=0.0
        
        for index in range(len(data)-1):
            if (data[index].features[featureVectorIndex]==data[0].features[featureVectorIndex] or
                data[index].features[featureVectorIndex]==data[len(data)-1].features[featureVectorIndex]): continue
                        
            leftDistr[data[index].output]+=1
            rightDistr[data[index].output]-=1
            
            lowValue=self.gini(leftDistr,len(data))
            highValue=self.gini(rightDistr,len(data))
            
            infGain=lowValue*numpy.sum(leftDistr)/len(data) + highValue*numpy.sum(rightDistr)/len(data)
            if minInfGain>infGain:
                minInfGain=infGain
                featureValue=data[index].features[featureVectorIndex]
        if minInfGain!=100500.0:
            informativeSplit=True
        return featureValue,informativeSplit
            
            
    def gini(self,distribution,length):
        giniValue=0.0
        for distr in distribution:
            probability=float(distr)/length
            giniValue+=probability*(1-probability)
        return giniValue

    
    def computeInformationGainParallel(self,data,labels,splitCriteria,
                               featureVectorIndex,threadCount=4):
        queue=multiprocessing.Queue()
        parts=util.getBounds(len(data),threadCount)
        threads=[]
        startTime=time.time()
        for part in parts:
            thread=ComputeMaxInformationGain(data,featureVectorIndex,labels,part[0],part[1],queue,splitCriteria)
            threads.append(thread)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        minInfGain=100500.0
        featureValue=0.0
        informativeSplit=False
        for i in range(len(parts)):
            data=queue.get()
            if minInfGain>data[0]:
                minInfGain=data[0]
                featureValue=data[1]
        if minInfGain!=100500.0: informativeSplit=True
#        print 'inf gain compute took time:',(time.time()-startTime),' for length: ',len(output)    
        return featureValue,informativeSplit
            
    def splitVectors(self,data,fLen,classificator):
        rightData=[]
        leftData=[]        
        for index in range(fLen):
            if(classificator.classify(data[index].features)):
                leftData.append(data[index])
            else:
                rightData.append(data[index])
        return leftData,rightData
        
    def findOutputRate(self,data,labelSize):
        labels=[]
        for i in range(labelSize):
            labels.append(0.0)
        for dataRow in data:
            labels[dataRow.output]+=1.0
        for index in range(labelSize):
            labels[index]=labels[index]/float(len(data))
        mx=numpy.argmax(labels)
        return mx,labels[mx],labels

class ComputeMaxInformationGain(multiprocessing.Process):
    
    def __init__(self,data,featureVectorIndex,labels,fromIndex,toIndex,outputQueue,splitCriteria):
        multiprocessing.Process.__init__(self)
        self.splitCriteria=splitCriteria
        self.outputQueue=outputQueue
        self.data=data
        self.featureVectorIndex=featureVectorIndex
        self.labels=labels
        self.fromIndex=fromIndex
        self.toIndex=toIndex
        
    def run(self):
        outLen=len(self.output)
        infGain=100500.0
        featureValue=0.0
        start=time.time()
        leftDistr,rigthDistr=self.countOutputs(self.data,self.labels)
#        leftLowIndex=self.fromIndex
#        leftHighIndex=self.fromIndex
#        rightLowIndex=self.fromIndex+1
#        rigthHighIndex=self.toIndex
#        for index in range(self.fromIndex,self.toIndex):
#            leftHighIndex=index
#            rightLowIndex=index+1              
#            feature=self.data[index].features[self.featureVectorIndex]
##            low,high=util.getSubsets(self.features[self.featureVectorIndex],
##                                     self.output,feature,self.splitCriteria)
#            if (self.data[self.fromIndex].features[self.featureVectorIndex]==feature or
#              self.data[self.toIndex].features[self.featureVectorIndex]==feature): continue
#            
#            lowEntropy=self.giniImpurity(low,self.labels)
#            highEntropy=self.giniImpurity(high,self.labels)
#            currInfGain=lowEntropy*float(len(low))/outLen+ highEntropy*float(len(high))/outLen
#            if currInfGain < infGain:
#                infGain=currInfGain
#                featureValue=feature
        print (time.time()-start)
        self.outputQueue.put([infGain,featureValue])
        
    def countOutputs(self,data,labels):        
        emptyDistribution=[]        
        outputDistribution=[]
        for i in range(labels):
            outputDistribution.append(0)
            emptyDistribution.append(0)
        for i in range(len(data)):
            outputDistribution[data[i].output]+=1
        return emptyDistribution,outputDistribution
        
        
    def giniImpurity(self,output,labels):
        probs=[]
        for i in range(labels):
            probs.append(0)
        for value in output:
            probs[value]+=1
        for i in range(len(probs)):
            probs[i]=float(probs[i])/len(output)
        entropy=0.0
        probs=filter(lambda l:l>0,probs)
        for i in range(len(probs)):
            entropy+=probs[i]*(1-probs[i])
        return entropy

        
    