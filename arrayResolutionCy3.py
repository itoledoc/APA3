#!/usr/bin/python


"""
Script to return the Cycle 3 array parameter for a given LAS, Angular Resolution:

Input :
    AR, LAS, ACA (Y|N), TP (Y|N), 12-m (1|2)
    
Output :
    minAR, maxAR
    
    

HISTORY:
    - 2015.07.06:
        - start from scatch to clean arrayResolution2p.py
        - check if the LAS with 1 12-m array is okay.
        - if no solution is found change the AR by a fudge factor to converge
        - by default the frequency is taken to 100 GHz, no scaling is applied.
        - if 2 12-m array are requested it returns 2 minAR and maxAR.


"""

__author__ = "ALMA : SL"
__version__ = "0.1.0@2015.07.06"


import pickle
import math

LATITUDE_ALMA = -23.03
DEG2RAD = math.pi/180.


class arrayRes:

    def __init__(self):
                      
        self.res = [3.4, 1.8, 1.2, 0.7, 0.5, 0.3, 0.1, 0.075]
        self.las = [25.3, 25.2, 25.2, 9.6, 7.8, 4.8, 1.5, 1.1]           
            
        self.frequencyReference = 100.
        self.freqObs = 0.
        self.silent = True
        self.nof12m = 1
        self.minAR = [0., -99.]
        self.maxAR = [0, -99.]
        self.data = []

        self.array = {
            0: "C36-1", 1: "C36-2", 2: "C36-3", 3: "C36-4", 4: "C36-5",
            5: "C36-6", 6: "C36-7", 7: "C36-8"}
        self.matchArrayCycle3 = {3: 0, 4: 1, 5: 2}
        self.read_cycle3()
        

    def read_cycle3(self):
    
        f = open('/home/itoledo/Work/APA3/conf/Resolution-C36-1.pickle')
        self.data.append(pickle.load(f))
        f.close()
        
        f = open('/home/itoledo/Work/APA3/conf/Resolution-C36-2.pickle')
        self.data.append(pickle.load(f))
        f.close()
        
        f = open('/home/itoledo/Work/APA3/conf/Resolution-C36-3.pickle')
        self.data.append(pickle.load(f))
        f.close()
        
        f = open('/home/itoledo/Work/APA3/conf/Resolution-C36-4.pickle')
        self.data.append(pickle.load(f))
        f.close()
        
        f = open('/home/itoledo/Work/APA3/conf/Resolution-C36-5.pickle')
        self.data.append(pickle.load(f))
        f.close()
        
        f = open('/home/itoledo/Work/APA3/conf/Resolution-C36-6.pickle')
        self.data.append(pickle.load(f))
        f.close()
        
        f = open('/home/itoledo/Work/APA3/conf/Resolution-C36-7.pickle')
        self.data.append(pickle.load(f))
        f.close()
    
        f = open('/home/itoledo/Work/APA3/conf/Resolution-C36-8.pickle')
        self.data.append(pickle.load(f))
        f.close()  

    def find_array(self, ar, las, declination, useACA, noOf12, uid):
    
        nData = len(self.data[0][0])
        decMin = self.data[0][0][0]
        decMax = self.data[0][0][nData - 1]
        deltaDec = (decMax - decMin) / nData
        
        index = int(math.floor(((declination - decMin) / deltaDec)))
        
        scalingFrequency = 1.
    
        notFound = True
        
        match = []
        
        for arr in self.array:
            
            resArr = math.sqrt(
                self.data[arr][1][index] *
                self.data[arr][2][index])
            
            # print ar,  ar /2., resArr, ar * 1.1
            spatialResolutionArr = resArr * scalingFrequency

            if (ar * 1.1 > spatialResolutionArr >= ar / 2.
                    and noOf12 == 2):
                match.append(arr)
                notFound = False
            
            if (ar * 1.1 > spatialResolutionArr >= ar / 2.
                    and noOf12 == 1):
                # if useACA == 0:
                #     if self.las[arr] * 2. > las:
                #         match.append(arr)
                #         notFound = False
                #     else:
                #         print self.las[arr] * 1.25, las, uid
                # else:
                match.append(arr)
                notFound = False

            if arr == 0 and ar * 1.1 >= spatialResolutionArr:
                match.append(arr)
                notFound = False

        return match, notFound

    def run(self, AR, LAS, declination, useACA, noOf12, OT_BestConf, uid):

        if OT_BestConf == "C36-7":
            return [self.res[6] * 0.8, 0], [self.res[6] * 1.2, 0], 'C36-7', ''
        if OT_BestConf == "C36-8":
            return [self.res[7] * 0.8, 0], [self.res[7] * 1.2, 0], 'C36-8', ''

        if declination >= 43:
            declination = 40.

        self.minAR = [0., -99.]
        self.maxAR = [0, -99.]

        notFound = True
        maxTry = 100
        nTry = 1
        deltaFudgeFactor = 0.05
        fudgeFactor = 1.0
        
        while notFound and nTry < maxTry:
            nTry += 1
            matchArr , notFound = self.find_array(
                AR, LAS, declination, useACA, noOf12, uid)
            fudgeFactor += deltaFudgeFactor  
            AR *= fudgeFactor
    
        if notFound and nTry == maxTry:
            print "No matching found ... ", uid, AR, LAS, useACA, noOf12
            return [0, 0], [0, 0], 'E', 'E'
    
        else:
            # print self.array[min(matchArr)], self.array[max(matchArr)]
            if noOf12 == 1:
                self.minAR[0] = self.res[max(matchArr)] * 0.8
                self.maxAR[0] = self.res[min(matchArr)] * 1.2

            elif noOf12 == 2:
                if min(matchArr) > 5:
                    print " LBL array cannot be combined, check OT"

                else:
                    maxar = False
                    try:
                        arr2 = self.matchArrayCycle3[min(matchArr)]
                    except KeyError:
                        try:
                            arr2 = self.matchArrayCycle3[max(matchArr)]
                            maxar = True
                        except KeyError:
                            print(
                                "Two Configurations required, but only one "
                                "needed, check OT?", uid)
                            print matchArr, AR, LAS, useACA, noOf12
                            return 0
                    if maxar:
                        self.minAR[0] = self.res[max(matchArr)] * 0.8
                        self.maxAR[0] = self.res[max(matchArr)] * 1.2
                    else:
                        self.minAR[0] = self.res[min(matchArr)] * 0.8
                        self.maxAR[0] = self.res[min(matchArr)] * 1.2
                    
                    self.minAR[1] = self.res[arr2] * 0.8
                    self.maxAR[1] = self.res[arr2] * 1.3
        if noOf12 == 2:
            if maxar:
                return self.minAR, self.maxAR, self.array[max(matchArr)], \
                    self.array[arr2]
            else:
                return self.minAR, self.maxAR, self.array[min(matchArr)], \
                    self.array[arr2]
        else:
            return self.minAR, self.maxAR, self.array[min(matchArr)], 'Ca  '
    
# #######################################################################
# ### main program######
# if __name__ == "__main__":
#
#     a = arrayRes()
#     inp = 1.4, 103., -23., 1, 1
#     print inp[0] * 1.1, inp[0] / 2.
#     print inp
#     minAR , maxAR = a.run(inp[0], inp[1], inp[2], inp[3], inp[4], 'uid')
#     print minAR
#     print maxAR
