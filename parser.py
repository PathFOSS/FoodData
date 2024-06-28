# Author: PathFoss
# License: GPLv3
# Requirement: Download & unzip into current folder "Full Download of All Data Types" at https://fdc.nal.usda.gov/download-datasets.html

import pandas as pd
import regex as re
import json
import os
import sys
import time
import threading 
 
# Define lists and dictionaries
categoricalDict = {}
comprehensiveDict = {}
foundationalDict = {}
foundationalList = []

# Define nutrient identifiers
proteinID = 1003
fatID = 1004
carbsID = 1005

# Define global elements
running = True
overwrite = True
threadID = 0
totalTime = 0
categoryNum = 0
lengthOfFile = 0
filePosition = 0

# Create a thread to print process progress
class thread(threading.Thread): 
        def __init__(self, message, threadID, isPercent): 
                threading.Thread.__init__(self) 
                self.message = message 
                self.threadID = threadID
                self.isPercent = isPercent

        def run(self): 
                global running, totalTime
                running = True
                totalTime = 0

                while running:
                        if self.isPercent:
                                percent = round(filePosition * 100/ lengthOfFile)
                                finalText = str(percent) + "%"
                                sys.stdout.write("\r{}".format(self.message + " [ " + finalText + " ]" + " [ " + str(totalTime) + "s ]"))
                        else:
                                sys.stdout.write("\r{}".format(self.message + " [ " + str(totalTime) + "s ]"))

                        sys.stdout.flush()
                        time.sleep(1)
                        totalTime += 1
                sys.stdout.write("\n")

# Start thread with short command
def startThread(message, isPercent):
        global threadID
        threadID += 1
        timerThread = thread(message, threadID, isPercent)
        timerThread.start()
        return timerThread

# Stop thread with short command
def stopThread(currentThread):
        global running
        running = False
        currentThread.join()

# Export foods by category into JSON format
def exportAsJSON (fileName, foodList):     
        with open("./food_by_category/" + fileName, "w") as final:
                json.dump(foodList, final, indent=4)

# Create a CSV file for foods and their macros
def createMacroNutrientCSV ():
        messagerThread = startThread("Creating file: macro_nutrients.csv", False)
        firstChunk = True
        
        for chunk in pd.read_csv("food_nutrient.csv", chunksize = 10000, usecols = ["fdc_id", "nutrient_id", "amount"]):
                filt1 = (chunk["nutrient_id"] == proteinID) | (chunk["nutrient_id"] == fatID) | (chunk["nutrient_id"] == carbsID)
                writePath='tmp_macro_nutrients.csv'
                
                df1 = chunk[filt1].set_index("fdc_id")[["nutrient_id", "amount"]]
                df1 = df1.replace(np.nan, 0)
                df1.to_csv(writePath, mode="w")
                df2 = pd.read_csv("tmp_macro_nutrients.csv").pivot_table(index='fdc_id', columns="nutrient_id", values="amount", fill_value=0).reset_index().set_index("fdc_id")
                
                if firstChunk:
                        df2.to_csv("macro_nutrients.csv", mode="a")
                        firstChunk = False
                        continue
                df2.to_csv("macro_nutrients.csv", mode="a", header=None)

        stopThread(messagerThread)

# Create dictionary from a CSV of foods
def createDictionary (dataFrame, dataType):
        global running
        messagerThread = startThread("Creating dictionary object for type: " + dataType, False)
        returnDict = dataFrame[dataFrame["data_type"] == dataType].set_index("fdc_id")["description"].to_dict()
        stopThread(messagerThread)
        return returnDict

# Complete food category dictionary
def modifyDictionary ():
        global categoricalDict, foundationalDict, categoryNum
        messagerThread = startThread("Reading file: branded_food.csv", False)
        brandedDataFrame = pd.read_csv("branded_food.csv", chunksize = 1000, usecols = ["fdc_id", "branded_food_category"])
        stopThread(messagerThread)
        messagerThread = startThread("Creating dictionary for food categories", False)

        for chunk in brandedDataFrame:
                temp = chunk.groupby('branded_food_category')['fdc_id'].agg(list).to_dict()
                
                categoricalDict = { key:categoricalDict.get(key,[]) + temp.get(key,[]) for key in set(list(categoricalDict.keys()) + list(temp.keys())) }
        
        categoricalDict["Foundational Foods"] = foundationalDict.keys()
        stopThread(messagerThread)
        categoryNum = len(categoricalDict)
        
        keyList = []

        for key in categoricalDict.keys():
                if key.split()[0] == "Includes" or key == "Ready-made combination meal, frozen dinner, frozen meal, or microwave meal is a pre-packaged frozen full meal.  The meal requires no preparation other than cooking and contains all the elements typically contained in a single-serving meal.  A ready-made meal (also known as a TV Dinner) must have a main component and at least one additional component such as a side item, a dessert and/or beverage. These products require cooking prior to consumption. These products must be frozen to extend their consumable life.":
                        keyList.append(key)
                        categoryNum -= 1
        for key in keyList:
                del categoricalDict[key]

        for key in categoricalDict.keys():
                keyDict = {}
                for item in categoricalDict[key]:
                        if str(item)[0:4] not in keyDict:
                                keyDict[str(item)[0:4]] = []
                        keyDict[str(item)[0:4]] += [item]
                categoricalDict[key] = keyDict

# Create an exportable master dictionary for each food category and export in JSON
def createFinalDictionary():
        global categoricalDict, foundationalDict, categoryNum, overwrite, totalTime, lengthOfFile, filePosition
        categoryCounter = 0
        
        with open("macro_nutrients.csv") as f:
                lengthOfFile = sum(1 for line in f)

        for category in categoricalDict.keys():
                categoryCounter += 1
                filePosition = 0

                fileName = category.lower()
                fileName = fileName.replace("'", "").replace("-", " ").replace("(", " ").replace(")", " ").replace(",", " ").replace("/"," or ")
                fileName = re.sub(r"\s+",'_', fileName.strip()) + ".json"

                if not overwrite and os.path.exists("./food_by_category/" + fileName):
                        filePosition = lengthOfFile
                        messagerThread = startThread("Generating file for category: [ " + str(categoryCounter) + "/" + str(categoryNum) + " ]", True)
                        stopThread(messagerThread)
                        continue
                
                messagerThread = startThread("Generating file for category: [ " + str(categoryCounter) + "/" + str(categoryNum) + " ]", True)
                exportList = []
                nameList = []

                for chunk in pd.read_csv("macro_nutrients.csv", chunksize=1000):
                        filePosition += 1000

                        for chunkDict in chunk.to_dict("records"):
                                foodItem = {}
                                name = comprehensiveDict.get(int(chunkDict["fdc_id"]))

                                if name != None and isinstance(name, str) and name not in nameList and chunkDict["fdc_id"] in categoricalDict[category].get(str(chunkDict["fdc_id"])[0:4], []):
                                        nameList.append(name)

                                        cleanName = name.lower().replace("+", " & ").replace(" and ", " & ").replace("'n", " & ").replace("'", "").replace(";", "").replace("#", "").replace("|", " ").replace("\"", "").replace(" / ", " ").replace("100%% organic", " ")
                                        cleanName = ' '.join(s for s in cleanName.split() if any(c == "%" for c in s) or not any(c.isdigit() for c in s)) # Delete words with digits except when it ends with a percent sign
                                        cleanName = re.sub(r"(\(.*\))|(\[.*\])", "", cleanName) # Delete bracketed items
                                        cleanName = re.sub(r"\b(oz|g|lb|lbs|ml|fl|gal)\b", "", cleanName) # Delete units
                                        cleanName = re.sub(r"\b(swn |iqf|x|vp|mpg|mbg|avg|servings|serving|pc|min|box|bag|cartons)\b", "", cleanName) # Delete random strings
                                        cleanName = re.sub(r"\s+",' ', cleanName) # Delete extra space

                                        split = cleanName.split(",")
                                        lastWords = split[-1].strip()
                                        firstWords = ",".join(split[0:len(split)-1])

                                        if lastWords in firstWords:
                                                cleanName = firstWords

                                        cleanName = cleanName.replace(" . ", "").strip().capitalize()

                                        if cleanName == "":
                                                continue
                                        
                                        foodItem["Name"] = cleanName
                                        nutrients = {}
                                        
                                        p = round(chunkDict[str(proteinID)], 1)
                                        f = round(chunkDict[str(fatID)], 1)
                                        c = round(chunkDict[str(carbsID)], 1)

                                        if p + f + c <= 100:

                                                nutrients["Protein"] = p
                                                nutrients["Fat"] = f
                                                nutrients["Carbs"] = c
                                                nutrients["Calories"] = round(p * 4 + f * 9 + c * 4, 1)

                                                foodItem["Nutrients"] = nutrients
                                                exportList.append(foodItem)

                exportAsJSON(fileName, exportList)
                stopThread(messagerThread)

# Main method
def main ():
        global comprehensiveDict, foundationalDict, running, overwrite
        print("USDA Food Data Simplifier, PathFOSS, under GPLv3 \n")
        messagerThread = startThread("Reading file: food.csv", False)
        identifierDataFrame = pd.read_csv("food.csv")
        stopThread(messagerThread)

        comprehensiveDict = createDictionary(identifierDataFrame, "branded_food")
        foundationalDict = createDictionary(identifierDataFrame, "foundation_food")
        comprehensiveDict.update(foundationalDict)
        modifyDictionary()

        if not os.path.exists("./food_by_category"):
                os.mkdir("./food_by_category")
        else:
                confirmOverwrite = input("Overwrite old json files? [ Y/n ] ")
                if confirmOverwrite.lower() == "n":
                        overwrite = False

        if os.path.isfile("macro_nutrients.csv"):
                confirmDeletion = input("Do you want to rewrite the file: macro_nutrients.csv? [ Y/n ] ")
                if confirmDeletion.lower() == "y":
                        os.remove("macro_nutrients.csv")
                        createMacroNutrientCSV()
                createFinalDictionary()
        else:
                createMacroNutrientCSV()
                createFinalDictionary()
        
        if os.path.isfile("tmp_macro_nutrients.csv"):
                os.remove("tmp_macro_nutrients.csv")

# Call main method
if __name__ == "__main__":
        main()
