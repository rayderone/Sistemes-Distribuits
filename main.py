import pywren_ibm_cloud as pywren
import numpy as np
from cos_backend import CosBackend
import sys
import time
import pickle

# Configuracio del ibm cloud
config = {'pywren' : {'storage_bucket' : 'urvbucket'},

          'ibm_cf':  {'endpoint': 'https://eu-gb.functions.cloud.ibm.com', 
                      'namespace': 'adria.ribas@estudiants.urv.cat_dev', 
                      'api_key': 'c4b8698b-aa44-4a49-a6f0-b231f9cfd2ae:I8oY3vVJeZOIc0TEP1dJmeHO3o1ZV7jCDPwmPvakzpMc3vrdmgYmWtTDLqmoyHjQ'}, 

          'ibm_cos': {'endpoint': 'https://s3.eu-gb.cloud-object-storage.appdomain.cloud',
                      'private_endpoint': 'https://s3.private.eu-gb.cloud-object-storage.appdomain.cloud',
                      'api_key': '-k3E4W-hzX4YaWW_Dopxem1MuPQciEMJNFIsIVv-yFUQ'}}

# Objecte per a gestionar el COS
cos = CosBackend()

# Nombre de treballadors
workers = 1

# Matriu A
RowsNumberMatrixA = 350
ColumnsNumberMatrixA = 350

# Matriu B
RowsNumberMatrixB = 350
ColumnsNumberMatrixB = 350

# Funcio map
# Rep com a parametre un vector de parelles de matrius
def matrixMultiplication(cosWorkloadObject, ibm_cos):

    workloadObject = pickle.loads(ibm_cos.get_object(Bucket="urvbucket", Key=cosWorkloadObject)['Body'].read())

    resultVector = []

    for workload in workloadObject["workloadObject"]:

        resultVector.append(workloadObject["workloadObject"][workload]["matrixA"] @ workloadObject["workloadObject"][workload]["matrixB"])

    return resultVector

# Si les mides de les matrius son compatibles i el numero de treballadors es diferent de 0
if ((ColumnsNumberMatrixA == RowsNumberMatrixB) & (workers != 0)):
    
    # Generacio de les matrius amb valors aleatoris entre el 0 i el 9
    matrixA = np.random.randint(0, 9, RowsNumberMatrixA * ColumnsNumberMatrixA, "int").reshape(RowsNumberMatrixA, ColumnsNumberMatrixA)
    matrixB = np.random.randint(0, 9, RowsNumberMatrixB * ColumnsNumberMatrixB, "int").reshape(RowsNumberMatrixB, ColumnsNumberMatrixB)

    # Nombre d'elements de la matriu resultant que ha de calcular cada treballador
    workersWorkloadNumber = int((RowsNumberMatrixA * ColumnsNumberMatrixB) / workers)

    # Nombre d'elements calculats de la matriu resultant sobrants degut a que la divisio no es exacte
    surplusWorkloadNumber = (RowsNumberMatrixA * ColumnsNumberMatrixB) % workers

    # Nombre d'elements que s'han de calcular de la matriu resultant
    pendingWorkloadNumber = RowsNumberMatrixA * ColumnsNumberMatrixB

    # Objecte per a gestionar el pywren
    pw = pywren.ibm_cf_executor(config=config)

    # Futurs retornats per la funcio map
    mapFutures = []

    # Vector de carrega de treball
    workloadArray = []

    # Si el numero de treballadors disponible es mes gran o igual que el numero de tasques a realitzar, es tracta del cas ideal
    # ja que cada treballador fara una unica tasca, aixo es degut a que la unitat minima de treball en la que es pot subdividir
    # la multiplicacio de matrius utilitzant una subdivisio de files-columnes es 1 fila * 1 columna
    if (workers >= pendingWorkloadNumber):

        for row in matrixA:

            for column in matrixB.transpose():

                individualWork = {"workloadObject" : {}}

                individualWork["workloadObject"][pendingWorkloadNumber] = {"matrixA" : row, "matrixB" : column}

                cos.put_object("urvbucket", str(pendingWorkloadNumber), pickle.dumps(individualWork))

                workloadArray.append(str(pendingWorkloadNumber))

                pendingWorkloadNumber = pendingWorkloadNumber - 1
    
    # Si el numero de treballadors es mes petit al numero de tasques que s'han de realitzar, hem de repartir les tasques entre
    # els treballadors, de manera que a cada treballador li tocara realitzar com a minim una tasca, el que fem es dividir
    # el nombre de tasques entre el nombre de treballadors per repartir les tasques, si aquesta divisio no es exacte, el que fem
    # es repartir les N tasques que sobren entre els N primers treballadors
    else:

        rowCounter = 0
        columnCounter = 0

        for worker in range(workers):

            individualWork = {"workloadObject" : {}}

            for workerWorkload in range(workersWorkloadNumber):

                individualWork["workloadObject"][pendingWorkloadNumber] = {"matrixA" : matrixA[rowCounter], "matrixB" : matrixB.transpose()[columnCounter]}

                columnCounter = columnCounter + 1

                pendingWorkloadNumber = pendingWorkloadNumber - 1

                if (columnCounter >= ColumnsNumberMatrixB):
                
                    columnCounter = columnCounter % ColumnsNumberMatrixB

                    rowCounter = rowCounter + 1

            if (surplusWorkloadNumber > 0):

                individualWork["workloadObject"][pendingWorkloadNumber] = {"matrixA" : matrixA[rowCounter], "matrixB" : matrixB.transpose()[columnCounter]}
                
                pendingWorkloadNumber = pendingWorkloadNumber - 1

                columnCounter = columnCounter + 1

                if (columnCounter >= ColumnsNumberMatrixB):
                    
                    columnCounter = columnCounter % ColumnsNumberMatrixB

                    rowCounter = rowCounter + 1

                surplusWorkloadNumber = surplusWorkloadNumber - 1
            
            cos.put_object("urvbucket", str(pendingWorkloadNumber), pickle.dumps(individualWork))

            workloadArray.append(str(pendingWorkloadNumber))
    
    # Inici del temporitzador
    timeStart = time.time()

    # Executem totes les tasques dels treballadors
    mapFutures = pw.map(matrixMultiplication, workloadArray, runtime_memory=2048)

    # Esperem a que tots els treballadors hagin acabat les seves tasques
    pw.wait(mapFutures)

    # Calculem el temps que han tardat els treballadors
    timeStop = time.time()
    elapsedTime = timeStop - timeStart

    print("Elapsed time:", elapsedTime)

    resultVector = []

    if (workers != 1):

        for result in pw.get_result():

            for element in result:

                resultVector.append(element)

    else:

        for result in [pw.get_result()]:

            for element in result:

                resultVector.append(element)

    # Obtenim els resultats i els formatem en una matriu
    matrixC = np.asarray(resultVector).reshape(RowsNumberMatrixA, ColumnsNumberMatrixB)

    # Guardem les dues matrius inicials al COS
    cos.put_object("urvbucket", "matrixA", np.ndarray.tobytes(matrixA))
    cos.put_object("urvbucket", "matrixB", np.ndarray.tobytes(matrixB))

    # Guardem la matriu resultant al COS
    cos.put_object("urvbucket", "matrixC", np.ndarray.tobytes(matrixC))

    print(matrixC)
    print(matrixA @ matrixB)
    print("Elapsed time:", elapsedTime)

    # Netejem les dades temporals que s'han generat
    pw.clean()

else:
    print("ERROR!!! No es poden multiplicar dues matrius de tamanys incompatibles")