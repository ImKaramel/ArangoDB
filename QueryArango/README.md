## ArangoDB Query 

## Installation
```bash
pip install pyArango
```

## Запуск

Приведен в run.sh, на вход подается путь к json файл, в котором записаны необходимые значения 
Также, в нем можно найти описание коллекций графа

```sh
python3 main.py "/path_to_config/configMooc.json"
```

## Описание запросов

### queryBFS и queryDFS

эти запросы, которые выполняют поиск в ширину и поиск в глубину соответственно в заданном графе в базе данных. Оба метода принимают следующие параметры:
- graph: имя графа, который будет использоваться
- depth: максимальная глубина, на которую будет выполнен поиск
- startVertex: идентификатор вершины, с которой начнется поиск
- param: имя атрибута ребра, который будет использоваться для фильтрации
- value: минимальное значение, которое должен иметь атрибут ребра, чтобы ребро было включено в поиск

json данных запросов:
```json
  "queryBFS_DFS" : {
    "depth": 1,
    "startVertex": "User/42",
    "fieldName" : "timestamp",
    "value" : "2571264.000"
  },
```

```python
        query = f'''
        FOR v, e, p IN 1..{depth} OUTBOUND "{startVertex}" GRAPH {graph}
            OPTIONS {{"order": "bfs"}}
            FILTER LENGTH(p.edges) == {depth} && (p.edges[*].{param} ALL >= {value})
            FILTER LENGTH(p.vertices) == {depth + 1}
            RETURN v
            '''

```

```python
        query = f""" 
        FOR v, e, p IN 1..{depth} OUTBOUND "{startVertex}" GRAPH {graph} 
            OPTIONS {json.dumps(options)}
            FILTER p.edges[*].{param} ALL >= {value}
            FILTER LENGTH(p.edges) == {depth}
            FILTER NOT (v._id IN (
                FOR v1, e1, p1 IN 1..1 OUTBOUND v._id GRAPH {graph}
                RETURN p1.vertices[1]._id))
            RETURN v
        """
```

### queryFilter

это метод, который выполняет простой запрос на фильтрацию в заданной коллекции в базе данных. Он принимает следующие параметры:

- collection: имя коллекции, в которой будет искаться
- fieldName: имя поля, которое будет использоваться для фильтрации
- value: значение, которое должно иметь поле, чтобы вершина был включен в поиск

```json
  "queryFilter": {
    "collection": "Action",
    "fieldName" : "timestamp",
    "value" : "2571264.000"
  },
```

```python
        query = (f"""
        FOR v IN {collection}
            FILTER (v.{fieldName}) == {value}
            RETURN v
        """)
```

## queryFilterExtended 

это запрос, который выполняет более сложный запрос на фильтрацию в заданной коллекции в базе данных. Он принимает следующие параметры:
- collection: имя коллекции, которая будет искаться
- action: тип ребра, которое будет обходиться
- fieldName: имя атрибута ребра, который будет использоваться для фильтрации
- value: минимальное значение, которое должен иметь атрибут ребра, чтобы ребро было включено в поиск
- degree: минимальное количество ребер, которые должны быть пройдены, чтобы документ был включен в поиск

```json
  "queryFilterExtended": {
    "collection": "User",
    "edge":  "Action",
    "fieldName" : "timestamp",
    "value" : "2571264.000",
    "degree": 1
  },
```

```python
        query = f"""
        FOR vertex IN {collection}
            LET degree = LENGTH((FOR v, e IN 1..1 OUTBOUND vertex {action} 
                FILTER e.{fieldName} == {value} RETURN 1))
            FILTER degree >= {degree}
            RETURN vertex
        """
```

 ## queryFilterSum

Запрос фильтрует документы на основе определенного значения поля и вычисляет сумму другого поля для отфильтрованных документов.
 Затем он возвращает вершину и общую сумму. 
 
- collection - имя коллекции, на которую будет выполнен запрос.
- action - название коллекции ребер, которые будут использоваться в запросе. 
- fieldName - имя поля, по которому будет производиться фильтрация документов.
- value - значение, по которому будут фильтроваться документы.
- sumValue - значение, по которому будет фильтроваться сумма поля.

 ```json
   "queryFilterSum": {
    "collection": "User",
    "action": "Action",
    "fieldName" : "timestamp",
    "value" : "2571264.000",
    "sumValue": "1000000"
  },
 ```

```python
        query = f"""
        FOR v IN {collection}
            LET sum = (
            FOR e IN OUTBOUND v {action}
                FILTER e.{fieldName} > {value}
                RETURN e.{fieldName}
             )
            LET totalSum = SUM(sum)
            FILTER totalSum > {sumValue}
        RETURN {{ vertex: v, sum: totalSum }}
        """
```


## queryTriangles

Он находит все треугольники в графе, начиная с заданной вершины. Треугольник определяется как набор из трех вершин, соединенных ребрами.
Метод возвращает количество найденных треугольников и список треугольников. 

- graph - имя графа, на который будет выполнен запрос.
- startVertex - начальная вершина, с которой начнется поиск треугольников в графе.

```json
    "queryTriangles": {
    "startVertex": "User/42"
  },
```

 