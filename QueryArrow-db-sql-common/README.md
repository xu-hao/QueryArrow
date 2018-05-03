### Predicate

```yaml
- predName:
  - <predicate name>
  predType:
    paramsType:
    - isInput: true
      paramType:
        tag: TypeCons
        contents: int64 | text
      isOutput: true
      isKey: true | false
    predKind: ObjectPred | PropertyPred
```

`ObjectPred` is a predicate containing all primary keys of a table.

`PropertyPred` is a predicate containing all primary keys and a non primary key of a table.

`isKey` is whether a column is a primary key.

### SQL Mapping

```yaml
- sqlMappingTable:
    sqlVar: '1'
    tableName: <table name>
  sqlMappingPredName: <predicate name>
  sqlMappingCols:
  - colName: <column name>
    tableVar: '1'
```

Define all columns that a predicate is mapped to.

Example given a table `T`

||   `A`  | `B` |
|---|---|---|
|primary key| `true` | `false` |
|type|  `int64` | `text` |

Defines two predicates


```yaml
- predName:
  - P
  predType:
    paramsType:
    - isInput: true
      paramType:
        tag: TypeCons
        contents: int64
      isOutput: true
      isKey: true
    predKind: ObjectPred
- predName:
  - Q
  predType:
    paramsType:
    - isInput: true
      paramType:
        tag: TypeCons
        contents: int64
      isOutput: true
      isKey: true
    - isInput: true
      paramType:
        tag: TypeCons
        contents: text
      isOutput: true
      isKey: false
    predKind: PropertyPred
```

```yaml
- sqlMappingTable:
    sqlVar: '1'
    tableName: T
  sqlMappingPredName: P
  sqlMappingCols:
  - colName: A
    tableVar: '1'
- sqlMappingTable:
    sqlVar: '1'
    tableName: T
  sqlMappingPredName: Q
  sqlMappingCols:
  - colName: A
    tableVar: '1'
  - colName: B
    tableVar: '1'
```
### Generating from sql schema

```
stack exec schema_parser_main <sql schema> <preds> <sql mappings>
```
