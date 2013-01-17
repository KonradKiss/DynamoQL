# dynamoql #
---

## What is DynamoQL? ##

A simple SQL-like interface to the NoSQL Amazon DynamoDB service.

## But that is already possible with [x]! ##

This librabry is intended to simlpify the most basic tasks, such as creating and dropping tables, scaling provisions and selecting, inserting, updating and deleting items from a table. The goal was to be able to prototype web applications quickly without the need to set up a more sophisticated infrastructure.

Hence it does not intend to support the more advanced features of SQL languages such as joining tables, grouping data and ordering the results.

This is merely a library that can make simple tasks less painful when dealing with Amazon DynamoDB. It requires the AWS SDK for PHP (ver.2) to be installed in order to work.

## Example ##

Ok, so instead of this:

```php
$dynamodb = new AmazonDynamoDB();
 
$table_name = 'my-table';
 
$response = $dynamodb->create_table(array(
    'TableName' => $table_name,
    'KeySchema' => array(
        'HashKeyElement' => array(
            'AttributeName' => 'ID',
            'AttributeType' => AmazonDynamoDB::TYPE_NUMBER
        ),
        'RangeKeyElement' => array(
            'AttributeName' => 'Date',
            'AttributeType' => AmazonDynamoDB::TYPE_NUMBER
        )
    ),
    'ProvisionedThroughput' => array(
        'ReadCapacityUnits' => 500,
        'WriteCapacityUnits' => 500
    )
));
```

You can do this:

```php

$dql = new DynamoQL();
$dql->dql("CREATE TABLE my_table HASH ID AS NUMBER, RANGE Date AS NUMBER", true);
$dql->dql("RESIZE THROUGHPUT my_table READ 500 WRITE 500");
```

I did my best to come up with a syntax that feels close to SQL syntaxes while also fits the most common Amazon DynamoDB tasks.

Report issues here: https://github.com/KonradKiss/DynamoQL/issues

## Syntax ##

### Hash and Range types ###

At this time DynamoQL supprots the following values for *Hash* and *Range* key types: `NUMBER`, `STRING`, `NUMBERSET`, `STRINGSET`

### Creating a table ###

```
CREATE TABLE table_name HASH hash_name AS hash_type [ RANGE range_name AS range_type ]
```

#### Example ####

```
CREATE TABLE my_table HASH uuid AS STRING [ RANGE timestamp AS NUMBER ]
```


---

Developed by Konrad Kiss (http://www.konradkiss.com/).

This software is licensed under Apache 2.0 license terms. Read LICENSE.txt for more information. AWS, Amazon DynamoDB and other Amazon Web Services are copyright 2012 Amazon Technologies, Inc. This product relies on software developed by Amazon Technologies, Inc (http://www.amazon.com/).

