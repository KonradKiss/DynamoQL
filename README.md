# dynamoql #
---

## What is DynamoQL? ##

A simple SQL-like interface to the NoSQL Amazon DynamoDB service.

**But that is already possible with [x]!**

This librabry is intended to simlpify the most basic tasks, such as creating and dropping tables, scaling provisions and selecting, inserting, updating and deleting items from a table. The goal was to be able to prototype web applications quickly without the need to set up a more sophisticated infrastructure.

Hence it does not intend to support the more advanced features of SQL languages such as joining tables, grouping data and ordering the results.

This is merely a library that can make simple tasks less painful when dealing with Amazon DynamoDB. It requires the [AWS SDK for PHP (ver.2)](https://github.com/aws/aws-sdk-php) to be installed in order to work.

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

We first create the table. The last `true` parameter signals that this operation is synchronous. This means that the next command will not execute until the table is available (`ACTIVE`) or the request times out. We can then change the throughput if the default (`5`,`5`) is not good enough.

While here we used two commands, most of the time it becomes simpler than that. I did my best to come up with a syntax that feels close to SQL syntaxes while also fits the most common Amazon DynamoDB tasks.

Report issues here: https://github.com/KonradKiss/DynamoQL/issues



## Syntax ##

DynamoQL command syntax is case-insensitive while variable syntax is case-sensitive. Here I've used all uppercase commands while variables are differentiated via lowercase characters.

### Hash and Range types ###

At this time DynamoQL supprots the following values for *Hash* and *Range* key types: `NUMBER`, `STRING`, `NUMBERSET`, `STRINGSET`

### Creating a table ###

```
CREATE TABLE table_name HASH hash_name AS hash_type [ RANGE range_name AS range_type ]
```

**Example**

```
CREATE TABLE my_table HASH uuid AS STRING RANGE timestamp AS NUMBER
// We now have a my_table table with a uuid key as a string Hash key 
// and a numeric timestamp as the range key.
```

### Dropping a table ###

```
REMOVE TABLE table_name
```

**Example**

```
REMOVE TABLE my_table
// my_table is no more.
```

### Changing throughput  ###

Throughput is only changed if the given value is not the same as the current value. It also needs to be at most two times the current value and it can not go below 1.

`RESIZE THROUGHPUT` is used to change the throughput of a single table while `GLOBAL THROUGHPUT` is used to change the throughput of every single table.

```
RESIZE THROUGHPUT table_name READ reads WRITE writes
GLOBAL THROUGHPUT READ reads WRITE writes
```

**Example**

```
RESIZE THROUGHPUT my_table READ 20 WRITE 10
// my_table now has a read throughput of 20 and a write throughput of 10.
GLOBAL THROUGHPUT READ 5 WRITE 5
// All tables now have a read/write throughput of 5.
```

### Inserting values into a table ###

Inserts happen one at a time. It is not possible to chain several inserts together within a single command, however, by prepending the command with a `Q` command, it is possible to queue the insert command into the write request batch.

Values only need to use `"` or `'` characters for encapsulation if a `,` character is present in the value. Types for values other than the hash and range keys are recognized by examining the given attribute's value.

If there is already a value identified by the hash or hash + range keys then we receive an exception.

```
(Q) INSERT INTO table_name VALUES hash_key = "hash_value", (range_key = range_value,) attribute1 = "value1", ... , attributen = "valuen"
```

**Example**

```
Q INSERT INTO  my_table VALUES uuid = "aea1ceb0-60f5-11e2-bcfd-0800200c9a66", timestamp = 1358461861, message = "Test message!"
// The insert statement is queued into the write request batch.. we could do more inserts here
FLUSH
// The write request batch is sent along with those possible other inserts.
// The table my_table receives a new record or we receive an exception if there is already 
// a record with the given hash + range key combo.
```






---

Developed by Konrad Kiss (http://www.konradkiss.com/).

This software is licensed under Apache 2.0 license terms. Read LICENSE.txt for more information. AWS, Amazon DynamoDB and other Amazon Web Services are copyright 2012 Amazon Technologies, Inc. This product relies on software developed by Amazon Technologies, Inc (http://www.amazon.com/).

