dynamoql
========

A simple SQL-like interface to the NoSQL Amazon DynamoDB service.

WHAT IS DYNAMOQL?

DynamoQL is a PHP class library that brings a (very) simple query language
(SQL-like) interface to the no-sql Amazon DynamoDB database service.



BUT THAT IS ALREADY POSSIBLE WITH HADOOP!

This librabry is intended to simlpify the most basic tasks, such as 
creating and dropping tables, scaling provisions and selecting, inserting,
updating and deleting items from a table. The goal was to be able to
prototype web applications quickly without the need to set up a more
sophisticated infrastructure.

Hence it does not intend to support the more advanced features of SQL 
languages such as joining tables, grouping data and ordering the results.

This is merely a library that can make simple tasks less painful when dealing
with Amazon DynamoDB. It requires the AWS SDK for PHP to be installed in
order to work.



EXAMPLE?

Ok, so instead of this:

####################################################################

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

####################################################################

You can do this:

####################################################################

$dql = new DynamoQL();
$dql->sql("CREATE TABLE my_table 
           HASH ID AS NUMBER, RANGE Date AS NUMBER
  		  READ 500 WRITE 500");

####################################################################


I did my best to come up with a syntax that feels close to SQL 
syntaxes while also fits the most common Amazon DynamoDB tasks.

If you find this useful, I'd appreciate a comment about it on the
website: http://www.konradkiss.com/p/dynamoql.html

If you'd like to join the project and support or develop DynamoQL
further, please contact me at konrad at konradkiss dot com.

Report issues here: https://github.com/KonradKiss/DynamoQL/issues

Cheers,
Konrad


========

Developed by Konrad Kiss (http://www.konradkiss.com/).

This software is licensed under Apache 2.0 license terms. 
Read LICENSE.txt for more information.

AWS, Amazon DynamoDB and other Amazon Web Services are 
copyright 2012 Amazon Technologies, Inc.

This product relies on software developed by Amazon Technologies, Inc 
(http://www.amazon.com/).

