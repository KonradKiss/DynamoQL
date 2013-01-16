<?php
//------------------------------------------------------------------------------
// Xenocell
// HTTP Application Server - Data Tunnel
//------------------------------------------------------------------------------
/*
// get db connection wrappers
require_once("./config.php");

// dynamoql
require_once("./dynamoql.php");

set_time_limit(5);
error_reporting(E_ALL ^ E_NOTICE);

$ddb = new DynamoQL(true); // true = debug

$dbc = db_openConnection();

function convert_type_mysql_to_ddb($type)
{
   $ddbt = AmazonDynamoDB::TYPE_STRING;

   $numerics = array( "bit", "tinyint", "bool", "boolean", "smallint", "mediumint", "int", "integer", "bigint", "serial", "decimal", "dec", "float", "double", "double precision", "real" );

   if (in_array($type, $numerics))
      $ddbt = AmazonDynamoDB::TYPE_NUMBER;

   return $ddbt;
}

function copyTable($table_name, $hash_key_name="", $hash_key_type=AmazonDynamoDB::TYPE_NUMBER, $range_key_name="", $range_key_type=AmazonDynamoDB::TYPE_NUMBER, $remove_if_exists=false)
{
   global $ddb, $dbc;

   // check if the table exists at the destination
   $table_info = $ddb->describe_table($table_name);
   $createtable = true;
   if ($table_info->status == 200) {
      // exists, table creation is skipped
      if ($remove_if_exists) {
         debugdump($table_name, "Warning: This table already exists at the destination. The table will now be removed.");
         $ddb->dql("REMOVE TABLE ".$table_name, true);
      } else {
         debugdump($table_name, "Warning: This table already exists at the destination. Using existing table.");
         $createtable = false;
         //exit;
      }
   }

   $primary = false;
   $sql = "DESCRIBE ".$table_name.";";
   $res = db_execSQL($sql);
   while ($row = mysql_fetch_assoc($res)) {
      $field = $row['Field'];
      $type = $row['Type'];
      $isnull = $row['Null'];
      $iskey = $row['Key'];
      $default = $row['Default'];
      $extra = $row['Extra'];
      if ($iskey)
         $primary = $field;
   }

   if ($createtable) {
      $field_list = array();
      $hash = "";
      $range = "";
      if ($range_key_name != "") {
         $range = $range_key_name . " AS " . $ddb->get_name_by_type($range_key_type);
      }

      //debugdump($field." (".$type.")", ($isnull ? "NULL, " : "") . ($iskey ? "KEY, " : ""), "DEFAULT: ".$default.", EXTRA: ".$extra);
      if ($primary) {
         if ($hash_key_name == "") {
            $tarr = explode("(", $type);
            $hash = $primary . " AS " . $ddb->get_name_by_type(convert_type_mysql_to_ddb($tarr[0]));
         } else {
            $hash = $hash_key_name . " AS " . $ddb->get_name_by_type($hash_key_type);
         }
      }

      if ($hash != "") {
         $createdql = "CREATE TABLE ".$table_name." HASH ".$hash.($range=="" ? "" : " RANGE ".$range);
         //debugdump($table_name, $createdql);
         $ddb->dql($createdql);
      } else {
         debugdump($table_name, "Error: Can only recreate tables with a primary key.");
         exit;
      }
   }

   // now to copy data
   $sql = "SELECT * FROM ".$table_name." ".($range_key_name != "" ? "ORDER BY ".$range_key_name." ASC" : "").";";
   $res = db_execSQL($sql);
   while ($row = mysql_fetch_assoc($res)) {
      $values = "";
      $uuid_data = "";
      foreach($row as $field => $value) {
         if ($field != $primary) {
            $values .= " ".$field."='".rawurlencode($value)."',";
            $uuid_data .= $value;
         } else {
            $values .= " ".$field."='#uuid#',";
         }
      }
      $values = trim($values, " ,");
      $values = str_replace("#uuid#", $ddb->generate_uuid($uuid_data), $values);
      $insertdql = "INSERT INTO ".$table_name." VALUES ".$values;
      $ddb->dql($insertdql);

      // we need to artifically slow the processing of rows
      //sleep(1);
      usleep(250000); // 4 every sec
   }
}

//copyTable("inventories", "intAvatarID", "N", "enuProfile", "S");
//copyTable("inventoryitems", "intInventoryID", "N", "intSlotID", "N");

$ddb->dql("SELECT intAvatarID, enuProfile FROM inventories", false);
*/

/*
$ddb->dql("REMOVE TABLE myTable00", false);
$ddb->dql("REMOVE TABLE myTable01", false);
$ddb->dql("REMOVE TABLE myTable02", false);
$ddb->dql("REMOVE TABLE myTable03", false);
$ddb->dql("REMOVE TABLE myTable04", false);
$ddb->dql("REMOVE TABLE myTable05", false);
$ddb->dql("REMOVE TABLE myTable06", false);
$ddb->dql("REMOVE TABLE myTable07", false);
$ddb->dql("REMOVE TABLE myTable08", false);
$ddb->dql("REMOVE TABLE myTable09", false);
$ddb->dql("REMOVE TABLE myTable000", false);
$ddb->dql("REMOVE TABLE myTable001", false);
$ddb->dql("REMOVE TABLE myTable002", false);
$ddb->dql("REMOVE TABLE myTable003", false);
$ddb->dql("REMOVE TABLE myTable004", false);
$ddb->dql("REMOVE TABLE myTable005", false);
$ddb->dql("REMOVE TABLE myTable006", false);
$ddb->dql("REMOVE TABLE myTable007", false);
$ddb->dql("REMOVE TABLE myTable008", false);
$ddb->dql("REMOVE TABLE myTable009", false);

$ddb->dql("CREATE TABLE myTable00 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable01 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable02 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable03 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable04 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable05 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable06 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable07 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable08 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable09 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable000 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable001 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable002 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable003 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable004 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable005 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable006 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable007 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable008 HASH id AS NUMBER", false);
$ddb->dql("CREATE TABLE myTable009 HASH id AS NUMBER", false);
*/


/*
$ddb->dql("CREATE TABLE inventories HASH intAvatarID AS NUMBER RANGE enuProfile AS STRING", true);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
$ddb->dql("INSERT INTO inventories VALUES enuProfile = 'newinventory'", false);
*/


//*$ddb->dql("DELETE FROM inventories WHERE intAvatarID = 10 AND enuProfile = 'doesnotexist'");

//$ddb->dql("CREATE TABLE myTable HASH id AS NUMBER", true);
//*$ddb->dql("CREATE TABLE myTable2 HASH id AS NUMBER", false);
//*$ddb->dql("CREATE TABLE myTable3 HASH id AS NUMBER RANGE oid AS STRING", true);

//*$ddb->dql("RESIZE THROUGHPUT READ 6 WRITE 6");

//*$ddb->dql("REMOVE TABLE myTable2", false);

//*$ddb->dql("MODIFY THROUGHPUT myTable READ 10 WRITE 10");

//*$ddb->dql("INSERT INTO myTable VALUES attribute1 = 244, attribute2 = 'value 11'");
//*$ddb->dql("INSERT INTO myTable VALUES attribute1 = 244, attribute2 = 'value 12'");
//*$ddb->dql("INSERT INTO myTable VALUES attribute1 = 244, attribute2 = 'value 13'");
//*$ddb->dql("INSERT INTO myTable VALUES id=1, attribute1 = 244, attribute2 = 'value 13'");

//*$ddb->dql("INSERT INTO myTable3 VALUES attribute1 = 223, attribute2 = 'value 13', id = '2', oid = 'abc-123'");

//*$ddb->dql("UPDATE myTable SET attribute1 = -1, attribute2 = 'minus one' WHERE Hash Key = 10");
//*$ddb->dql("UPDATE myTable3 SET attribute1 = -1, attribute2 = 'minus one' WHERE id = 2 AND oid = 'abc-123'");
//*$ddb->dql("UPDATE myTable SET attribute1 = 0, attribute2 = 'zero' WHERE id=10");

//*$ddb->dql("DELETE FROM myTable WHERE Hash Key = 10");

//*$ddb->dql("SELECT id, oid, attribute1, attribute2 FROM myTable");
//*$ddb->dql("SELECT id, attribute1, attribute2 FROM myTable WHERE Hash Key > 100");
//*$ddb->dql("SELECT attribute1, attribute2 FROM myTable WHERE Hash Key < 200 LIMIT 1");
//*$ddb->dql("SELECT attribute1, attribute2 FROM myTable WHERE LIMIT 1");
//*$ddb->dql("SELECT attribute1, attribute2 FROM myTable WHERE STARTING 100");
//*$ddb->dql("SELECT id, oid, attribute1, attribute2 FROM myTable WHERE Hash Key > 1 LIMIT 1 STARTING 100");
//*$ddb->dql("SELECT id, oid, attribute1, attribute2 FROM myTable WHERE Hash Key = 100");
//*$ddb->dql("SELECT id, oid, attribute1, attribute2 FROM myTable WHERE Hash Key = 150");
//*$ddb->dql("SELECT id, oid, attribute1, attribute2 FROM myTable3 WHERE Hash Key = 2 and OID = 'abc-123'");

//*$ddb->dql("CHOOSE id, oid, attribute1, attribute2 FROM myTable");
//*$ddb->dql("CHOOSE id, attribute1, attribute2 FROM myTable WHERE Hash Key > 100");
//*$ddb->dql("CHOOSE attribute1, attribute2 FROM myTable WHERE Hash Key < 200 LIMIT 1");
//*$ddb->dql("CHOOSE attribute1, attribute2 FROM myTable WHERE LIMIT 1");
//*$ddb->dql("CHOOSE attribute1, attribute2 FROM myTable WHERE STARTING 100");
//*$ddb->dql("CHOOSE id, oid, attribute1, attribute2 FROM myTable WHERE Hash Key > 1 LIMIT 1 STARTING 100");
//*$ddb->dql("CHOOSE id, oid, attribute1, attribute2 FROM myTable WHERE Hash Key = 100");
//*$ddb->dql("CHOOSE id, oid, attribute1, attribute2 FROM myTable WHERE Hash Key = 150");
//*$ddb->dql("CHOOSE id, oid, attribute1, attribute2 FROM myTable3 WHERE Hash Key = 2 and OID = 'abc-123'");

//*$ddb->dql("REMOVE TABLE myTable", false);
//*$ddb->dql("REMOVE TABLE myTable3", false);
