<?php
namespace DynamoQL\Common;
/**
 * Contains enumerable values used in the framework
 */

use DynamoQL\Common\DKeyData;

class DKeyPair
{
    public $hash = null;
    public $range = null;

    public function __construct( $table_data )
    {
        for ($i=0; $i<count($table_data["KeySchema"]); $i++)
        {
            $key = $table_data["KeySchema"][$i];
            if ($key["KeyType"] == "HASH")
            {
                $this->hash = new DKeyData($key["AttributeName"], DKeyPair::getAttributeType($table_data, $key["AttributeName"]));
            }
            else if ($key["KeyType"] == "RANGE")
            {
                $this->range = new DKeyData($key["AttributeName"], DKeyPair::getAttributeType($table_data, $key["AttributeName"]));
            }
        }
    }

    public function getAttributeType($data, $name)
    {
        for ($j=0;$j<count($data["AttributeDefinitions"]);$j++)
        {
            $k = $data["AttributeDefinitions"][$j];
            if ($k["AttributeName"] == $name)
                return $k["AttributeType"];
        }

        return null;
    }
}
