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
        $this->hash = new DKeyData($table_data["KeySchema"]["HashKeyElement"]["AttributeName"],
            $table_data["KeySchema"]["HashKeyElement"]["AttributeType"]);
        if ( isset($table_data["KeySchema"]["RangeKeyElement"]) ) {
            $this->range = new DKeyData($table_data["KeySchema"]["RangeKeyElement"]["AttributeName"],
                $table_data["KeySchema"]["RangeKeyElement"]["AttributeType"]);
        }
    }
}
