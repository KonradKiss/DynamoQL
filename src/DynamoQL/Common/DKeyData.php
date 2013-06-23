<?php
namespace DynamoQL\Common;
/**
 * Contains enumerable values used in the framework
 */

class DKeyData
{
    public $name;
    public $type;

    public function __construct( $name, $type )
    {
        $this->name = $name;
        $this->type = $type;
    }
}
