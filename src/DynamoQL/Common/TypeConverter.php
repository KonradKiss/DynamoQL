<?php
namespace DynamoQL\Common;

/**
 * Created by: Konrad Kiss
 * Date: 1/17/13
 * Time: 6:27 PM
 */

use Aws\DynamoDb\Enum\Type;

class TypeConverter
{
    public static function byType( $type )
    {
        $name_to_type = array(
            Type::STRING     => "STRING",
            Type::NUMBER     => "NUMBER",
            Type::STRING_SET => "STRINGSET",
            Type::NUMBER_SET => "NUMBERSET"
        );

        return $name_to_type[$type];
    }

    public static function byName( $name )
    {
        $type_to_name = array(
            "STRING"    => Type::STRING,
            "NUMBER"    => Type::NUMBER,
            "STRINGSET" => Type::STRING_SET,
            "NUMBERSET" => Type::NUMBER_SET
        );

        return $type_to_name[strtoupper($name)];
    }
}
